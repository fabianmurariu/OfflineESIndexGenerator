package com.github.fabianmurariu.esoffline

import java.nio.file.{Files, Path}

import com.github.fabianmurariu.esoffline.offline.FailedToSnapshotIndex
import com.sksamuel.elastic4s.Indexes
import com.sksamuel.elastic4s.embedded.{InternalLocalNode, LocalNode}
import com.sksamuel.elastic4s.http.snapshots.CreateSnapshotResponse
import com.sksamuel.elastic4s.http.{ElasticClient, RequestFailure, RequestSuccess}
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.http.HttpHost
import org.apache.log4j.Logger
import org.elasticsearch.analysis.common.CommonAnalysisPlugin
import org.elasticsearch.client.RestClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.reindex.ReindexPlugin
import org.elasticsearch.percolator.PercolatorPlugin
import org.elasticsearch.plugin.analysis.kuromoji.AnalysisKuromojiPlugin
import org.elasticsearch.plugin.analysis.nori.AnalysisNoriPlugin
import org.elasticsearch.plugin.analysis.smartcn.AnalysisSmartChinesePlugin
import org.elasticsearch.script.mustache.MustachePlugin
import org.elasticsearch.transport.Netty4Plugin
import io.circe.generic.auto._
import io.circe.parser._
import scala.io.Source
import org.apache.commons.io.filefilter.DirectoryFileFilter.DIRECTORY

object EsNode {

  @transient lazy val LOG: Logger = Logger.getLogger(this.getClass)

  private def defaultSettings(root: Path, partitionId: Int, attemptId: Int, http: Boolean = true): Task[Settings.Builder] = Task {
    val clusterName = s"offline-cluster-$partitionId-$attemptId"
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    val pathData = root.resolve("data")
    val pathHome = root.resolve("home")
    val pathRepo = root.resolve("repo")
    LOG.info(s"Setting up node with repo: ${pathRepo.toAbsolutePath} data: ${pathData.toAbsolutePath} home: ${pathHome.toAbsolutePath}")

    Settings.builder
      .put("http.enabled", http)
      .put("processors", 1)
      .put("node.name", s"EsNode_${partitionId}_$attemptId")
      .put("cluster.name", clusterName)
      .put("node.master", true)
      .put("discovery.type", "single-node")
      .put("path.data", s"$pathData")
      .put("indices.memory.index_buffer_size", "5%")
      .put("indices.fielddata.cache.size", "0%")
      .put("path.repo", s"$pathRepo")
      .put("path.home", s"${pathHome}")
  }

  private def localNodeWithHttp(settings: Settings): Task[ElasticClient] = Task {
    require(settings.get("cluster.name") != null)
    require(settings.get("path.home") != null)
    require(settings.get("path.data") != null)
    require(settings.get("path.repo") != null)

    val plugins =
      List(classOf[Netty4Plugin], classOf[MustachePlugin], classOf[PercolatorPlugin], classOf[ReindexPlugin], classOf[CommonAnalysisPlugin])

    val additionalPlugins =
      List(classOf[AnalysisKuromojiPlugin], classOf[AnalysisSmartChinesePlugin], classOf[AnalysisNoriPlugin])

    val mergedSettings = Settings
      .builder()
      .put(settings)
      .put("http.type", "netty4")
      .put("http.enabled", "true")
      .put("node.max_local_storage_nodes", "10")
      .build()

    val ln = new InternalLocalNode(mergedSettings, plugins ++ additionalPlugins)
    ln.start()
    ln
  }.map { ln => ElasticClient.fromRestClient(RestClient.builder(new HttpHost(ln.ip, ln.port)).build()) }

  def http(partitionId: Int, attemptId: Int, localPath: Path, additionalSettings: (String, String)*): Task[ElasticClient] = {
    {
      for {
        path <- Task(localPath).map {
          case p if Files.exists(p) =>
            FileUtils.forceDelete(p.toFile)
            Files.createDirectory(p)
          case p =>
            Files.createDirectory(p)
        }
        builder <- defaultSettings(path, partitionId, attemptId)
        settings <- Task(additionalSettings.foldLeft(builder) { case (b, (k, v)) => b.put(k, v) }.build())
        client <- localNodeWithHttp(settings)
      } yield client
    }

  }

  def finalizeTask(elasticClient: Task[ElasticClient], o: OfflineIndexPartition): Unit = {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    implicit val ss: SchedulerService = Scheduler.io()

    elasticClient.flatMap { ec =>
      Task.deferFuture {
        ec.execute {
          flushIndex(o.indices)
          forceMerge(o.indices)
          createSnapshot(s"offline-snapshot", "offline-backup")
            .indices(Indexes(o.indices))
            .waitForCompletion(true)
        }
      }
    }.flatMap {
      case out: RequestSuccess[CreateSnapshotResponse] =>
        LOG.info(s"Snapshot: $out")
        liftDataSegmentToHDFS(o)
      case out: RequestFailure => throw FailedToSnapshotIndex(out.toString)
    }.runSyncUnsafe()
  }

  def liftDataSegmentToHDFS(o: OfflineIndexPartition, conf: Configuration = new Configuration()): Task[Unit] = o match {
    case OfflineIndexPartition(partitionId, dest, localPath, _) =>

      val fs = new HPath(dest.toString).getFileSystem(conf)
      import scala.collection.JavaConversions._
      val localRepo = localPath.resolve("repo")
      LOG.info(s"Lifting snapshot from ${localRepo.toAbsolutePath} for $partitionId dest: $partitionId ")
      fs.setWriteChecksum(false)
      //read index0
      // find the segment with data for every index
      val fileContent = Source.fromFile(localRepo.resolve("index-0").toFile).mkString
      Task
        .fromEither(decode[Index0](fileContent))
        .foreachL {
          case Index0(Snapshot(_, snapshotId) :: _, indices) =>
            indices.foreach {
              case (name, IndexMeta(id, _)) =>
                val indexPath = localRepo.resolve("indices").resolve(id)
                val dataSegmentFile = FileUtils
                  .listFilesAndDirs(indexPath.toFile, DIRECTORY, DIRECTORY)
                  .tail.maxBy(FileUtils.sizeOfDirectory)
                fs.copyFromLocalFile(true, true,
                  new HPath(dataSegmentFile.getAbsolutePath),
                  new HPath(dest.toString, s"indices/$name/$partitionId"))
                if (partitionId == 0) {
                  fs.copyFromLocalFile(
                    new HPath(indexPath.resolve(s"meta-$snapshotId.dat").toAbsolutePath.toString),
                    new HPath(dest.toString, s"indices/$name/meta-$snapshotId.dat"))
                }
            }
            // for partition 0 copy metadata
            if (partitionId == 0) {
              val files = FileUtils
                .listFiles(localRepo.toFile, null, false)
                .toVector
              files.foreach { f => fs.copyFromLocalFile(new HPath(f.toURI), new HPath(dest)) }
            }
        }
    // we always copy the 0 partition id in full
  }

  def renameSnapFilesInSegments(dest: String)(implicit fs: FileSystem): Task[Unit] = {
    val indicesPath: HPath = new HPath(dest, "indices")

    for {
      meta <- Task.fromEither(decode[Index0](Source.fromInputStream(fs.open(new HPath(dest, "index-0"))).mkString))
    } yield {
      val statuses = fs.globStatus(new HPath(indicesPath, "*/*/snap-*.dat"))
      statuses
        .foreach {
          src =>
            val snapshotId = meta.snapshots.find(_.name == "offline-snapshot").get
            val dest = new HPath(src.getPath.getParent, s"snap-${snapshotId.uuid}.dat")
            if (!fs.exists(dest)) fs.rename(src.getPath, dest)
        }
    }
  }

  def renameSnapshotIndices(dest: String)(implicit fs: FileSystem): Task[Unit] = {
    val indicesPath: HPath = new HPath(dest, "indices")

    for {
      meta <- Task.fromEither(decode[Index0](Source.fromInputStream(fs.open(new HPath(dest, "index-0"))).mkString))
      indices <- Task(fs.listStatus(indicesPath))
    } yield {
      indices.foreach { f =>
        val indexId = meta.indices(f.getPath.getName).id
        val destIndexPath = new HPath(indicesPath, indexId)
        fs.rename(f.getPath, destIndexPath)
      }
    }
  }

  def renameIndicesAndSnapshot(dest: String)(implicit fs: FileSystem): Task[Unit] =
    renameSnapshotIndices(dest).flatMap(_ => renameSnapFilesInSegments(dest))

}

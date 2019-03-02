package com.github.fabianmurariu.esoffline

import com.github.fabianmurariu.esoffline.Hdfs.OfflineIndexPartition
import com.sksamuel.elastic4s.Indexes
import com.sksamuel.elastic4s.http._
import io.circe.generic.auto._
import io.circe.parser._
import monix.eval.Task
import monix.execution.schedulers.SchedulerService
import monix.execution.{Callback, Scheduler}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.DirectoryFileFilter.DIRECTORY
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, RemoteIterator, Path => HPath}

import scala.io.Source

object EsLang {

  def createPipeline(elasticClient: ElasticClient): Task[ElasticClient] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val restClient = elasticClient.client
    val body =
      """
        | {
        |  "description": "A pipeline to index data into language specific analyzers",
        |  "processors": [
        |    {
        |      "langdetect": {
        |        "field": "my_field",
        |        "target_field": "lang"
        |      }
        |    },
        |    {
        |      "script": {
        |        "source": "ctx.language = [:];ctx.language[ctx.lang] = ctx.remove('my_field')"
        |      }
        |    }
        |  ]
        | }
      """.stripMargin
    val stringEntity = HttpEntity(body)

    val createTemplateTask = Task.deferFuture {
      elasticClient.execute(
        createIndexTemplate("default", "*").create(true).settings(
          Map("number_of_shards" -> 3, "number_of_replicas" -> 0)
        ).order(0)
      )
    }

    val createIndexTask = Task.deferFuture {
      elasticClient.execute {
        createIndex("docs1").mappings(
          mapping("doc").fields(
            textField("text")
          )
        )
        createIndex("docs2").mappings(
          mapping("doc").fields(
            textField("text")
          )
        )
        createRepository("offline-backup", "fs").settings(Map(
          "location" -> ".",
          "compress" -> "false",
          "max_snapshot_bytes_per_sec" -> "400mb"))
      }
    }

    val createLangPipeline = Task.async {
      cb: Callback[Throwable, HttpResponse] =>
        restClient.send(ElasticRequest("PUT", "_ingest/pipeline/langdetect-analyzer-pipeline", stringEntity), cb)
    }

    createLangPipeline.flatMap(_ => createTemplateTask).flatMap(_ => createIndexTask).map(_ => elasticClient)
  }

  def endStream[T](s: Stream[T], elasticClient: Task[ElasticClient], o: OfflineIndexPartition): Stream[T] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    implicit val ss: SchedulerService = Scheduler.io()

    s match {
      case Stream.Empty =>
        elasticClient.flatMap { ec =>
          Task.deferFuture {
            ec.execute {
              flushIndex(o.indices)
              forceMerge(o.indices)
              createSnapshot(s"offline-snapshot", "offline-backup")
                .indices(Indexes(o.indices)).waitForCompletion(true)
            }
          }
        }.flatMap(_ => liftDataSegmentToHDFS(o)).runSyncUnsafe()
        Stream.empty[T]
      case head #:: next => head #:: endStream(next, elasticClient, o)
    }
  }


  def liftDataSegmentToHDFS(o: OfflineIndexPartition, conf: Configuration = new Configuration()): Task[Unit] = o match {
    case OfflineIndexPartition(partitionId, dest, localPath, _) =>

      val fs: FileSystem = FileSystem.get(conf)
      import scala.collection.JavaConversions._
      val localRepo = localPath.resolve("repo")
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

class IteratorWrapper[+E](ri: RemoteIterator[E]) extends Iterator[Either[Exception, E]] {
  override def hasNext: Boolean = try {
    ri.hasNext
  } catch {
    case t: Exception => false
  }

  override def next(): Either[Exception, E] = try {
    Right(ri.next())
  } catch {
    case e: Exception => Left(e)
  }
}

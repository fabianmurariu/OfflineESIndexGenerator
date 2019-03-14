package com.github.fabianmurariu.esoffline

import com.github.fabianmurariu.esoffline.Hdfs.OfflineIndexPartition
import com.optimaize.langdetect.i18n.LdLocale
import com.sksamuel.elastic4s.Indexes
import com.sksamuel.elastic4s.http._
import io.circe.generic.auto._
import io.circe.parser._
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.DirectoryFileFilter.DIRECTORY
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.ngram.NgramExtractors
import com.optimaize.langdetect.profiles.LanguageProfileReader
import com.optimaize.langdetect.text.CommonTextObjectFactories
import org.apache.spark.TaskContext

import scala.io.Source

object EsLang {

  val supportedLang: Map[String, String] = Seq("arabic" -> "ar", "basque" -> "eu",
    "bengali" -> "bn", "brazilian" -> "pt", "bulgarian" -> "bg", "catalan" -> "ca",
    "czech" -> "cs", "danish" -> "da", "dutch" -> "nl",
    "english" -> "en", "finnish" -> "fi", "french" -> "fr",
    "german" -> "de", "greek" -> "el", "hindi" -> "hi", "hungarian" -> "hu",
    "indonesian" -> "id", "irish" -> "ga", "italian" -> "it", "latvian" -> "lv",
    "lithuanian" -> "lt", "norwegian" -> "no", "persian" -> "fa", "portuguese" -> "pt",
    "romanian" -> "ro", "russian" -> "ru", "spanish" -> "es",
    "swedish" -> "sv", "turkish" -> "tr", "thai" -> "th").map(_.swap).toMap

  def createPipeline(elasticClient: ElasticClient): Task[ElasticClient] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val createTemplateTask = Task.deferFuture {
      elasticClient.execute(
        createIndexTemplate("default", "*").create(true).settings(
          Map("number_of_shards" -> 3, "number_of_replicas" -> 0)
        ).order(0)
      )
    }

    val createIndexTask = Task.deferFuture {
      val langFields = supportedLang.values.map(lang => textField(s"field_$lang")).toList
      elasticClient.execute {
        createIndex("docs").mappings(
          mapping("doc").fields(List(
            textField("text").analyzer("standard"),
            textField("lang"),
            textField("url"),
            textField("host"),
            textField("mime"),
            longField("length"),
            dateField("date")) ++ langFields
          )
        )
        createRepository("offline-backup", "fs").settings(Map(
          "location" -> ".",
          "compress" -> "false",
          "max_snapshot_bytes_per_sec" -> "400mb"))
      }
    }

    createTemplateTask.flatMap(_ => createIndexTask).map(_ => elasticClient)
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

  def langDetect: String => Option[LdLocale] = {
    //load all languages://load all languages:

    val languageProfiles = new LanguageProfileReader().readAllBuiltIn

    //build language detector:
    val languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard).withProfiles(languageProfiles).build

    //create a text object factory
    val textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText

    { text: String =>
      val textObject = textObjectFactory.forText(text)
      val maybeLang = languageDetector.detect(textObject)
      if (maybeLang.isPresent) Some(maybeLang.get())
      else None
    }
  }

}

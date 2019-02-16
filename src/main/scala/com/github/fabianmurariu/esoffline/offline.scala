package com.github.fabianmurariu.esoffline

import java.io.ByteArrayOutputStream
import java.net.URI
import java.nio.file.Paths

import com.github.fabianmurariu.esoffline.Hdfs.OfflineIndexPartition
import com.sksamuel.elastic4s.http.{ElasticClient, HttpClient}
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.{CanBlock, SchedulerService}
import org.apache.hadoop.io.Text
import org.apache.spark.TaskContext
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.archive.io.{ArchiveReader, ArchiveRecord}
import warc.WARCFileInputFormat

object offline {

  case class OfflineResult[T](result: Option[T], failed: Option[String] = None)

  implicit class OfflineIndexDatasetOps[T](val ds: Dataset[T]) {

    def indexPartitionHttp[U](batchSize: Int, dest: URI, indices: Seq[String], init: ElasticClient => Task[ElasticClient] = Task(_))
                             (f: (ElasticClient, Seq[T]) => U)(implicit E: Encoder[OfflineResult[U]]): Dataset[OfflineResult[U]] = {
      ds.mapPartitions {
        ts =>
          val tc = TaskContext.get()
          val localEsPath = Paths.get(s"offline_worker_${tc.partitionId()}_${tc.attemptNumber()}")
          val client: Task[ElasticClient] = EsNode.http(tc.partitionId(), tc.attemptNumber(), localEsPath).flatMap(init).memoize
          implicit val s: SchedulerService = Scheduler.io()
          implicit val cb: CanBlock = CanBlock.permit

          val iterator = ts.grouped(batchSize)
            .map {
              t: Seq[T] =>
                client.map(tc => f(tc, t)).attempt.runSyncUnsafe() match {
                  case Right(v) => OfflineResult(Some(v))
                  case Left(failure) =>
                    OfflineResult(Option.empty[U], Some(failure.getMessage))
                }
            }

          EsLang.endStream[OfflineResult[U]](
            iterator.toStream, client, OfflineIndexPartition(TaskContext.getPartitionId(), dest, localEsPath, indices)).iterator
      }
    }
  }

  def loadWETFiles(path: String)(implicit spark: SparkSession): Dataset[WebDocument] = {
    import spark.implicits._

    import scala.collection.JavaConversions.iterableAsScalaIterable
    spark.sparkContext.newAPIHadoopFile[Text, ArchiveReader, WARCFileInputFormat]("data/*.warc.wet.gz")
      .flatMap {
        case (file, archive: ArchiveReader) =>
          archive.map {
            record: ArchiveRecord =>
              val header = record.getHeader
              val os = new ByteArrayOutputStream()
              record.dump(os)
              os.close()
              val text = new String(os.toByteArray, "UTF-8")
              WebDocument(file.toString, header.getDate, header.getContentLength, header.getMimetype, header.getUrl, header.getVersion, header.getRecordIdentifier, text)
          }
      }.toDS()

  }

  def loadIndexFiles(path: String)(implicit spark: SparkSession): Dataset[WARCIndexDoc] = {
    import spark.implicits._
    import org.apache.spark.sql.functions.get_json_object
    val frame = spark.read.text(path).as[String].map {
      line =>
        val from = line.indexOf("{")
        line.substring(from)
    }.toDF("value")
    frame.show(5, truncate = false)
    frame
      .select(get_json_object('value, "$.languages").as("languages"), get_json_object('value, "$.url").as("url"))
      .as[WARCIndexDoc]
  }

  trait ESRouting[T] {
    def route(t: T): String
  }

}

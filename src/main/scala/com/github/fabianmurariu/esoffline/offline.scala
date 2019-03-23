package com.github.fabianmurariu.esoffline

import java.io.ByteArrayOutputStream
import java.net.URI
import java.nio.file.Paths

import com.github.fabianmurariu.esoffline.Hdfs.OfflineIndexPartition
import com.sksamuel.elastic4s.http.ElasticClient
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

    def indexPartitionHttp2[U, X](batchSize: Int, dest: URI)
                                 (implicit O: OfflineIndexable[X, U, T], E: Encoder[OfflineResult[U]]): Dataset[OfflineResult[U]] = {
      ds.mapPartitions {
        ts: Iterator[T] =>
          val x = O.partitionContext
          val tc = TaskContext.get()
          val localEsPath = Paths.get(s"offline_worker_${tc.partitionId()}_${tc.attemptNumber()}")
          val client: Task[ElasticClient] = EsNode.http(tc.partitionId(), tc.attemptNumber(), localEsPath).flatMap(O.init).memoize
          implicit val s: SchedulerService = Scheduler.io()
          implicit val cb: CanBlock = CanBlock.permit

          val iterator = ts.grouped(batchSize)
            .map {
              t: Seq[T] =>
                client.map(elClient => O.indexBatch(elClient, x, t)).attempt.runSyncUnsafe() match {
                  case Right(v) => OfflineResult(Some(v))
                  case Left(failure) =>
                    OfflineResult(Option.empty[U], Some(failure.getMessage))
                }
            }

          EsLang.endStream[OfflineResult[U]](
            iterator.toStream, client, OfflineIndexPartition(TaskContext.getPartitionId(), dest, localEsPath, O.indices)).iterator
      }
    }

  }

  def loadWETFiles(path: String)(implicit spark: SparkSession): Dataset[WebDocument] = {
    import spark.implicits._

    import scala.collection.JavaConversions.iterableAsScalaIterable
    spark.sparkContext.newAPIHadoopFile[Text, ArchiveReader, WARCFileInputFormat](path)
      .flatMap {
        case (file, archive: ArchiveReader) =>
          archive.map {
            record: ArchiveRecord =>
              val header = record.getHeader
              val os = new ByteArrayOutputStream()
              record.dump(os)
              os.close()
              val text = new String(os.toByteArray, "UTF-8")
              WebDocument(origin = file.toString,
                date = header.getDate,
                length = header.getContentLength,
                mime = header.getMimetype,
                url = header.getUrl,
                version = header.getVersion,
                recordId = header.getRecordIdentifier,
                text = text,
                topDomain = WebDocument.privateTopDomain(header.getUrl))
          }
      }.toDS()

  }

}

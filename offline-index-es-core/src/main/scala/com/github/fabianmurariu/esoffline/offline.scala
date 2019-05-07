package com.github.fabianmurariu.esoffline

import java.net.URI
import java.nio.file.Paths

import com.sksamuel.elastic4s.http.ElasticClient
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.{CanBlock, SchedulerService}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object offline {

  case class OfflineResult[T](result: Option[T], failed: Option[String] = None)

  implicit class OfflineIndexDatasetOps[T](val ds: Dataset[T]) {

    def indexPartitionHttp2[U, X](batchSize: Int, dest: URI, conf:OfflineIndexConf)
                                 (implicit O: OfflineIndexable[X, U, T], E: Encoder[OfflineResult[U]]): Dataset[OfflineResult[U]] = {
      ds.mapPartitions {
        ts: Iterator[T] =>
          val pc = O.partitionContext
          val tc = TaskContext.get()
          val localEsPath = Paths.get(s"offline_worker_${tc.partitionId()}_${tc.attemptNumber()}")
          val client: Task[ElasticClient] = EsNode.http(tc.partitionId(), tc.attemptNumber(), localEsPath).flatMap(O.init(conf)).memoize
          implicit val s: SchedulerService = Scheduler.io()
          implicit val cb: CanBlock = CanBlock.permit

          tc.addTaskCompletionListener{
            tc0 =>
              EsNode.finalizeTask(client, OfflineIndexPartition(tc0.partitionId(), dest, localEsPath, O.indices))
          }

          ts.grouped(batchSize)
            .map {
              t: Seq[T] =>
                client.map(elClient => O.indexBatch(elClient, pc, t)).attempt.runSyncUnsafe() match {
                  case Right(v) => OfflineResult(Some(v))
                  case Left(failure) =>
                    OfflineResult(Option.empty[U], Some(failure.getMessage))
                }
            }
      }
    }

  }

  sealed trait Unrecoverable
  case class FailedToCreateIndex(msg:String) extends Exception(msg) with Unrecoverable
  case class FailedToCreateIndexTemplate(msg:String) extends Exception(msg) with Unrecoverable
  case class FailedToSnapshotIndex(msg:String) extends Exception(msg) with Unrecoverable
}

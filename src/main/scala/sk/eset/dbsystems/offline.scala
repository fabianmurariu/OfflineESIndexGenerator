package sk.eset.dbsystems

import com.sksamuel.elastic4s.TcpClient
import monix.execution.Scheduler
import monix.execution.schedulers.{CanBlock, SchedulerService}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{Dataset, Encoder}
import sk.eset.dbsystems.spark.OfflineResult

object offline {

  implicit class OfflineIndexDatasetOps[T](val ds: Dataset[T]) extends AnyVal {
    def indexPartition[U](f: (TcpClient, T) => U)(implicit E: Encoder[OfflineResult[U]]): Dataset[OfflineResult[U]] = {

      ds.mapPartitions {
        ts =>
          val tc = TaskContext.get()
          val client = EsNode.tcp(tc.partitionId(), tc.attemptNumber(), None).memoize
          implicit val s: SchedulerService = Scheduler.io()
          implicit val cb: CanBlock = CanBlock.permit
          ts.map {
            t =>
              client.map(tc => f(tc, t)).attempt.runSyncUnsafe() match {
                case Right(v) => OfflineResult(Some(v))
                case Left(failure) =>
                  failure.printStackTrace()
                  OfflineResult(None, Some(failure.getMessage))
              }
          }
      }
    }
  }

  def loadWETFiles(path:String) = ???

}

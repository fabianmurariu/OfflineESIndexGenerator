package sk.eset.dbsystems

import java.nio.file.{Files, Path, Paths}

import com.sksamuel.elastic4s.TcpClient
import com.sksamuel.elastic4s.embedded.LocalNode
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.{CanBlock, SchedulerService}
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.sql.{Dataset, Encoder}
import org.elasticsearch.common.settings.Settings
import sk.eset.dbsystems.spark.OfflineResult

object EsNode {

  type Io[A] = Task[A]

  @transient lazy val LOG: Logger = Logger.getLogger(this.getClass)

  private def defaultSettings(root: Path, partitionId: Int, attemptId: Int): Task[Settings.Builder] = Task {
    val clusterName = s"offline-cluster"
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    Settings.builder
      .put("http.enabled", false)
      .put("transport.type", "local")
      .put("processors", 1)
      .put("node.name", s"EsNode_${partitionId}_$attemptId")
      .put("cluster.name", clusterName)
      .put("path.data", s"${root.resolve("data")}")
      //.put("bootstrap.memory_lock", true)
      .put("indices.store.throttle.type", "none")
      .put("indices.memory.index_buffer_size", "5%")
      .put("indices.fielddata.cache.size", "0%")
      .put("path.repo", s"${root.resolve("repo")}")
      .put("path.home", s"${root.resolve("home")}")
  }

  private def localNode(settings: Settings): Task[TcpClient] = Task {
    val ln = LocalNode(settings)
    ln.start()
    LOG.info("STUFF HAPPENED")
    println("YEAH ME TOO!")
    ln
  }.bracket(ln => Task(ln.elastic4sclient(true)))(ln => Task(ln.stop(true)))

  def tcp(partitionId: Int, attemptId: Int, localPath: Option[Path], additionalSettings: (String, String)*): Task[TcpClient] = {
    for {
      path <- Task(localPath.getOrElse(Paths.get(s"offline_worker_${partitionId}_$attemptId"))).map {
        case p if Files.exists(p) =>
          FileUtils.forceDelete(p.toFile)
          Files.createDirectory(p)
        case p =>
          Files.createDirectory(p)
      }
      builder <- defaultSettings(path, partitionId, attemptId)
      settings <- Task(additionalSettings.foldLeft(builder) { case (b, (k, v)) => b.put(k, v) }.build())
      client <- localNode(settings)
    } yield client
  }
}

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


}

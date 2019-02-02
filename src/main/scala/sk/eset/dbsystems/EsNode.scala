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
    ln
  }.bracket(ln => Task(ln.elastic4sclient(true))) { ln =>
    LOG.info("STOPPING ES NODE")
    Task(ln.stop(true))
  }

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



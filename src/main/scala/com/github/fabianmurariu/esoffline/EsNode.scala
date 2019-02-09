package com.github.fabianmurariu.esoffline

import java.nio.file.{Files, Path, Paths}

import com.sksamuel.elastic4s.embedded.LocalNode
import com.sksamuel.elastic4s.http.{ElasticClient, HttpClient}
import monix.eval.Task
import org.apache.commons.io.FileUtils
import org.apache.http.HttpHost
import org.apache.log4j.Logger
import org.elasticsearch.client.{RestClient, RestClientBuilder}
import org.elasticsearch.common.settings.Settings

object EsNode {

  @transient lazy val LOG: Logger = Logger.getLogger(this.getClass)

  private def defaultSettings(root: Path, partitionId: Int, attemptId: Int, http: Boolean = true): Task[Settings.Builder] = Task {
    val clusterName = s"offline-cluster-$partitionId-$attemptId"
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    Settings.builder
      .put("http.enabled", http)
      .put("processors", 1)
      .put("node.name", s"EsNode_${partitionId}_$attemptId")
      .put("cluster.name", clusterName)
      .put("node.master", true)
      .put("discovery.type", "single-node")
      .put("path.data", s"${root.resolve("data")}")
      .put("indices.memory.index_buffer_size", "5%")
      .put("indices.fielddata.cache.size", "0%")
      .put("path.repo", s"${root.resolve("repo")}")
      .put("path.home", s"${root.resolve("home")}")
  }

  private def localNodeWithHttp(settings: Settings): Task[ElasticClient] = Task {
    val ln = LocalNode(settings)
    ln.start()
    ln
  }.map{ln => ElasticClient.fromRestClient(RestClient.builder(new HttpHost(ln.ip, ln.port)).build()) }


  def http(partitionId: Int, attemptId: Int, localPath: Option[Path], additionalSettings: (String, String)*): Task[ElasticClient] = {
    {
      for {
        path <- Task(localPath.getOrElse(Paths.get(s"offline_worker_${partitionId}_$attemptId"))).map {
          case p if Files.exists(p) =>
            FileUtils.forceDelete(p.toFile)
            Files.createDirectory(p)
          case p =>
            Files.createDirectory(p)
        }
        builder <- defaultSettings(path, partitionId, attemptId, true)
        settings <- Task(additionalSettings.foldLeft(builder) { case (b, (k, v)) => b.put(k, v) }.build())
        client <- localNodeWithHttp(settings)
      } yield client
    }

  }
}

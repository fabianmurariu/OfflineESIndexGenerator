package com.github.fabianmurariu.esoffline

import java.nio.file.{Files, Path}

import com.sksamuel.elastic4s.embedded.{InternalLocalNode, LocalNode}
import com.sksamuel.elastic4s.http.ElasticClient
import monix.eval.Task
import org.apache.commons.io.FileUtils
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
}

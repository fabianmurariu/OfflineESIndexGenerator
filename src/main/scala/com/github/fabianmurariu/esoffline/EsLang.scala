package com.github.fabianmurariu.esoffline

import java.net.URI

import com.github.fabianmurariu.esoffline.Hdfs.OfflineIndexPartition
import com.sksamuel.elastic4s.{Index, Indexes}
import com.sksamuel.elastic4s.http._
import monix.eval.Task
import monix.execution.schedulers.SchedulerService
import monix.execution.{Callback, Scheduler}
import org.apache.hadoop.conf.Configuration

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

  def uploadToDest(o: OfflineIndexPartition): Task[Unit] =
    new Hdfs.HdfsRepo(new Configuration()).copyToHDFS(o)

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
        }.flatMap(_ => uploadToDest(o)).runSyncUnsafe()
        Stream.empty[T]
      case head #:: next => head #:: endStream(next, elasticClient, o)
    }
  }

}

package com.github.fabianmurariu.esoffline

import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.snapshots.CreateSnapshotResponse
import monix.eval.Task
import monix.execution.schedulers.{ExecutorScheduler, SchedulerService}
import monix.execution.{Callback, Scheduler}
import org.apache.spark.TaskContext

import scala.concurrent.Future

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
        createIndex("docs").mappings(
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

  def endStream[T](s: Stream[T], elasticClient: Task[ElasticClient]): Stream[T] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    implicit val ss: SchedulerService = Scheduler.io()

    s match {
      case Stream.Empty =>
        elasticClient.flatMap { ec =>
          Task.deferFuture {
            ec.execute {
              flushIndex("docs")
              forceMerge("docs")
              createSnapshot(s"offline-snapshot", "offline-backup").index(Index("docs")).waitForCompletion(true)
            }
          }
        }.map(println).runSyncUnsafe()
        Stream.empty[T]
      case head #:: next => head #:: endStream(next, elasticClient)
    }
  }

}

package com.github.fabianmurariu.esoffline

import com.sksamuel.elastic4s.http.index.CreateIndexResponse
import com.sksamuel.elastic4s.http._
import monix.eval.Task
import monix.execution.Callback

import scala.annotation.tailrec
import scala.concurrent.Future

object EsLang {

  def createPipeline(elasticClient: ElasticClient): Task[ElasticClient] = {
    import com.sksamuel.elastic4s.mappings.FieldType._

    import io.circe.generic.auto._
    import com.sksamuel.elastic4s.circe._
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

    val createIndexTask =  Task.deferFuture {
      elasticClient.execute {
        createIndex("docs").mappings(
          mapping("doc").fields(
            textField("text")
          )
        ).shards(3)
      }
    }

    val createLangPipeline = Task.async {
      cb: Callback[Throwable, HttpResponse] =>
        restClient.send(ElasticRequest("PUT", "_ingest/pipeline/langdetect-analyzer-pipeline", stringEntity), cb)
    }

    createLangPipeline.flatMap(_ => createIndexTask).map(_ => elasticClient)
  }

  def endStream[T](s:Stream[T], elasticClient: Task[ElasticClient]):Stream[T] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    s match {
      case Stream.empty =>
        elasticClient.map{
          ec => ec.execute{
            createRepository("offline-backup", "fs").settings(Map(
              "location" -> ".",
              "compress" -> "false",
              "max_snapshot_bytes_per_sec" -> "400mb"))
          }
        }
        Stream.empty
      case head #:: next => head #:: endStream(next, elasticClient)
    }
  }

}

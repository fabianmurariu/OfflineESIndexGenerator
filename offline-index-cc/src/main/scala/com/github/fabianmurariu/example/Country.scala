package com.github.fabianmurariu.example

import java.net.URI

import com.github.fabianmurariu.esoffline.{OfflineConf, OfflineIndex}
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.index.CreateIndexResponse
import com.sksamuel.elastic4s.http.{ElasticClient, RequestFailure, RequestSuccess}
import monix.eval.Task
import org.apache.spark.sql.SparkSession

case class Country(name: String, capital: String)

object Country {
  implicit val countryIsOfflineIndexable: OfflineIndex[Unit, BulkResponse, Country] = new OfflineIndex[Unit, BulkResponse, Country] {
    override def partitionContext: Unit = ()

    import com.sksamuel.elastic4s.http.ElasticDsl._

    override def init(shards: OfflineConf)(tc: ElasticClient): Task[ElasticClient] = Task.deferFuture {
      tc.execute(
        createIndex("places").mappings(
          mapping("country").fields(List(
            textField("name"),
            textField("capital")
          ))))
    }.flatMap {
      case _: RequestSuccess[CreateIndexResponse] => Task(tc)
      case f: RequestFailure => Task.raiseError(new RuntimeException(s"${f.error}"))
    }

    override def indices: Seq[String] = Seq("places")

    override def indexBatch(es: ElasticClient, c: Unit, ts: Seq[Country]): BulkResponse = es.execute(
      bulk(ts.map(
        c => indexInto("places" / "country")
          .fields("name" -> c.name, "capital" -> c.capital)
      ))).await.result
  }

  def example(implicit spark:SparkSession)= {
    import com.github.fabianmurariu.esoffline.offline._
    import spark.implicits._
    val countries= Seq(Country("Romania", "Bucharest")).toDS
    countries.indexPartitionHttp2(20, new URI("countriesRepo"), OfflineConf(store = true, 64))
  }
}

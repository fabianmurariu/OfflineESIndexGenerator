package com.github.fabianmurariu.esoffline

import com.sksamuel.elastic4s.http.ElasticClient
import monix.eval.Task

trait OfflineIndexable[C, B, A] extends Serializable {

  def routing(t: A): String

  def partitionContext: C

  def init(shards:Int)(tc: ElasticClient): Task[ElasticClient]

  def indices: Seq[String]

  def indexBatch(es: ElasticClient, c: C, ts: Seq[A]): B

}

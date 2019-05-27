package com.github.fabianmurariu.esoffline

import com.sksamuel.elastic4s.http.ElasticClient
import monix.eval.Task

trait OfflineIndex[Ctx, Res, Doc] extends Serializable {

  def partitionContext: Ctx

  def init(shards: OfflineConf)(tc: ElasticClient): Task[ElasticClient]

  def indices: Seq[String]

  def indexBatch(es: ElasticClient, c: Ctx, ts: Seq[Doc]): Res

}

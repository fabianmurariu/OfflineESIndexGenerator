package com.github.fabianmurariu.esoffline

import com.optimaize.langdetect.i18n.LdLocale
import com.sksamuel.elastic4s.http.ElasticClient
import monix.eval.Task

case class WebDocument(origin: String, date: String, length: Long, mime: String, url: String, version: String, recordId: String, text: String)

object WebDocument {
  implicit val offlineIndexable: OfflineIndexable[String => Option[LdLocale], Int, WebDocument] = new OfflineIndexable[String => Option[LdLocale], Int, WebDocument] {
    override def routing(t: WebDocument): String = "<CONSTANT>"

    override def partitionContext: String => Option[LdLocale] = ???

    override def init(tc: ElasticClient): Task[ElasticClient] = ???

    override def indices: Seq[String] = ???

    override def indexBatch(es: ElasticClient, c: String => Option[LdLocale], ts: Seq[WebDocument]): Int = ???
  }
}

case class WARCIndexDoc(languages: String, url: String)

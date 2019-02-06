package com.github.fabianmurariu.esoffline

case class WebDocument(origin: String, date: String, length: Long, mime: String, url: String, version: String, recordId: String, text: String)

case class WARCIndexDoc(languages: String, url: String)

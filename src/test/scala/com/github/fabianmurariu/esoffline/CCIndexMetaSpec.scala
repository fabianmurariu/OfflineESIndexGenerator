package com.github.fabianmurariu.esoffline

import java.nio.file.Paths
import java.util

import com.github.fabianmurariu.esoffline.FileLocator.{CDXMeta, LocalIndexMeta, Meta}
import org.apache.commons.collections4.trie.PatriciaTrie
import org.scalatest.{FlatSpec, FunSuite, Matchers}

import scala.collection.mutable

class CCIndexMetaSpec extends FlatSpec with Matchers {

  "LocalCCIndexMeta" should "find the paths for some URIs" in {

    LocalIndexMeta(Paths.get("/home/murariuf/Source/OfflineESIndexGenerator/data"))
      .reversedUrls.firstKey() shouldBe "0,0,0,1"

  }

  it should "find http://news.bbc.co.uk in files containing UK domains" in {
    val actual = LocalIndexMeta(Paths.get("/home/murariuf/Source/OfflineESIndexGenerator/data")).pathsFor(
      Seq("news.bbc.co.uk")
    )
    actual.take(10).foreach(println)
    assert(actual.head.surt.startsWith("uk,co,bbc,news"))

  }

}

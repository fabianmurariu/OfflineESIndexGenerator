package com.github.fabianmurariu.esoffline

import org.scalatest.{FlatSpec, Matchers}

class IndexTheWorldSpec extends FlatSpec with Matchers {
  //TODO: make this integration test

  "IndexTheWorld" should "find the files from the parquet index and create the snapshot as per configuration" in {
    val home = System.getProperty("user.home") + "/Source"
    IndexTheWorld.main(Array(
      "--indices", "CC-MAIN-2019-09",
      "--hosts", "huffingtonpost.ca, epfl.ch, people.com.cn,uol.com.br, apple.com,alibaba.com",
      "--partitions", "20",
      "--cc-index", s"$home/OfflineESIndexGenerator/data/index-demo",
      "--snapshot-out", s"$home/OfflineESIndexGenerator/data/repo",
      "--archive-root", s"$home/OfflineESIndexGenerator/data",
      "--local","true"
    ))
  }
}

package com.github.fabianmurariu.esoffline

import org.scalatest.{FlatSpec, Matchers}

class IndexTheWorldSpec extends FlatSpec with Matchers {

  "IndexTheWorld" should "find the files from the parquet index and create the snapshot as per configuration" in {
    IndexTheWorld.main(Array(
      "--indices", "CC-MAIN-2019-09",
      "--hosts", "huffingtonpost.ca, epfl.ch, people.com.cn,uol.com.br, apple.com,alibaba.com",
      "--partitions", "4",
      "--cc-index", s"../data/index-small",
      "--snapshot-out", s"../data/repo",
      "--archive-root", s"../data",
      "--local","true"
    ))
  }
}

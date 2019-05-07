package com.github.fabianmurariu.esoffline

case class OfflineIndexConf(indices: Seq[String] = Seq.empty,
                            hosts: Set[String] = Set.empty,
                            partitions: Int = 0,
                            where: Option[String] = None,
                            indexRoot: String = "s3://commoncrawl/cc-index/table/cc-main/warc/",
                            esSnapshotPath: String = "s3://offline-elastic-world-index/repo",
                            filesRoot: String = "s3://commoncrawl/",
                            local: Boolean = false,
                            store: Boolean = false,
                            `type`: String = "wet")

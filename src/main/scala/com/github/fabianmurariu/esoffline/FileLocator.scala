package com.github.fabianmurariu.esoffline

import org.apache.spark.sql.SparkSession

object FileLocator {

  def loadWETFromIndex(config: OfflineIndexConf)(implicit spark: SparkSession): Vector[String] = {

    import spark.implicits._
    val indices = config.indices
    val frame = spark.read.parquet(config.indexRoot)
      .where('crawl.isin(indices: _*))
      .where('subset === "warc")
      .where('fetch_status === 200)

    val frameDomainSubset = if (config.hosts.isEmpty) frame
    else frame.where('url_host_registered_domain.isin(config.hosts.toSeq: _*))

    val index = if (config.where.isEmpty) frameDomainSubset
    else frameDomainSubset.where(config.where.get)

    val outFiles = index
      .select("warc_filename")
      .distinct()
      .as[String]
      .map(_.replaceAll("/warc/", "/wet/").replaceAll("warc.gz", "warc.wet.gz"))
      .collect().toVector

    assert(outFiles.nonEmpty, s"No warc files found for $config")

    outFiles
  }

}

package com.github.fabianmurariu.esoffline

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.hadoop.io.Text
import org.archive.io.ArchiveReader
import org.archive.io.{ArchiveReader, ArchiveRecord}
import warc.WARCFileInputFormat

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

  def loadWETFiles(path: String)(implicit spark: SparkSession): Dataset[WebDocument] = {
    import spark.implicits._

    import scala.collection.JavaConversions.iterableAsScalaIterable
    spark.sparkContext.newAPIHadoopFile[Text, ArchiveReader, WARCFileInputFormat](path)
      .flatMap {
        case (file, archive: ArchiveReader) =>
          archive.map {
            record: ArchiveRecord =>
              val header = record.getHeader
              val os = new ByteArrayOutputStream()
              record.dump(os)
              os.close()
              val text = new String(os.toByteArray, "UTF-8")
              WebDocument(origin = file.toString,
                date = header.getDate,
                length = header.getContentLength,
                mime = header.getMimetype,
                url = header.getUrl,
                version = header.getVersion,
                recordId = header.getRecordIdentifier,
                text = text,
                topDomain = WebDocument.privateTopDomain(header.getUrl))
          }
      }.toDS()
  }
}

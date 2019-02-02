package sk.eset.dbsystems

import java.io.ByteArrayOutputStream
import java.nio.file.Paths

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}
import com.sksamuel.elastic4s.TcpClient
import com.sksamuel.elastic4s.ElasticDsl._
import org.apache.hadoop.io.Text
import org.archive.io.{ArchiveReader, ArchiveRecord}
import warc.WARCFileInputFormat

class offlineTest extends FlatSpec with Matchers {

  "offline-indexing" should "trigger the offline indexing of a dataset" in {
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    import spark.implicits._
    import offline._
    import scala.collection.JavaConversions.{iterableAsScalaIterable, mapAsScalaMap}

    spark.sparkContext.newAPIHadoopFile[Text, ArchiveReader, WARCFileInputFormat]("data/*.warc.wet.gz")
      .flatMap {
        case (file, archive: ArchiveReader) =>
          archive.map {
            record: ArchiveRecord =>
              val header = record.getHeader
              val os = new ByteArrayOutputStream()
              record.dump(os)
              os.close()
              val text = new String(os.toByteArray, "UTF-8")
              WebDocument(file.toString, header.getDate, header.getContentLength, header.getMimetype, header.getUrl, header.getVersion, header.getRecordIdentifier, text)
          }
      }.toDS()
      .show(false)

  }

}

case class WebDocument(origin: String, date: String, length: Long, mime: String, url: String, version: String, recordId: String, text: String)

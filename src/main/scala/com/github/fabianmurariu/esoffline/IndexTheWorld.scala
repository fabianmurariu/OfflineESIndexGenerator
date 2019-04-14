package com.github.fabianmurariu.esoffline

import java.net.URI

import com.optimaize.langdetect.i18n.LdLocale
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object IndexTheWorld {

  def main(args: Array[String]): Unit = {
    parseArgs(args) match {
      case Some(conf) =>
        import offline._
        val builder = SparkSession.builder().appName("IndexTheWorld")
        implicit val spark: SparkSession = if (conf.local) builder.master("local[4]").getOrCreate() else builder.getOrCreate()
        import spark.implicits._

        val repo = new URI(conf.esSnapshotPath)
        implicit val fs: FileSystem = new Path(repo.toString).getFileSystem(spark.sparkContext.hadoopConfiguration)
        val files = FileLocator.loadWETFromIndex(conf)

        val existingFiles = files.flatMap { p =>
          val fullPath = new Path(conf.filesRoot, p)
          if (conf.filesRoot.startsWith("s3") || fs.exists(fullPath)) List(fullPath)
          else Nil
        }

        println(s"${conf.hosts.mkString("[",",","]")} found on ${existingFiles.length} files")
        existingFiles.foreach(println)

        loadWETFiles(existingFiles.mkString(","))
          .filter(_.topDomain.exists(td => conf.hosts(td)))
          .coalesce(conf.partitions)
          .indexPartitionHttp2[Int, String => Option[LdLocale]](20, repo, conf.partitions)
          .cache()
          .count

        implicit val scheduler: SchedulerService = Scheduler.io()
        EsLang.renameIndicesAndSnapshot(repo.toString).runSyncUnsafe()

      case None => throw new IllegalArgumentException(s"Unable to parse args ${args.toVector}")
    }

  }

  def parseArgs(args: Array[String]): Option[OfflineIndexConf] = {

    val parser = new scopt.OptionParser[OfflineIndexConf]("index-the-world") {

      opt[Seq[String]]("indices") required() action {
        (x, c) => c.copy(indices = x.map(_.trim))
      } text "eg: CC-MAIN-2019-04,CC-MAIN-2018-51"

      opt[Seq[String]](name = "hosts") optional() action {
        (x: Seq[String], c) => c.copy(hosts = x.map(_.trim).toSet)
      } text "ec: ebay.com,amazon.co.uk"

      opt[Int](name = "partitions") required() action {
        (x, c) => c.copy(partitions = x)
      } text "number of partitions and therefor shards in ES"

      opt[String]("where") optional() action {
        (x, c) => c.copy(where = Some(x))
      } text "additional filter to be dropped in the where clause of the CC index"

      opt[String]("cc-index") optional() action {
        (x, c) => c.copy(indexRoot = x)
      }

      opt[String]("archive-root") optional() action {
        (x, c) => c.copy(filesRoot = x)
      }

      opt[String]("snapshot-out") required() action {
        (x, c) => c.copy(esSnapshotPath = x)
      }

      opt[Boolean]("local") optional() action {
        (x, c) => c.copy(local = x)
      }

      opt[Boolean]("store") optional() action {
        (x, c) => c.copy(store = x)
      }



    }

    parser.parse(args, OfflineIndexConf())
  }

}

case class OfflineIndexConf(indices: Seq[String] = Seq.empty,
                            hosts: Set[String] = Set.empty,
                            partitions: Int = 0,
                            where: Option[String] = None,
                            indexRoot: String = "s3://commoncrawl/cc-index/table/cc-main/warc/",
                            esSnapshotPath: String = "s3://offline-elastic-world-index/repo",
                            filesRoot: String = "s3://commoncrawl/",
                            local: Boolean = false,
                            store: Boolean = false)

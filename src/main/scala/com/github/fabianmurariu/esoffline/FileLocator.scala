package com.github.fabianmurariu.esoffline

import java.net.URI
import java.nio.file.Path
import java.util.Comparator

import org.apache.commons.collections4.trie.PatriciaTrie
import org.apache.hadoop.fs.{Path => HPath}

import scala.io.Source

object FileLocator {


  case class Meta(file: Seq[String], from: Int, to: Int)

  case class CDXMeta(surt: String, meta: Option[Meta] = None)

  sealed trait CCIndexMeta {
    self =>

    def pathsFor(hosts: Seq[String]): Seq[CDXMeta] = self match {
      case li@LocalIndexMeta(_) =>
        import scala.collection.JavaConversions._

        val pt = li.reversedUrls

        hosts.flatMap {
          host =>
            val reverseUrl = new URI(s"http://$host/").getHost.split("\\.").reverse.mkString(",")
            val strings = pt.prefixMap(reverseUrl).values()
            if (strings.nonEmpty) strings
            else {
              val value = pt.selectKey(reverseUrl)
              Seq(pt.get(pt.previousKey(value)))
            }
        }
      case _ => ???
    }

  }

  case class LocalIndexMeta(root: Path) extends CCIndexMeta {

    def truncateSurt(meta: CDXMeta): String = meta.surt.takeWhile(_ != ')')

    @transient lazy val reversedUrls: PatriciaTrie[CDXMeta] = {
      import scala.collection.JavaConversions.mapAsJavaMap

      val groupedMetas = Source.fromFile(root.resolve("indexes/cluster.idx").toFile).getLines().flatMap {
        _.split("\\s+").toList match {
          case List(reversedUrl, _, file, from, to, _) =>
            List(CDXMeta(reversedUrl, Some(Meta(Seq(file), from.toInt, to.toInt))))
          case _ => List.empty
        }
      }.toVector.groupBy(truncateSurt).map {
        case (reversedUrl, vals) =>
          val minFrom = vals.collect { case i if i.meta.isDefined => i.meta.get.from }.min
          val maxTo = vals.collect { case i if i.meta.isDefined => i.meta.get.to }.sum
          val files = vals.collect { case i if i.meta.isDefined => i.meta.get.file }.flatten.distinct
          reversedUrl -> CDXMeta(reversedUrl, Some(Meta(files, minFrom, maxTo)))
      }

      new PatriciaTrie(groupedMetas)
    }
  }

  case class HDFSIndexMeta(path: HPath) extends CCIndexMeta

  "s3://commoncrawl/cc-index/collections/CC-MAIN-2019-04/indexes/cluster.idx"

  def indexPaths(index: String) = s"s3://commoncrawl/crawl-data/$index/cc-index.paths.gz"

}

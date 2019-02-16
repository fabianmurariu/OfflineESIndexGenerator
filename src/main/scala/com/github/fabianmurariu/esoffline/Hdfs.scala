package com.github.fabianmurariu.esoffline

import java.net.URI
import java.nio.file.Path

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.{Path => HPath}
import io.circe.parser._
import io.circe.generic.auto._
import monix.eval.Task
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.DirectoryFileFilter
import org.apache.commons.io.filefilter.DirectoryFileFilter.DIRECTORY

import scala.io.Source

object Hdfs {

  class HdfsRepo(conf: Configuration) extends Serializable {
    @transient lazy val fs: FileSystem = FileSystem.get(conf)

    def copyToHDFS(o: OfflineIndexPartition): Task[Unit] = o match {
      case OfflineIndexPartition(partitionId, dest, localPath, _) =>
        import scala.collection.JavaConversions._
        val localRepo = localPath.resolve("repo")
        fs.setWriteChecksum(false)
        //read index0
        // find the segment with data for every index
        val fileContent = Source.fromFile(localRepo.resolve("index-0").toFile).mkString
        Task
          .fromEither(decode[Index0](fileContent))
          .foreachL {
            case Index0(_, indices) =>
              indices.foreach {
                case (name, IndexMeta(id, _)) =>
                  val dataSegmentFile = FileUtils
                    .listFilesAndDirs(localRepo.resolve("indices").resolve(id).toFile, DIRECTORY, DIRECTORY)
                    .tail.maxBy(FileUtils.sizeOfDirectory)
                  fs.copyFromLocalFile(true, true,
                    new HPath(dataSegmentFile.getAbsolutePath),
                    new HPath(dest.toString, s"indices/$name/$partitionId"))

              }
              // for partition 0 copy metadata
              if (partitionId == 0) {
                val files = FileUtils
                  .listFiles(localRepo.toFile, null, false)
                  .toVector
                files.foreach { f => fs.copyFromLocalFile(new HPath(f.toURI), new HPath(dest)) }
              }
          }
      // we always copy the 0 partition id in full
    }

  }

  case class OfflineIndexPartition(partitionId: Int, dest: URI, localPath: Path, indices: Seq[String])

}

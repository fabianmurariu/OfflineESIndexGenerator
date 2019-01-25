package sk.eset.dbsystems

import com.sksamuel.elastic4s.embedded.LocalNode
import monix.eval.Task
import org.apache.spark.rdd.RDD
import org.elasticsearch.common.settings.Settings

/**
  * Created by andrej.babolcai on 22. 1. 2016.
  *
  */

package object spark {

  val t = Task{ LocalNode(Settings.builder().build())}

  implicit class DBSysSparkRDDFunctions[T <: Map[String,Object]](val rdd: RDD[T]) extends AnyVal {
    def saveToESSnapshot(workdir:String,
                         indexName: String,
                         documentType: String,
                         mapping: String,
                         indexSettings: String,
                         snapshotrepoName: String,
                         snapshotName: String,
                         destDFSDir: String,
                         hadoopConfResources: Seq[String],
                         _IDField:Option[String]
                        ):Unit = {

      Product1
      //Configure job
      val numPartitions = rdd.partitions.length //For some reason we have to use this variable instead of directly passing ...length (NullPointerException)
      lazy val esWriter = new ESWriter(
        numPartitions,
        workdir,
        indexName,
        documentType,
        mapping,
        indexSettings,
        snapshotrepoName,
        snapshotName,
        destDFSDir,
        hadoopConfResources,
        _IDField
      )
      //Run job
      rdd.sparkContext.runJob(rdd, esWriter.processPartition _)
    }
  }
}


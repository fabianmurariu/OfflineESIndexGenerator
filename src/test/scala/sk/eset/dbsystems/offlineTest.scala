package sk.eset.dbsystems

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class offlineTest extends FlatSpec with Matchers {

  "offline-indexing" should "trigger the offline indexing of a dataset" in {
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    import spark.implicits._
    import offline._

    Seq("a" -> 1, "b" -> 2).toDS()
      .indexPartition{
        (client, row) =>
          1
      }.count()
  }

}

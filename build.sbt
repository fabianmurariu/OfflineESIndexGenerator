import sbt.Keys._

lazy val root = (project in file(".")).
  settings(
    name := "OfflineESIndex",
    version := "3.0.0",
    scalaVersion := "2.11.11",
    mainClass in Compile := Some("sk.eset.dbsystems.OfflineESIndexGenerator"),
    exportJars := true,
    retrieveManaged := true
  )

val circeVersion = "0.10.0"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

val elastic4sVersion = "5.6.9"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided",
  "com.github.scopt" %% "scopt" % "3.5.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.8.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.8.2",
  "com.github.scopt" %% "scopt" % "3.5.0",
  "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-tcp" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-embedded" % elastic4sVersion,
  "io.monix" %% "monix" % "3.0.0-RC2",
  "io.monix" %% "monix-execution" % "3.0.0-RC2"
)

// There is a conflict between Guava/Jackson versions on Elasticsearch and Hadoop
// Shading Guava Package
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename(
    "com.github.scopt.**" -> "shadescopt.@1",
    "org.joda.**" -> "shadeorgjoda.@1",
    "com.jsuereth.**" -> "shadejsuereth.@1",
    "com.fasterxml.**" -> "shadefasterxml.@1",
    "com.google.**" -> "shadegoogle.@1").inAll
)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

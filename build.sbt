import sbt.Keys._

lazy val root = (project in file(".")).
  settings(
    name := "OfflineESIndex",
    version := "3.0.0",
    scalaVersion := "2.11.11",
    mainClass in Compile := Some("sk.eset.dbsystems.OfflineESIndexGenerator"),
    Test / fork := true,
    Test / javaOptions += "-Xmx8G",
    resolvers += Resolver.mavenLocal,
    test in assembly := {}
  )

val circeVersion = "0.10.0"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

val elastic4sVersion = "6.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "com.github.scopt" %% "scopt" % "3.5.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.11.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.11.1",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.11.1",
  "com.github.scopt" %% "scopt" % "3.5.0",
  "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-embedded" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-circe" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-http" % elastic4sVersion,
  "io.monix" %% "monix" % "3.0.0-RC2",
  "io.monix" %% "monix-execution" % "3.0.0-RC2",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "com.google.guava" % "guava" % "14.0.1",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "com.optimaize.languagedetector" % "language-detector" % "0.7.1",
  "org.apache.commons" % "commons-collections4" % "4.3",
  "org.netpreserve.commons" % "webarchive-commons" % "1.1.8" exclude("org.apache.hadoop", "hadoop-core")
)

dependencyOverrides ++= Seq(
  "com.google.guava" % "guava" % "14.0.1"
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

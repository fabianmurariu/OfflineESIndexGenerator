import sbt.Keys._

lazy val projV = "0.1-SNAPSHOT"
lazy val scalaV = "2.11.11"
lazy val circeVersion = "0.10.0"
lazy val elastic4sVersion = "6.5.1"
lazy val sparkVersion = "2.4.1"

lazy val commonSettings = Seq(
  version := projV,
  scalaVersion := scalaV,
  resolvers += Resolver.mavenLocal,
  dependencyOverrides ++= Seq(
    "com.google.guava" % "guava" % "14.0.1"
  )
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "offline-index"
  ).aggregate(offlineIndexCore, offlineIndexCC)

lazy val offlineIndexCore = (project in file("offline-index-es-core")).
  settings(commonSettings: _*).
  settings(
    name := "offline-index-es-core",
    libraryDependencies ++= (elastic4sDeps ++ circeDeps ++ sparkDeps ++ otherDeps)
  )

lazy val offlineIndexCC = (project in file("offline-index-cc")).
  settings(commonSettings: _*).
  settings(
    name := "offline-index-cc",
    test in assembly := {},
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename(
        "com.github.scopt.**" -> "shadescopt.@1",
        "org.joda.**" -> "shadeorgjoda.@1",
        "com.jsuereth.**" -> "shadejsuereth.@1",
        "com.fasterxml.**" -> "shadefasterxml.@1",
        "com.google.**" -> "shadegoogle.@1").inAll
    ),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyJarName in assembly := s"${name.value}-${version.value}.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    Test / fork := true,
    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % "3.5.0",
      "com.optimaize.languagedetector" % "language-detector" % "0.7.1",
      "org.apache.commons" % "commons-collections4" % "4.3",
      "org.netpreserve.commons" % "webarchive-commons" % "1.1.8" exclude("org.apache.hadoop", "hadoop-core"),
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    ) ++ elastic4sDeps ++ sparkDeps
  ).dependsOn(offlineIndexCore)


lazy val circeDeps = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

lazy val sparkDeps = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

lazy val elastic4sDeps = Seq(
  "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion % "provided",
  "com.sksamuel.elastic4s" %% "elastic4s-embedded" % elastic4sVersion % "provided",
  "com.sksamuel.elastic4s" %% "elastic4s-circe" % elastic4sVersion % "provided",
  "com.sksamuel.elastic4s" %% "elastic4s-http" % elastic4sVersion % "provided"
)

lazy val otherDeps = Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.11.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.11.1",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.11.1",
  "io.monix" %% "monix" % "3.0.0-RC2",
  "io.monix" %% "monix-execution" % "3.0.0-RC2",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "com.google.guava" % "guava" % "14.0.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)

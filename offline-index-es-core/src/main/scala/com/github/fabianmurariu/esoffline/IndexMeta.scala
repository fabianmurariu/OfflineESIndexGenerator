package com.github.fabianmurariu.esoffline

import java.nio.file.Path

import monix.eval.Task
import io.circe.generic.auto._
import io.circe.parser._
import scala.io.Source

case class Snapshot(name: String, uuid: String)

case class IndexMeta(id: String, snapshots: Seq[String])

case class Index0(snapshots: Seq[Snapshot] = Seq.empty, indices: Map[String, IndexMeta] = Map.empty)

object Index0 {
  def fromFile(path: Path): Task[Index0] = {
    Task {
      Source.fromFile(path.toFile, "UTF-8")
    }.bracket { s =>
      Task.fromEither(parse(s.mkString))
        .flatMap(json => Task.fromEither(json.as[Index0]))
    }(s => Task(s.close))
  }
}


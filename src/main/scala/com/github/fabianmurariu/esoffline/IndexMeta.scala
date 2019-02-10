package com.github.fabianmurariu.esoffline

case class Snapshot(name: String, uuid: String)

case class IndexMeta(id: String, snapshots: Seq[Snapshot])

case class Index0(snapshots: Seq[Snapshot] = Seq.empty, indices: Map[String, IndexMeta] = Map.empty)


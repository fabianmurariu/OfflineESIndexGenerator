package com.github.fabianmurariu.esoffline

import java.net.URI
import java.nio.file.Path

case class OfflineIndexPartition(partitionId: Int, dest: URI, localPath: Path, indices: Seq[String])

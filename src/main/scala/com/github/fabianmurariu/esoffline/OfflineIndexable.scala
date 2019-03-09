package com.github.fabianmurariu.esoffline

trait OfflineIndexable[T] {

  def routing(t: T): String

}

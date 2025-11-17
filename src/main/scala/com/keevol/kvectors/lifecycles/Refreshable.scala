package com.keevol.kvectors.lifecycles

trait Refreshable {
  // init at startup or refresh after sometime.
  def refresh(): Unit
}


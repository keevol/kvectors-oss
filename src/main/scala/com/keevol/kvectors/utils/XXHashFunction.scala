package com.keevol.kvectors.utils

import java.lang

class XXHashFunction extends HashFunction {
  override def hash(value: String): lang.Long = Hashing.hash(value)
}


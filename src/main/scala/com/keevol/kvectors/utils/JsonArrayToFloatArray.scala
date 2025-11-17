package com.keevol.kvectors.utils

import io.vertx.core.json.JsonArray
import collection.JavaConverters._


object JsonArrayToFloatArray {
  def apply(jsonArray: JsonArray): Array[Float] = jsonArray.getList.asScala.map { // 3. 遍历每个元素并进行安全的转换
    case n: java.lang.Number => n.floatValue() // 4. 如果是数字，安全地转为 Float
    case other => throw new ClassCastException(s"无法将'${other}'转换为Float")
  }.toArray // 5. 将最终的集合转换为 Array[Float]
}
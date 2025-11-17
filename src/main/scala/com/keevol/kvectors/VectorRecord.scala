package com.keevol.kvectors

/**
 * <pre>
 * :::    ::: :::::::::: :::::::::: :::     :::  ::::::::  :::
 * :+:   :+:  :+:        :+:        :+:     :+: :+:    :+: :+:
 * +:+  +:+   +:+        +:+        +:+     +:+ +:+    +:+ +:+
 * +#++:++    +#++:++#   +#++:++#   +#+     +:+ +#+    +:+ +#+
 * +#+  +#+   +#+        +#+         +#+   +#+  +#+    +#+ +#+
 * #+#   #+#  #+#        #+#          #+#+#+#   #+#    #+# #+#
 * ###    ### ########## ##########     ###      ########  ##########
 * </pre>
 * <p>
 * KEEp eVOLution!
 * <p>
 *
 * @author fq@keevol.cn
 * @since 2017.5.12
 *        <p>
 *        Copyright 2017 © 杭州福强科技有限公司版权所有 (<a href="https://www.keevol.cn">keevol.cn</a>)
 */

import io.vertx.core.json.JsonObject

case class VectorMetadata(relationId: String, extra: Option[JsonObject] = None) {
  def asJson(): String = new JsonObject().put("rid", relationId).put("extra", if (extra.isEmpty) null else extra.get).encode()
}

object VectorMetadata {
  def fromJsonString(jsonStr: String): VectorMetadata = {
    val json = new JsonObject(jsonStr)
    val rid = json.getString("rid")
    val extra = json.getJsonObject("extra")
    VectorMetadata(rid, Option(extra))
  }
}

/**
 * id is implicit internal to kvectors, not aimed to be used by users.
 *
 * as to vector and its original record(text, img, etc.) relation, users should put another identity information to the metadata.
 *
 * Furthermore, other filter conditions or enrichment information can be added into metadata to complement the raw vector.
 *
 *
 * @param vector   the raw full precise vector data in float32
 * @param metadata other information related to current vector.
 */
case class VectorRecord(val vector: Array[Float], metadata: VectorMetadata)

/**
 *
 * @param id       is globally generated implicitly, so most of the time, users only read it if necessary(most of the time, it's not)
 * @param meta in json string
 * @param score similarity score
 */
case class VectorResult(id: Long, meta: JsonObject, score: Option[Float] = None)





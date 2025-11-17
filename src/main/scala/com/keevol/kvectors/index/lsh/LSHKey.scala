package com.keevol.kvectors.index.lsh

/**
 * <pre>
 * ██╗  ██╗ ███████╗ ███████╗ ██╗   ██╗  ██████╗  ██╗
 * ██║ ██╔╝ ██╔════╝ ██╔════╝ ██║   ██║ ██╔═══██╗ ██║
 * █████╔╝  █████╗   █████╗   ██║   ██║ ██║   ██║ ██║
 * ██╔═██╗  ██╔══╝   ██╔══╝   ╚██╗ ██╔╝ ██║   ██║ ██║
 * ██║  ██╗ ███████╗ ███████╗  ╚████╔╝  ╚██████╔╝ ███████╗
 * ╚═╝  ╚═╝ ╚══════╝ ╚══════╝   ╚═══╝    ╚═════╝  ╚══════╝
 * </pre>
 * <p>
 * KEEp eVOLution!
 * <p>
 *
 * @author fq@keevol.cn
 * @since 2017.5.12
 * <p>
 * Copyright 2017 © 杭州福强科技有限公司版权所有 (<a href="https://www.keevol.cn">keevol.cn</a>)
 */

import java.math.BigInteger

/**
 * a marker trait for LSH hashcode as key in hash table.
 */
trait LSHKey

case class ByteLSHKey(value: Byte) extends LSHKey

case class ShortLSHKey(value: Short) extends LSHKey

case class IntLSHKey(value: Int) extends LSHKey

case class LongLSHKey(value: Long) extends LSHKey

/**
 * for hashcode longer than 64 bits, we use BigInteger to hold it which use byte array underneath.
 *
 * @param value hashcode value
 */
case class BigIntLSHKey(value: BigInteger) extends LSHKey



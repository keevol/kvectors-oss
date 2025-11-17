package com.keevol.kvectors.utils
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
import org.apache.commons.lang3.time.DateFormatUtils

import java.util.Date

object Dates {
  val DEFAULT_FORMAT: String = "yyyy-MM-dd HH:mm:ss.SSS"

  def toDisplayString(ts: Long): String = DateFormatUtils.format(ts, DEFAULT_FORMAT)

  def defaultFormat(dt: Date): String = DateFormatUtils.format(dt, DEFAULT_FORMAT)
}
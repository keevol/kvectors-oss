package com.keevol.kvectors.lifecycles

import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicBoolean

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
 * <p>
 * Copyright 2017 © 杭州福强科技有限公司版权所有 (<a href="https://www.keevol.cn">keevol.cn</a>)
 */
trait Lifecycle extends AutoCloseable {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  val running: AtomicBoolean = new AtomicBoolean()

  def start(): Unit = {
    if (running.compareAndSet(false, true)) {
      doStart()
    } else {
      logger.warn("you can't start again.")
    }
  }

  protected def doStart(): Unit

  override def close(): Unit = {
    if (running.compareAndSet(true, false)) {
      doStop()
    } else {
      logger.warn("it is closed already or NOT start yet?")
    }
  }

  protected def doStop(): Unit

  /**
   * an alias for close method which is not a good name in lifecycle scenario(but good for autoclose)
   */
  def stop(): Unit = close()
}
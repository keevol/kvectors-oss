package com.keevol.kvectors.collections.config
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
import com.keevol.kvectors.utils.TypeAlias.Dir
import org.apache.commons.lang3.Strings

import java.io.File

trait ConfigFeature {
  def configFileSuffix: String

  def getConfigFile(collectionDir: Dir): File = getConfigFileAsPer(collectionDir, collectionDir.getName)

  def getConfigFileAsPer(collectionDir: Dir, collectionName: String): File = new File(collectionDir, s"""${collectionName}${if (Strings.CS.startsWith(configFileSuffix, ".")) configFileSuffix else s".${configFileSuffix}"}""")
}
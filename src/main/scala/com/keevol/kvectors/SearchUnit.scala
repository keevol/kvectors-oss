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
 * <p>
 * Copyright 2017 © 杭州福强科技有限公司版权所有 (<a href="https://www.keevol.cn">keevol.cn</a>)
 */

import io.github.jbellis.jvector.disk.ReaderSupplier
import io.github.jbellis.jvector.graph.{GraphSearcher, RandomAccessVectorValues}
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex
import io.github.jbellis.jvector.quantization.{PQVectors, ProductQuantization}

/**
 * This is mainly for atomic swap, since we can only swap one instance of this instead of swapping multiple values in a transaction.
 *
 * @param pq vector compressor
 * @param pqv compressed vectors
 * @param index graph index
 * @param searcher graph index searcher
 * @param ravv vector data source to use.
 */
case class SearchUnit(pqv: PQVectors, index: OnDiskGraphIndex, searcher: GraphSearcher, ravv: RandomAccessVectorValues, pqvReaderSupplier: ReaderSupplier, indexReaderSupplier: ReaderSupplier)


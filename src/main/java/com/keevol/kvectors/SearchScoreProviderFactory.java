package com.keevol.kvectors;

import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;

/**
 * <pre>
 * :::    ::: :::::::::: :::::::::: :::     :::  ::::::::  :::
 * :+:   :+:  :+:        :+:        :+:     :+: :+:    :+: :+:
 * +:+  +:+   +:+        +:+        +:+     +:+ +:+    +:+ +:+
 * +#++:++    +#++:++#   +#++:++#   +#+     +:+ +#+    +:+ +#+
 * +#+  +#+   +#+        +#+         +#+   +#+  +#+    +#+ +#+
 * #+#   #+#  #+#        #+#          #+#+#+#   #+#    #+# #+#
 *  * </pre>
 * <p>
 * KEEp eVOLution!
 * <p>
 *
 * @author fq@keevol.cn
 * @since 2017.5.12
 * <p>
 * Copyright 2017 © 杭州福强科技有限公司版权所有 (<a href="https://www.keevol.cn">keevol.cn</a>)
 */
public class SearchScoreProviderFactory {

    /**
     * This is a jvector-specific utility
     *
     * @param index graph index
     * @param pqv pq compressed vectors
     * @param query query vector
     * @param vsf similarity function type
     * @return an instance of SearchScoreProvider
     */
    public SearchScoreProvider create(OnDiskGraphIndex index,
                                      PQVectors pqv,
                                      VectorFloat<?> query,
                                      VectorSimilarityFunction vsf) {
        if (pqv != null) {
            // 两阶段搜索：PQ + 重排
            ScoreFunction.ApproximateScoreFunction asf = pqv.precomputedScoreFunctionFor(query, vsf);
            ScoreFunction.ExactScoreFunction reranker = index.getView().rerankerFor(query, vsf);
            return new DefaultSearchScoreProvider(asf, reranker);
        } else {
            // 纯精确搜索
            ScoreFunction.ExactScoreFunction esf = index.getView().rerankerFor(query, vsf);
            return new DefaultSearchScoreProvider(esf);
        }
    }
}

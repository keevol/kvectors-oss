package com.keevol.kvectors.utils;

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
public class Vectors {
    // Mean Pooling: 取所有 token embedding 的平均值（基于 attention_mask）
    public static float[] meanPooling(float[][] tokenEmbeddings, int[] attentionMask) {
        int numTokens = tokenEmbeddings.length;
        int embeddingDim = tokenEmbeddings[0].length;
        float[] sentenceEmbedding = new float[embeddingDim];
        int activeTokens = 0;

        for (int i = 0; i < numTokens; i++) {
            if (attentionMask[i] == 1) {
                for (int j = 0; j < embeddingDim; j++) {
                    sentenceEmbedding[j] += tokenEmbeddings[i][j];
                }
                activeTokens++;
            }
        }

        if (activeTokens > 0) {
            for (int j = 0; j < embeddingDim; j++) {
                sentenceEmbedding[j] /= activeTokens;
            }
        }
        return sentenceEmbedding;
    }

    // L2 归一化
    public static void normalize(float[] vector) {
        float norm = 0.0f;
        for (float v : vector) {
            norm += v * v;
        }
        norm = (float) Math.sqrt(norm);

        if (norm > 0) {
            for (int i = 0; i < vector.length; i++) {
                vector[i] /= norm;
            }
        }
    }
}

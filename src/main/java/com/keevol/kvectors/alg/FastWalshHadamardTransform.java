package com.keevol.kvectors.alg;

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

import java.util.Arrays;

/**
 * 快速沃尔什-阿达玛变换 (Fast Walsh-Hadamard Transform) 实现。
 * FWHT 是一种高效的正交变换，时间复杂度为 O(n log n)。
 */
public class FastWalshHadamardTransform {

    /**
     * 对输入向量执行快速沃尔什-阿达玛变换 (in-place)。
     * 输入向量的长度必须是 2 的幂。
     *
     * @param vec 输入/输出向量
     * @throws IllegalArgumentException 如果向量长度不是 2 的幂
     */
    public static void apply(float[] vec) {
        if (vec == null) {
            throw new IllegalArgumentException("Vector cannot be null.");
        }

        final int n = vec.length;
        if ((n == 0 || n == 1) || (n & (n - 1)) != 0) {
            throw new IllegalArgumentException("Vector length must be a power of 2.");
        }

        for (int h = 1; h < n; h <<= 1) {
            for (int i = 0; i < n; i += h << 1) {
                for (int j = i; j < i + h; j++) {
                    float x = vec[j];
                    float y = vec[j + h];
                    vec[j] = x + y;
                    vec[j + h] = x - y;
                }
            }
        }
    }

    /**
     * 对变换后的向量进行归一化。
     * 归一化因子为 1/sqrt(n)，使变换成为正交变换。
     *
     * @param vec 需要归一化的向量
     */
    public static void normalize(float[] vec) {
        if (vec == null || vec.length == 0) {
            return;
        }

        float norm = (float) Math.sqrt(vec.length);
        for (int i = 0; i < vec.length; i++) {
            vec[i] /= norm;
        }
    }

    public static void main(String[] args) {
        float[] vector = {100.0f, 0.1f, -200.0f, 0.2f, 5.0f, -0.1f, -8.0f, 0.3f};
        float[] original = Arrays.copyOf(vector, vector.length);

        System.out.println("Original vector:           " + Arrays.toString(vector));

        // 执行变换
        FastWalshHadamardTransform.apply(vector);
        System.out.println("Transformed vector (FWHT): " + Arrays.toString(vector));

        System.out.println("Inverse transformed:       " + Arrays.toString(vector));
        System.out.println("Matches original:          " + Arrays.equals(original, vector));
    }
}

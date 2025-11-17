package com.keevol.kvectors.utils;

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

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

import java.util.Random;

/**
 * 一个封装了随机投影逻辑的类
 * <p>
 * 随机投影的数学保证（JL 引理）是建立在一个前提之上的：所有的向量都必须被投影到同一个随机子空间中。
 */
public class RandomProjection {

    private final int originalDim;
    private final int projectedDim;
    private final RealMatrix randomMatrix; // 随机投影矩阵 R

    /**
     * @param originalDim  原始向量维度 (D)
     * @param projectedDim 目标投影维度 (k)
     */
    public RandomProjection(int originalDim, int projectedDim) {
        if (projectedDim >= originalDim) {
            throw new IllegalArgumentException("Projected dimension must be smaller than original dimension.");
        }
        this.originalDim = originalDim;
        this.projectedDim = projectedDim;
        this.randomMatrix = createRandomMatrix();
    }

    /**
     * 创建一个 D x k 的随机矩阵
     * 矩阵中的每个元素都从标准正态分布（高斯分布）中采样
     */
    private RealMatrix createRandomMatrix() {
        double[][] matrixData = new double[this.originalDim][this.projectedDim];
        Random random = new Random();

        // JL 引理的一个常见实现技巧是，对随机矩阵进行归一化
        // 但为了简单起见，这里直接使用高斯随机数，效果已经很好
        for (int i = 0; i < this.originalDim; i++) {
            for (int j = 0; j < this.projectedDim; j++) {
                matrixData[i][j] = random.nextGaussian();
            }
        }
        return MatrixUtils.createRealMatrix(matrixData);
    }

    /**
     * 将一个高维向量投影到低维空间
     *
     * @param highDimVector 原始高维向量
     * @return 降维后的低维向量
     */
    public double[] project(double[] highDimVector) {
        if (highDimVector.length != this.originalDim) {
            throw new IllegalArgumentException("Input vector dimension mismatch.");
        }

        // 将输入向量转换为一个 1 x D 的行矩阵
        RealMatrix vectorMatrix = MatrixUtils.createRowRealMatrix(highDimVector);

        // 核心操作：v' = v * R
        // [1 x k] = [1 x D] * [D x k]
        RealMatrix projectedMatrix = vectorMatrix.multiply(this.randomMatrix);

        // 将结果从矩阵转换回数组
        return projectedMatrix.getRow(0);
    }

    // --- 辅助方法和主程序 ---

    /**
     * 计算两个向量之间的欧氏距离
     * <p>
     * this is mainly for main() method to demonstrate the random projection results.
     * <p>
     * A more efficient way to implement computation of Euclidean distance is to use Java Vector API which enables SIMD of CPU.
     */
    public static double euclideanDistance(double[] v1, double[] v2) {
        double sum = 0.0;
        for (int i = 0; i < v1.length; i++) {
            double diff = v1[i] - v2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    public static void main(String[] args) {
        int originalDim = 1024;
        int projectedDim = 128;

        // 1. 创建一个随机投影实例
        RandomProjection rp = new RandomProjection(originalDim, projectedDim);

        // 2. 创建三个高维向量：
        //    - v1 和 v2 彼此相似
        //    - v3 与 v1, v2 都不相似
        Random random = new Random();
        double[] v1 = new double[originalDim];
        double[] v2 = new double[originalDim];
        double[] v3 = new double[originalDim];
        for (int i = 0; i < originalDim; i++) {
            v1[i] = random.nextDouble();
            v2[i] = v1[i] + random.nextGaussian() * 0.01; // v2 是 v1 加上一点点噪音
            v3[i] = random.nextDouble() * 5.0;            // v3 是完全不同的
        }

        // 3. 将它们投影到低维空间
        double[] p_v1 = rp.project(v1);
        double[] p_v2 = rp.project(v2);
        double[] p_v3 = rp.project(v3);

        System.out.println("Original dimension: " + originalDim);
        System.out.println("Projected dimension: " + projectedDim);
        System.out.println("--------------------------------------------------");

        // 4. 比较投影前后的距离关系
        // 我们的期望是：投影后，相似向量的距离仍然很小，不相似向量的距离仍然很大

        System.out.println("--- Before Projection (High-dimensional space) ---");
        System.out.printf("Distance(v1, v2) (similar vectors): %.4f%n", euclideanDistance(v1, v2));
        System.out.printf("Distance(v1, v3) (dissimilar vectors): %.4f%n", euclideanDistance(v1, v3));

        System.out.println("\n--- After Projection (Low-dimensional space) ---");
        System.out.printf("Distance(p_v1, p_v2) (similar vectors): %.4f%n", euclideanDistance(p_v1, p_v2));
        System.out.printf("Distance(p_v1, p_v3) (dissimilar vectors): %.4f%n", euclideanDistance(p_v1, p_v3));

        System.out.println("\n--- Why is this useful? ---");
        System.out.println("Notice that the *ratio* of distances is approximately preserved.");
        double originalRatio = euclideanDistance(v1, v3) / euclideanDistance(v1, v2);
        double projectedRatio = euclideanDistance(p_v1, p_v3) / euclideanDistance(p_v1, p_v2);
        System.out.printf("Original distance ratio (far/near): %.2f%n", originalRatio);
        System.out.printf("Projected distance ratio (far/near): %.2f%n", projectedRatio);
        System.out.println("This shows that the 'nearness' relationship is maintained after projection!");
    }
}
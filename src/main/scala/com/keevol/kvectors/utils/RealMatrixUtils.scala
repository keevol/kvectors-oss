package com.keevol.kvectors.utils

import org.apache.commons.math3.linear.{Array2DRowRealMatrix, RealMatrix}

import java.io.RandomAccessFile

/**
 * for matrix serialization/deserialization
 */
object RealMatrixUtils {
  /**
   * 将一个 RealMatrix 以自定义格式写入 RandomAccessFile。
   * 格式: [Int: rows][Int: cols][Double * rows * cols]
   *
   * @param matrix 要序列化的矩阵
   * @param raf    目标文件
   */
  def writeMatrix(matrix: RealMatrix, raf: RandomAccessFile): Unit = {
    val rows = matrix.getRowDimension
    val cols = matrix.getColumnDimension

    raf.writeInt(rows)
    raf.writeInt(cols)

    // 按行优先顺序写入所有 double 值 ｜ 在 Scala 中，当处理底层数据结构（如 Array）并执行性能极其敏感的计算时，使用 while 循环几乎总是最快的选择。
    var i = 0
    while (i < rows) {
      var j = 0
      while (j < cols) {
        raf.writeDouble(matrix.getEntry(i, j))
        j += 1
      }
      i += 1
    }
  }

  /**
   * 从 RandomAccessFile 中以自定义格式读取 RealMatrix。
   *
   * @param raf 数据源文件
   * @return 反序列化后的 RealMatrix
   */
  def readMatrix(raf: RandomAccessFile): RealMatrix = {
    val rows = raf.readInt()
    val cols = raf.readInt()

    require(rows > 0 && cols > 0, "rows and cols should bigger than zero.")

    val data = Array.ofDim[Double](rows, cols)

    // 在 Scala 中，当处理底层数据结构（如 Array）并执行性能极其敏感的计算时，使用 while 循环几乎总是最快的选择。
    var i = 0
    while (i < rows) {
      var j = 0
      while (j < cols) {
        data(i)(j) = raf.readDouble()
        j += 1
      }
      i += 1
    }

    new Array2DRowRealMatrix(data, false) // false 表示 data 数组不再被外部修改，可以安全地被内部引用，效率更高
  }
}
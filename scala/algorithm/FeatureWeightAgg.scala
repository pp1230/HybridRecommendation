package scala.algorithm

import org.apache.spark.ml.linalg.{DenseVector, Vectors, SQLDataTypes}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by pi on 17-12-1.
  */
object FeatureWeightAgg extends UserDefinedAggregateFunction{
  // Data types of input arguments of this aggregate function
  def inputSchema: StructType = StructType(StructField("inputColumn1", SQLDataTypes.VectorType) ::
    StructField("inputColumn2", DoubleType) :: Nil)
  // Data types of values in the aggregation buffer
  def bufferSchema: StructType = {
    StructType(StructField("sum", SQLDataTypes.VectorType) :: StructField("count", DoubleType) :: Nil)
  }
  // The data type of the returned value
  def dataType: DataType = SQLDataTypes.VectorType
  // Whether this function always returns the same output on the identical input
  def deterministic: Boolean = true
  // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
  // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
  // the opportunity to update its values. Note that arrays and maps inside the buffer are still
  // immutable.
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Vectors.dense(Array(
      //      0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
      //      0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
      //      0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
      //      0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
      //      0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
      //      0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
      0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
      0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
      0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
    )).toDense
    buffer(1) = 0.0
  }
  // Updates the given aggregation buffer `buffer` with new input data from `input`
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      //      buffer(0) = buffer.getAs[DenseVector](0) + input.getAs[DenseVector](0)
      val v1 = buffer.getAs[DenseVector](0)
      val v2 = input.getAs[DenseVector](0)
      val w = input.getAs[Double](1)
      val v3 = new Array[Double](v1.size)
      for (i <- 0 to v3.length - 1) {
        val sum = v1.apply(i) + w*v2.apply(i)
        v3.update(i, sum)
        //println(i+"update:"+v1.apply(i)+"/"+v2.apply(i)+"/"+w+"/"+sum)
      }
      buffer(0) = Vectors.dense(v3).toDense
      buffer(1) = buffer.getDouble(1) + w
    }
  }
  // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    val v1 = buffer1.getAs[DenseVector](0)
    val v2 = buffer2.getAs[DenseVector](0)
    val v3 = new Array[Double](v1.size)
    for (i <- 0 to v3.length - 1) {
      val sum = v1.apply(i) + v2.apply(i)
      v3.update(i, sum)
      //println(i+"merge:"+sum)
    }
    buffer1(0) = Vectors.dense(v3).toDense
    buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
  }
  // Calculates the final result
  def evaluate(buffer: Row): DenseVector = {
    //buffer.getLong(0).toDouble / buffer.getLong(1)
    val v = buffer.getAs[DenseVector](0)
    val v3 = new Array[Double](v.size)
    val num = buffer.getDouble(1)
    for (i <- 0 to v3.length - 1) {
      v3.update(i, v.apply(i)/num)
    }
    Vectors.dense(v3).toDense
  }
}

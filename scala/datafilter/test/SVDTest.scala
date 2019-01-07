package scala.datafilter.test

import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.Vector
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix

import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors

import breeze.linalg._

/**
  * Created by pi on 17-12-29.
  */
object SVDTest {
//  val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
//  val sc = new SparkContext(conf)
  val ss = SparkSession.builder().appName("Yelp Rating")
    .master("local[*]").getOrCreate()
  def main(args: Array[String]) {
//    val data = Array(
//      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
//      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
//      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))
//
//    val rows = sc.parallelize(data)
//
//    val mat: RowMatrix = new RowMatrix(rows)
//
//    // Compute the top 5 singular values and corresponding singular vectors.
//    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(3, computeU = true)
//    val U: RowMatrix = svd.U  // The U factor is a RowMatrix.
//    val s: Vector = svd.s     // The singular values are stored in a local dense vector.
//    val V: Matrix = svd.V     // The V factor is a local dense matrix.
//
//    println(U.numRows()+"/"+U.numCols())
//    println(s)
//    println(V)
//    val size = s.size
//    val arr = new Array[Double](size*size)
//    for(i <- 0 to size*size-1){
//      if(i % (size+1) == 0){
//        arr.update(i, s.apply(i/size))
//      }
//      else arr.update(i, 0)
//    }
//
//    val sm = new DenseMatrix(size,size,arr)
//    val result = U.multiply(sm).multiply(V.transpose).rows
//
//    result.foreach(println(_))


    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = ss.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    df.show(false)
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)

    val result = pca.transform(df).select("pcaFeatures")
    result.show(false)

  }
}

package scala.socialtext

import org.apache.spark.ml.linalg.DenseVector

import scala.algorithm.{UserLinearRegression, LinearRegressionAl, FeatureAgg}
import scala.datafilter.DataAnalysis
import org.apache.spark.sql.functions._

/**
  * Created by pi on 18-1-4.
  */
object SVDTest {
  val rootPath = "/home/pi/doc/dataset/"
  val dataPath = "svdoutput/YelpTextFeatureLDA30JoinEmRev20Num"
  //w=1
  //val trainingPath = "output/YelpTextTimeWeight1FeatureLDA30JoinEmRev20Num"
  val trainingPath = "svdoutput/Training0.8"
  val testPath = "svdoutput/Testing0.2"
  val reMatrixPath = "svdoutput/ReMatrix100"
  val userFeaturePath = "svdoutput/userfeaturesvd100"
  val itemFeaturePath = "svdoutput/itemfeaturesvd100"
  val originalModelPath = "/home/pi/doc/dataset/svdoutput/usermodel/"
  val svdModelPath = "/home/pi/doc/dataset/svdoutput/usermodelsvd100/"
  val originalPredicitonPath = "svdoutput/originalPrediction"
  val svdPredicitonPath = "svdoutput/svdPrediction100"
}

object LinearRegressionAndUserItemFeature2{
  val analysis = new DataAnalysis(SVDTest.rootPath)

  def main(args: Array[String]) {

    val output1 = analysis.getData(SVDTest.trainingPath,"parquet")
    output1.show(false)
    val session = analysis.getdata.ss
    val userrdd = output1.groupBy("user_id").agg(FeatureAgg(output1.col("topicDistribution"))).toDF("user_id","feature")
    userrdd.show()
    analysis.outputResult(userrdd,"parquet", 1,SVDTest.userFeaturePath)
    val itemrdd = output1.groupBy("business_id").agg(FeatureAgg(output1.col("topicDistribution"))).toDF("business_id","feature")
    itemrdd.show()
    analysis.outputResult(itemrdd,"parquet", 1,SVDTest.itemFeaturePath)

  }
}

object LinearModelSaver2{
  val analysis = new DataAnalysis(SVDTest.rootPath)
  
  def main(args: Array[String]) {
    val lr = new LinearRegressionAl()
    
    val frame1 = analysis.getData(SVDTest.trainingPath,"parquet")
    val model1 = lr.fit(frame1, "topicDistribution", "s")
    model1.save(SVDTest.originalModelPath)

  }
}

object SocialPrediction12{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SVDTest.rootPath)
    //    val output1 = analysis.getData("output/YelpTextFeature0.001/part-00000-9c56faff-aea0-4bed-838b-3b30f4845920.snappy.parquet","parquet")
    //    output1.show(false)
    val testData = analysis.getData(SVDTest.testPath,"parquet").select("user_id", "item_id", "topicDistribution", "s")
      .toDF("user_id", "business_id", "topicDistribution", "s")
    testData.show(false)

//    val clusterData = analysis.getData(SVDTest.socialPath,"parquet")
//      .select("user_id", "class")
//    clusterData.show(150)
    
//    val output1 = testData.join(clusterData, "user_id")
//    output1.show()
    
    val output1 = testData.withColumn("class", lit(1))
    val output2 = analysis.getData(SVDTest.userFeaturePath,"parquet")
      .toDF("user_id", "user_feature")
    output2.show(false)
    val output3 = analysis.getData(SVDTest.itemFeaturePath,"parquet")
      .toDF("business_id", "business_feature")
    output3.show(false)
    //注意修改预测模型
//    val prediction = UserLinearRegression.userItemClusterPrediction("LR",SVDTest.originalModelPath, output1, output2, output3)
//    val fprediction = prediction.map(row => Feature4(row.getString(0), row.getString(1),  row.getInt(2),
//      row.getAs[DenseVector](3), row.getAs[DenseVector](4), row.getAs[DenseVector](5), row.getInt(6), row.getDouble(7)))
//    val session = analysis.getdata.ss
//    val predictionResult = session.createDataFrame(fprediction)
//    val selected1 = predictionResult.select("user_id", "star", "prediction", "feature")
//    selected1.show(1000)
//    analysis.outputResult(selected1,"parquet",SVDTest.originalModelPath)

  }
}

object SocialPrediction22{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SVDTest.rootPath)
    val selected1 = analysis.getData(SVDTest.originalModelPath,"parquet")
    val selected2 = selected1
      //使用user所在类别的平均分预测
      .groupBy("user_id", "star", "feature").avg("prediction")
      .select("star", "avg(prediction)")
      .toDF("star", "prediction")
    //选择其中一个类别预测
    //.select("star", "prediction")
    val filterdata = UserLinearRegression.filterPrediction("star", "prediction", selected2)
    filterdata.show(1000)
    println("PredictNum:"+selected2.count())
    println("PredictUserNum:"+selected1.select("user_id").distinct().count())
    val result1 = new LinearRegressionAl().evaluate("rmse", "star", "prediction", filterdata)
    val result2 = new LinearRegressionAl().evaluate("mae", "star", "prediction", filterdata)
    println(result1)
    println(result2)
  }
}
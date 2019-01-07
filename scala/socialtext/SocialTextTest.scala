package scala.socialtext

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.{Row, Column, SparkSession}
import org.apache.spark.sql.types.{StructType, StringType, StructField, IntegerType}

import scala.algorithm._
import scala.datafilter.DataAnalysis

/**
  * Created by pi on 17-9-12.
  */
object SocialTextTest {
  //本地模型参数保存路径
  val rootPath = "/home/pi/doc/dataset/"
  val dataPath = "output/EnglishPaper/Yelp2016UserBusinessStarReview20"
  //w=1
  //val trainingPath = "output/YelpTextTimeWeight1FeatureLDA30JoinEmRev20Num"
  val reviewNum = 10
  val LDANum = 20
  //1=Cluster,2=Social,3=mix
  val modelType = "2"
  //modelType = "1":GBT,RFT,LR
  val clusterModel = "LR"
  //1=CNM,2=CoDA
  val socialAl = "2"
  val per = 0.8
  val trainingPath = "output/EnglishPaper/Yelp2016UserBusinessStarReview"+reviewNum+"Training0.8LDA"+LDANum+"Iter100"
  val testPath = "output/EnglishPaper/Yelp2016UserBusinessStarReview"+reviewNum+"Testing0.2s"
  val clusterPath = "output/EnglishPaper/Yelp2016UserBusinessStarReview"+reviewNum+"Cluster"
  val userFeaturePath = "output/EnglishPaper/Yelp2016UserBusinessStarReview"+reviewNum+"Training0.8LDA"+LDANum+"Iter100UserFeature"
  val itemFeaturePath = "output/EnglishPaper/Yelp2016UserBusinessStarReview"+reviewNum+"Training0.8LDA"+LDANum+"Iter100ItemFeature"
  val userModelPath = "/home/pi/doc/dataset/output/EnglishPaper/Yelp2016UserBusinessStarReview"+reviewNum+"Training0.8LDA"+LDANum+"Iter100UserModel/"+modelType+"/"+clusterModel
  val userModelMixSocial = "/home/pi/doc/dataset/output/EnglishPaper/Yelp2016UserBusinessStarReview"+reviewNum+"Training0.8LDA"+LDANum+"Iter100UserModel/"+2+"/LR"
  val userModelMixCluster = "/home/pi/doc/dataset/output/EnglishPaper/Yelp2016UserBusinessStarReview"+reviewNum+"Training0.8LDA"+LDANum+"Iter100UserModel/"+1+"/LR"
  //val userModelPath = "/home/pi/doc/dataset/output/GLRLDA90Cluster1UserModel/"
  //bigclam
  //val socialAlgorithmResult = "output/socialUR20Group100.txt"
  //Coda
  val CodaResult = "output/EnglishPaper/Review"+reviewNum+"mc100xc250ClusterSkipcmtyvv.in.txt"
  //CNM
  //val socialAlgorithmResult = "output/ChinesePaper/Yelp2016UserBusinessStarReview10ClusterKeep.txt"
  val socialAlgorithmResult = "output/EnglishPaper/Yelp2016UserBusinessStarReview5ClusterSkipFiltered.txt"

  val socialPath = "output/EnglishPaper/Yelp2016UserBusinessStarReview"+reviewNum+"SocialIDToInt"

  val predicitonResultPath = "output/EnglishPaper/Yelp2016UserBusinessStarReview10Prediction"
  val predicitonSocialPath = "output/EnglishPaper/Yelp2016UserBusinessStarReview10SocialTmp"

  //集群参数
//  val rootPath = "hdfs://172.31.34.183:9000/"
//  val dataPath = "outputdata/YelpTextFeatureLDA80JoinEm1"
//    val trainingPath = "outputdata/YelpTextFeatureLDA80JoinEm0.7"
//    val testPath = "outputdata/YelpTextFeatureLDA80JoinEm0.3"
//    val clusterPath = "outputdata/YelpTextFeatureLDA80JoinEmCluster0.7g500"
//    val userFeaturePath = "outputdata/userLDA80feature0.7"
//    val itemFeaturePath = "outputdata/itemLDA80feature0.7"
//    val userModelPath = "hdfs://172.31.34.183:9000/outputdata/userLDA80model0.7g500/"
//
//    val predicitonResultPath = "outputdata/predicitonLDA80result0.3g500"
}
object Integration{
  def main(args: Array[String]) {
    //SplitData.main(args)

    //LinearRegressionAndUserItemFeature.main(args)

    if(SocialTextTest.modelType.equals("1")) {
      //聚类模型（所有用户属于一类）
      Clustering.main(args)
      LinearRegressionModelClusterSaver.main(args)
      ClusterPrediction.main(args)
    }
    else if(SocialTextTest.modelType.equals("2")){
      //社交模型
      SocialResultTransform.main(args)
      LinearRegressionModelSocialSaver.main(args)
      SocialPrediction1.main(args)
      SocialPrediction2.main(args)
    }
    else {
      //社交聚类混合模型
      SocialResultTransform.main(args)
      LinearRegressionModelSocialSaver.main(args)
      SocialMixPrediction.main(args)
    }
  }
}
case class we(w: DenseVector, e: Double, userid: String)
case class UserFeature(feature: DenseVector, userid: String)
case class ItemFeature(feature: DenseVector, itemid: String)
case class Feature3(business_id: String, user_id: String,
                    star: Int, user_feature:DenseVector, business_feature:DenseVector, feature: DenseVector, prediction:Double)
case class Feature4(business_id: String, user_id: String,
                    star: Int, user_feature:DenseVector, business_feature:DenseVector, feature: DenseVector, group:Int, prediction:Double)
case class LRModel(userid:String, model:LinearRegressionModel)

/**
  * 步骤一：数据分割
  * 步骤三：聚类
  * 个性化模型、时间模型跳过此步骤
  */
object SplitData{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SocialTextTest.rootPath)
    //数据分割
    val total = analysis.getData(SocialTextTest.dataPath,"parquet")
  //若item较为稀疏，改为item聚类
//      .select("topicDistribution", "business_id", "user_id", "s")
//      .toDF("topicDistribution", "user_id", "business_id", "s")
    total.show(false)
    val Array(training,testing) = total.randomSplit(Array(8,2))
    println(total.count()+"/"+training.count()+"/"+testing.count())
    //analysis.outputResult(clusterData,"parquet", SocialTextTest.clusterPath)
    analysis.outputResult(training,"parquet", SocialTextTest.trainingPath)
    analysis.outputResult(testing,"parquet", SocialTextTest.testPath)

    //聚类
//    val training = analysis.getData(SocialTextTest.userFeaturePath, "parquet")
//    training.show(100)
////    val testing = analysis.getData(SocialTextTest.testPath, "parquet")
////    testing.show(100)
//    val gmm = new BKM()
//    val gmmModel = gmm.fit(training, "feature", 200)
//    val clusterData = gmm.transform(training, "feature", gmmModel)
//      .select("feature", "user_id", "prediction")
//      .toDF("feature", "user_id", "class").orderBy("user_id")
//    clusterData.show(1000)
//    analysis.outputResult(clusterData,"parquet", SocialTextTest.clusterPath)

//    println(total.select("user_id").distinct().count())
//    println(training.select("user_id").distinct().count())
//    println(testing.select("user_id").distinct().count())

//    val sparkSession = analysis.getdata.ss
//    sparkSession.udf.register("FeatureAgg", FeatureAgg)
//    val Array(training,testing) = total.randomSplit(Array(1,1000))
//    val feature = training.select("user_id", "topicDistribution")
//    feature.createOrReplaceTempView("feature")
//    feature.show(100)
//    feature.groupBy("user_id").agg(FeatureAgg(feature.col("topicDistribution"))).orderBy("user_id").show(false)

    //val result = sparkSession.sql("SELECT FeatureAgg(topicDistribution) as userFeature FROM feature")
    //result.show(false)
  }
}
/**
  * 步骤二：计算用户和物品的特征并保存
  */
object LinearRegressionAndUserItemFeature{
  val analysis = new DataAnalysis(SocialTextTest.rootPath)

  def main(args: Array[String]) {
//    val output1 = analysis.getData("output/YelpTextFeature0.001/part-00000-9c56faff-aea0-4bed-838b-3b30f4845920.snappy.parquet","parquet")
//    output1.show(false)
//    val sss = analysis.getdata.ss
    val output1 = analysis.getData(SocialTextTest.trainingPath,"parquet")
    output1.show(false)
//    val output8 = analysis.getData("output/feature/part-00000-2e518fac-3645-4dec-aa60-54a860419cf4-c000.snappy.parquet","parquet")
//    output8.show(1000)

//    println("--------TotalRegression----------")
//    val result1 = new LinearRegressionAl().run(output1,"topicDistribution","s")
//
//    val output2 = UserLinearRegression.filterByUserId("09R2T2MjJVa50dZx7TTojg", output1)
//    output2.show(false)
//
//    val output3 = UserLinearRegression.getUserId(output1)
//    println(output3.apply(1).getString(0))

    //计算用户的w和e
//    val output4 = UserLinearRegression.userLinearRegression(output1)
//    for(i <- 0 to output4.length - 1)
//    println(output4.apply(i))

//    //计算用户特征
//    val output5 = UserLinearRegression.userFeature(output1)
////    println(output5.apply(0))
////    for(i <- 0 to output5.length - 1)
////      println(i+":"+output5.apply(i))
//
//    //计算物品特征
//    val output6 = UserLinearRegression.itemFeature(output1)
////    println(output6.apply(0))
////    for(i <- 0 to output6.length - 1)
////      println(i+":"+output6.apply(i))
//
////    val schema = StructType(
////      List(
////        StructField("w", StringType, true),
////        StructField("e", StringType, true),
////        StructField("userid", StringType, true)
////      )
////    )
//
//    val usfeature = output5.map(x => UserFeature(x.getAs[DenseVector](0),x.getString(1)))
//    val bufeature = output6.map(x => ItemFeature(x.getAs[DenseVector](0),x.getString(1)))
//    val feature = output4.map(x => we(x.getAs[DenseVector](0),x.getDouble(1), x.getString(2)))
    val session = analysis.getdata.ss
    val userrdd = output1.groupBy("user_id").agg(FeatureAgg(output1.col("topicDistribution"))).toDF("user_id","feature")
    //加入时间权重
//    val userrdd = output1.groupBy("user_id").agg(FeatureWeightAgg(
//      output1.col("topicDistribution"),output1.col("weight"))).toDF("user_id","feature")
    userrdd.show()
    analysis.outputResult(userrdd,"parquet", 1,SocialTextTest.userFeaturePath)
    val itemrdd = output1.groupBy("business_id").agg(FeatureAgg(output1.col("topicDistribution"))).toDF("business_id","feature")
    //加入时间权重
//    val itemrdd = output1.groupBy("business_id").agg(FeatureWeightAgg(
//      output1.col("topicDistribution"),output1.col("weight"))).toDF("business_id","feature")
    itemrdd.show()
    analysis.outputResult(itemrdd,"parquet", 1,SocialTextTest.itemFeaturePath)

  }
}

import org.apache.spark.sql.functions._
/**
  * 步骤三：普通聚类
  */
object Clustering{
  def main(args: Array[String]) {
    //聚类
    val analysis = new DataAnalysis(SocialTextTest.rootPath)
        val training = analysis.getData(SocialTextTest.userFeaturePath, "parquet")
        training.show(100)
    //    val testing = analysis.getData(SocialTextTest.testPath, "parquet")
    //    testing.show(100)
//        val gmm = new BKM()
//        val gmmModel = gmm.fit(training, "feature", 2)
//        val clusterData = gmm.transform(training, "feature", gmmModel)
//          .select("feature", "user_id", "prediction")
//          .toDF("feature", "user_id", "class").orderBy("user_id")
    //全部数据当作一类
    val clusterData = training.withColumn("class", lit(1)).select("feature", "user_id", "class")
        clusterData.show(1000)
        analysis.outputResult(clusterData,"parquet", SocialTextTest.clusterPath)
  }
}

/**
  * 步骤三：社交网络聚类
  */
object SocialResultTransform{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SocialTextTest.rootPath)

    //CNM
    val result1 =
      analysis.getData(SocialTextTest.socialAlgorithmResult, "csv1")
        .toDF("user_id","class1")
    var result = result1.withColumn("class", result1.col("class1").cast(IntegerType)).select("user_id","class")
    //result.show(500)

    //bigclam and coda
    if(SocialTextTest.socialAl.equals("2")) {
      val group = analysis.getData(SocialTextTest.CodaResult, "text")
      result = analysis.getdata.transGroupNum(group)
      group.show(false)
      result.show(2000)
    }
    println("usernum:"+result.select("user_id").distinct().count())
    analysis.outputResult(result,"parquet", 1,SocialTextTest.socialPath)
  }
}
/**
  * 步骤四：计算每个用户的线性回归模型并保存
  * 个性化模型使用
  */
object LinearRegressionModelSaver{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SocialTextTest.rootPath)
//    val output1 = analysis.getData("output/YelpTextFeature0.001/part-00000-9c56faff-aea0-4bed-838b-3b30f4845920.snappy.parquet","parquet")
//    output1.show(false)
    val output1 = analysis.getData(SocialTextTest.trainingPath,"parquet")
    output1.show(false)
    val output2 = UserLinearRegression.userModelLinearRegressionFunctional(output1)
//    for(i <- 0 to output2.size - 1) {
//      val ob = output2.apply(i)
//      if(ob != null) {
//        val model = ob._2
//        val userid = ob._1
//        model.save(SocialTextTest.userModelPath + userid)
//      }
//    }
//    output2.foreach(row => {
////      val model = row.getAs[LinearRegressionModel](1)
////      val userid = row.getString(0)
//      val lrmodel = row.getAs[LRModel](0)
//      val userid = lrmodel.userid
//      val model = lrmodel.model
//      model.save(SocialTextTest.userModelPath + userid)
//    })
  }
}
/**
  * 步骤四：计算用户群的线性回归模型并保存
  * 聚类模型使用
  */
object LinearRegressionModelClusterSaver {
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SocialTextTest.rootPath)
    //    val output1 = analysis.getData("output/YelpTextFeature0.001/part-00000-9c56faff-aea0-4bed-838b-3b30f4845920.snappy.parquet","parquet")
    //    output1.show(false)
    val cluster = analysis.getData(SocialTextTest.clusterPath, "parquet")
      .select("user_id", "class")
    cluster.show(false)
    val training = analysis.getData(SocialTextTest.trainingPath, "parquet")
    val result = training.join(cluster, "user_id")
    result.show()
    if(SocialTextTest.clusterModel.equals("LR"))
      UserLinearRegression.userModelLinearRegressionClusterFunctional(result)
    else
      UserLinearRegression.userModelGBTClusterFunctional(SocialTextTest.clusterModel, result)
  }
}

/**
  * 步骤四：计算用户群的线性回归模型并保存
  * 社交模型使用
  */
object LinearRegressionModelSocialSaver {
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SocialTextTest.rootPath)
    //    val output1 = analysis.getData("output/YelpTextFeature0.001/part-00000-9c56faff-aea0-4bed-838b-3b30f4845920.snappy.parquet","parquet")
    //    output1.show(false)
    val cluster = analysis.getData(SocialTextTest.socialPath, "parquet")
      .select("user_id", "class")
    println("---------------Social Data---------------")
    cluster.show(false)
    val training = analysis.getData(SocialTextTest.trainingPath, "parquet")
    val result = training.join(cluster, "user_id").cache()
    println("---------------Training Data---------------")
    result.sort("business_id").show(false)
    UserLinearRegression.userModelLinearRegressionClusterFunctional(result)
    //UserLinearRegression.userModelGBTClusterFunctional(result)
  }
}

object LinearRegressionModelReader{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SocialTextTest.rootPath)
    //此处应该输入用户和物品的混合特征pq
    val output2 = analysis.getData("output/userfeature/part-00000-e4d7c86f-9a89-4b30-90b5-76c708c94475-c000.snappy.parquet","parquet")
    output2.show(false)
//    output2.map(row => {
//      val userid = row.getString(1)
//      val userModel = LinearRegressionModel.load("/home/pi/doc/dataset/output/model/"+userid)
//      userModel.setFeaturesCol("feature")
//      userModel.transform()
//    })
    val prediction = UserLinearRegression.userLinearRegressionPrediection(output2)
    for(i <- 0 to prediction.size - 1)
      if(prediction.apply(i) != null)
        prediction.apply(i).show(false)

  }
}

/**
  * 步骤五：使用用户和物品特征的积和用户的线性模型预测评分
  * 个性化模型使用
  */

object Prediction{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SocialTextTest.rootPath)
//    val output1 = analysis.getData("output/YelpTextFeature0.001/part-00000-9c56faff-aea0-4bed-838b-3b30f4845920.snappy.parquet","parquet")
//    output1.show(false)
    val output1 = analysis.getData(SocialTextTest.testPath,"parquet")
    output1.show(false)
    val output2 = analysis.getData(SocialTextTest.userFeaturePath,"parquet")
      .toDF("user_feature", "user_id")
    output2.show(false)
    val output3 = analysis.getData(SocialTextTest.itemFeaturePath,"parquet")
      .toDF("business_feature", "business_id")
    output3.show(false)
    val prediction = UserLinearRegression.userItemPrediction(SocialTextTest.userModelPath, output1, output2, output3)
    val fprediction = prediction.map(row => Feature3(row.getString(0), row.getString(1),  row.getInt(2),
      row.getAs[DenseVector](3), row.getAs[DenseVector](4), row.getAs[DenseVector](5), row.getDouble(6)))
    val session = analysis.getdata.ss
    val predictionResult = session.createDataFrame(fprediction)
    analysis.outputResult(predictionResult,"parquet", 1,SocialTextTest.predicitonResultPath)
    val selected = predictionResult.select("star", "prediction")
    val filterdata = UserLinearRegression.filterPrediction("star", "prediction", selected)
    filterdata.show(1000)
    val result = new LinearRegressionAl().evaluate("rmse", "star", "prediction", filterdata)
    println(result)
  }
}
/**
  * 步骤五：使用用户和物品特征的积和用户所属类的线性模型预测评分
  * 聚类模型使用
  */
  object ClusterPrediction{
    def main(args: Array[String]) {
      val analysis = new DataAnalysis(SocialTextTest.rootPath)
      //    val output1 = analysis.getData("output/YelpTextFeature0.001/part-00000-9c56faff-aea0-4bed-838b-3b30f4845920.snappy.parquet","parquet")
      //    output1.show(false)
      val testData = analysis.getData(SocialTextTest.testPath,"parquet")
      testData.show(false)
      val clusterData = analysis.getData(SocialTextTest.clusterPath,"parquet")
          .select("user_id", "class").dropDuplicates()
      clusterData.show(150)
      val output1 = testData.join(clusterData, "user_id")
      output1.show()
      val output2 = analysis.getData(SocialTextTest.userFeaturePath,"parquet")
        .toDF("user_id", "user_feature")
      output2.show(false)
      val output3 = analysis.getData(SocialTextTest.itemFeaturePath,"parquet")
        .toDF("business_id", "business_feature")
      output3.show(false)
      //注意修改预测模型
      val prediction = UserLinearRegression.userItemClusterPrediction(SocialTextTest.clusterModel,
        SocialTextTest.userModelPath, SocialTextTest.userModelMixCluster, output1, output2, output3)
      val fprediction = prediction.map(row => Feature4(row.getString(0), row.getString(1),  row.getInt(2),
        row.getAs[DenseVector](3), row.getAs[DenseVector](4), row.getAs[DenseVector](5), row.getInt(6), row.getDouble(7)))
      val session = analysis.getdata.ss
      val predictionResult = session.createDataFrame(fprediction)
      //analysis.outputResult(predictionResult,"parquet",SocialTextTest.predicitonResultPath)
      val selected = predictionResult.select("star", "prediction")
      val filterdata = UserLinearRegression.filterPrediction("star", "prediction", selected)
      filterdata.show(1000)
//      val result = new LinearRegressionAl().evaluate("rmse", "star", "prediction", filterdata)
//      println(result)
      val result1 = new LinearRegressionAl().evaluate("rmse", "star", "prediction", filterdata)
      val result2 = new LinearRegressionAl().evaluate("mae", "star", "prediction", filterdata)
      println(result1)
      println(result2)
    }
  }

/**
  * 步骤五(1)：使用用户和物品特征的积和用户所属类的线性模型预测评分
  * 社交模型使用
  * 注意：有些testdata并没有好友关系，所以找不到对应的类别，没有做预测
  */
object SocialPrediction1{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SocialTextTest.rootPath)
    //    val output1 = analysis.getData("output/YelpTextFeature0.001/part-00000-9c56faff-aea0-4bed-838b-3b30f4845920.snappy.parquet","parquet")
    //    output1.show(false)
    val testData = analysis.getData(SocialTextTest.testPath,"parquet")
    testData.show(false)

    val clusterData = analysis.getData(SocialTextTest.socialPath,"parquet")
      .select("user_id", "class")
      //选择其中一个类别预测
      //.groupBy("user_id").max("class").toDF("user_id", "class")
    clusterData.show(150)
    //没有社区的没有做预测，被过滤了
    val output1 = testData.join(clusterData, "user_id")
    output1.show()
    println("Test Data Count:"+testData.count())
    println("Social Test Count: "+output1.count())
    val output2 = analysis.getData(SocialTextTest.userFeaturePath,"parquet")
      .toDF("user_id", "user_feature")
    output2.show(false)
    val output3 = analysis.getData(SocialTextTest.itemFeaturePath,"parquet")
      .toDF("business_id", "business_feature")
    output3.show(false)
    //注意修改预测模型
    val prediction = UserLinearRegression.userItemClusterPrediction("LR", SocialTextTest.userModelPath,
      SocialTextTest.userModelMixCluster, output1, output2, output3)
    val fprediction = prediction.map(row => Feature4(row.getString(0), row.getString(1),  row.getInt(2),
      row.getAs[DenseVector](3), row.getAs[DenseVector](4), row.getAs[DenseVector](5), row.getInt(6), row.getDouble(7)))
    val session = analysis.getdata.ss
    val predictionResult = session.createDataFrame(fprediction)
    val selected1 = predictionResult.select("user_id", "business_id","star", "prediction")
    selected1.show(1000)
    analysis.outputResult(selected1,"parquet",SocialTextTest.predicitonSocialPath)

  }
}

case class MixPrediction(user_id:String, business_id:String, star:Int, prediction:Double)
object SocialMixPrediction{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SocialTextTest.rootPath)
    //    val output1 = analysis.getData("output/YelpTextFeature0.001/part-00000-9c56faff-aea0-4bed-838b-3b30f4845920.snappy.parquet","parquet")
    //    output1.show(false)
    val testData = analysis.getData(SocialTextTest.testPath,"parquet")
    testData.show(false)

    val clusterData = analysis.getData(SocialTextTest.socialPath,"parquet")
      .select("user_id", "class")
    //选择其中一个类别预测
    //.groupBy("user_id").min("class").toDF("user_id", "class")
    clusterData.show(150)
    //没有社区的没有做预测，被过滤了
    val output1 = testData.join(clusterData, "user_id")
    output1.show()
    println("Social Test Count: "+output1.count())
    val output2 = analysis.getData(SocialTextTest.userFeaturePath,"parquet")
      .toDF("user_id", "user_feature")
    output2.show(false)
    val output3 = analysis.getData(SocialTextTest.itemFeaturePath,"parquet")
      .toDF("business_id", "business_feature")
    output3.show(false)
    //注意修改预测模型
    val prediction = UserLinearRegression.userItemClusterPrediction(SocialTextTest.per,
      SocialTextTest.userModelMixSocial,
      SocialTextTest.userModelMixCluster,output1, output2, output3)
    println("RowNum:"+prediction.size)
    val fprediction = prediction.filter(row => row!=null)
      .map(row =>MixPrediction(row.getString(0), row.getString(1), row.getInt(2), row.getDouble(3)))
    val session = analysis.getdata.ss
    val predictionResult = session.createDataFrame(fprediction)
    val selected1 = predictionResult.toDF("user_id", "business_id", "star", "prediction")
    selected1.show(100)
    //analysis.outputResult(selected1,"parquet",SocialTextTest.predicitonSocialPath)
    val filterdata = UserLinearRegression.filterPrediction("star", "prediction", selected1)
    filterdata.show(100)
//    println("PredictNum:"+selected2.count())
//    println("PredictUserNum:"+selected1.select("user_id").distinct().count())
    val result1 = new LinearRegressionAl().evaluate("rmse", "star", "prediction", filterdata)
    val result2 = new LinearRegressionAl().evaluate("mae", "star", "prediction", filterdata)
    println(result1)
    println(result2)
  }
}
/**
  * 步骤五(2)：使用用户和物品特征的积和用户所属类的线性模型预测评分
  * 社交模型使用
  */
object SocialPrediction2 {
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SocialTextTest.rootPath)
    val selected1 = analysis.getData(SocialTextTest.predicitonSocialPath,"parquet")
    val selected2 = selected1
      //使用user所在类别的平均分预测
//      .groupBy("user_id","business_id", "star").avg("prediction")
//      .select("star", "avg(prediction)")
//      .toDF("star", "prediction")
      //选择所有类别预测
        .select("star", "prediction")
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

object ShowSomething{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SocialTextTest.rootPath)
    val output1 = analysis.getData(SocialTextTest.dataPath,"parquet")
    output1.show(false)
    val output2 = analysis.getData(SocialTextTest.testPath,"parquet")
    output2.show(false)
    val output3 = analysis.getData(SocialTextTest.trainingPath,"parquet")
    output3.show(false)
//    val output4 = analysis.getData(SocialTextTest.socialPath,"parquet")
//    output4.orderBy("class").show(1000,false)
//    val output5 = analysis.getData(SocialTextTest.userFeaturePath,"parquet")
//    output5.show(false)
    println("TotalCount:"+output1.count())
    println("TotalTestCount:"+output2.count())
    println("TotalTrainCount:"+output3.count())
    println("TotalUserCount:"+output1.select("user_id").distinct().count())
    println("TotalItemCount:"+output1.select("business_id").distinct().count())
    println("TestUserCount:"+output2.select("user_id").distinct().count())
    println("TrainUserCount:"+output3.select("user_id").distinct().count())
    println("TestItemCount:"+output2.select("business_id").distinct().count())
    println("TrainItemCount:"+output3.select("business_id").distinct().count())
//    println("TrainClassCount:"+output4.select("class").distinct().count())
//    println("TrainClusterUserCount:"+output4.select("user_id").distinct().count())
//    println("TrainFeatureUserCount:"+output5.select("user_id").distinct().count())

    //println("UserCount:"+UserLinearRegression.getUserId(output1).size)
//    val selected = output1.select("star", "prediction")
//    val filterdata = UserLinearRegression.filterPrediction("star", "prediction", selected)
//    filterdata.show(8000)
//    val result = new LinearRegressionAl().evaluate("rmse", "star", "prediction", filterdata)
//    println(result)
  }

}

object DoSomething{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SocialTextTest.rootPath)
    val output1 = analysis.getData(SocialTextTest.dataPath,"parquet")
      .select("user_id", "business_id", "stars")
    output1.show()
//    val output2 = output1.withColumn("weight", GussianDistribution.calculate(1.0, 0, output1.col("s")))
//    output2.show(false)
//    val output3 = output1.withColumn("s",output1.col("stars").cast(IntegerType))
//    output3.show()
    //analysis.outputResult(output3, "parquet", 1, SocialTextTest.testPath+"s")
    analysis.outputResult(output1, "csv", 1, "output/EnglishPaper/CsvDataReview5/AllData")
//    analysis.getData(SocialTextTest.trainingPath,"parquet")
//      .select("s").agg(avg("s")).show()
  }

}


object ConvertSocialNetwork{
  def main(args: Array[String]) {

    val analysis = new DataAnalysis(SocialTextTest.rootPath)
    val spark = analysis.getdata.ss
    import spark.implicits._
    val output1 = analysis.getData(SocialTextTest.socialAlgorithmResult,"csv1")
    output1.show()
    val count = output1.select("_c1").groupBy("_c1").count().orderBy(desc("count"))
    count.show()
    val result = output1.join(count, "_c1").map(row => {
      val group = row.getString(0)
      val user_id = row.getString(1)
      val count = row.getLong(2)
      if(count<5)
        (user_id, "10")
      else (user_id, group)
    }).toDF()

    analysis.outputResult(result, "csv", 1, "output/EnglishPaper/Yelp2016UserBusinessStarReview5ClusterSkipFiltered.txt")
  }
}
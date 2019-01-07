package scala.vector

import org.apache.spark.ml.feature.{Word2Vec, PCA}
import org.apache.spark.ml.linalg.{SparseVector, DenseVector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DataType, IntegerType, DoubleType}

import scala.algorithm.{LinearRegressionAl, UserLinearRegression, VectorAgg}
import scala.datafilter.{WriteFile, DataAnalysis}
import scala.socialtext.{Feature4, Feature3}

/**
  * Created by pi on 18-1-29.
  */
object VectorProcessing {
  var rootPath = "G:/Ubuntu/doc/dataset/"
  var modelType = "LR"
//  var factorNum =10
//  val pcaNum = 10
  var word2VecNum = 10
  var reviewNum = 20
  var minCount = 5
  var windowNum = 5
  //1=CNM,2=CoDA
  var socialAl = "coda"
//  val vectorPath = "glove/glove.6B."+factorNum+"d.txt"
  var textPath = "Yelp2016UserBusinessStarReview"+reviewNum
  var trainPath = "output/Access/Yelp2016UserBusinessStarReview"+reviewNum+"Training0.8"
  var testPath = "output/Access/Yelp2016UserBusinessStarReview"+reviewNum+"Testing0.2"
  var featurePath = "output/Access/YelpTextUserReviewMorethan"+reviewNum+"Feature"+word2VecNum+"Window"+windowNum
  var userFeaturePath = "output/Access/YelpTextUserReviewMorethan"+reviewNum+"UserFeature"+word2VecNum+"Window"+windowNum
  var itemFeaturePath = "output/Access/YelpTextUserReviewMorethan"+reviewNum+"ItemFeature"+word2VecNum+"Window"+windowNum
  var userModelPath = "output/Access/wordUserModel/"
  var socialModelPath = "output/Access/socialUserModel/"
  var predicitonResultPath = "output/Access/wordPrediction0.2"

  var cnmResult = "Yelp2016UserBusinessStarReview"+reviewNum+"cnm2.txt"
  //Coda
  var codaResult = "Review"+reviewNum+"mc50xc200ClusterSkipcmtyvv.in.txt"
  var socialPath = "output/Access/Yelp2016UserBusinessStarReview"+reviewNum+"SocialIDToInt"

  var analysis = new DataAnalysis(rootPath)
  val ss = analysis.getdata.ss
  import ss.implicits._

  /**
    * 分割数据
    */
  def splitData: Unit ={
    val text = analysis.getData(textPath,"parquet")
    //text.show()
    val Array(training, testing) = text.randomSplit(Array(8,2))
    analysis.outputResult(training,"parquet", 1, trainPath)
    analysis.outputResult(testing,"parquet", 1, testPath)
  }

  /**
    * 生成评论的特征
    */
  def generateWordFeature: Unit ={

    //读取glove词向量
//    val rawVectors = analysis.getData(vectorPath,"csv2")
//      .withColumnRenamed("_c0", "word").filter(col("_c1").isNotNull)
//    //rawVectors.show(false)
//    var numcols :Seq[String] = Seq()
//    for(i <- 1 to factorNum){
//      //numcols = numcols.++(Seq("_c"+i))
//      val s = "_c"+i
//      numcols = numcols :+ s
//    }
    //将词库读取到dataframe
//    val arrays = rawVectors.withColumn("array", array(numcols.map(x=>col(x).cast(DoubleType)): _*))
//      .select("word", "array")
//    //arrays.show()
    //将array转换成为vector形式
//    val vectors = arrays.map(row =>
//      (row.getString(0), new DenseVector(row.getSeq[Double](1).toArray))).toDF("word","vectors")
//    val featureVectors = vectors.withColumn("feature", vectors.col("vectors"))
//      .select("word", "feature")
    //featureVectors.show()


    //PCA降维
//    val pca = new PCA()
//      .setInputCol("features")
//      .setOutputCol("feature")
//
//      .setK(pcaNum)
//      .fit(featureVectors)
//
//    val pcaVectors = pca.transform(featureVectors).select("word", "feature")
//    pcaVectors.show(100,false)

    //训练数据
    val training = analysis.getData(trainPath,"parquet")
    //training.show()

    //分词并去除停用词
    val textArray = analysis.textToFeatureArray(training, "text")
      .select("user_id", "business_id", "stars", "remove2")
      .toDF("user_id", "business_id", "stars", "text")

    //textArray.show()
    //将句子行展开成为词语行
//    val textExplode = textArray.withColumn("word", explode(textArray.col("remove2")))
//      .select("user_id", "business_id", "stars", "word")
    //textExplode.show()

    //word2vector
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("feature")
      .setVectorSize(word2VecNum)
      .setMinCount(minCount)
      .setWindowSize(windowNum)
    val model = word2Vec.fit(textArray)

    val wordVector = model.transform(textArray)
      .select("user_id", "business_id", "stars", "feature")
      .map(row => {
        val user_id = row.getInt(0)
        val business_id = row.getInt(1)
        val stars = row.getDouble(2)
        val feature = row.getAs[SparseVector](3).toDense
        (user_id, business_id, stars, feature)
      })
    //wordVector.show(100,false)



    //将每个词的词向量取出
//    val wordVector = textExplode.join(featureVectors, "word")
//    //wordVector.show()
    //将每个用户对商家的评论向量通过词向量相加得到
//    val textVector = wordVector.drop("word").groupBy("user_id", "business_id")
//      .agg(avg("stars"), VectorAgg(col("feature")))
////    val textVector = wordVector.drop("word").groupBy("user_id", "business_id", "stars")
////        .agg(VectorAgg(col("feature")))
//    //textVector.show(100,false)
//
    //保存结果
    analysis.outputResult(wordVector.toDF("user_id", "business_id", "stars", "feature"),"parquet", 1, featurePath)
  }

  /**
    * 通过评论特征生成商家和用户特征
    */
  def userItemFeature: Unit ={
    val rawVectors = analysis.getData(featurePath,"parquet")
      //.map(x => (x(0).toString, x(1).toString, x(2).toString.toDouble, new DenseVector(x.getSeq[Double](3).toArray)))
      //.toDF("user_id", "business_id", "stars", "feature")
    //rawVectors.show()
    val userrdd = rawVectors.groupBy("user_id").agg(VectorAgg(rawVectors.col("feature"))).toDF("user_id","feature")
    val itemrdd = rawVectors.groupBy("business_id").agg(VectorAgg(rawVectors.col("feature"))).toDF("business_id","feature")
    //userrdd.show()
    //itemrdd.show()
    analysis.outputResult(userrdd,"parquet", 1, userFeaturePath)
    analysis.outputResult(itemrdd,"parquet", 1, itemFeaturePath)
  }

  //训练整体模型
  def trainingUserModel: Unit ={
    val training = analysis.getData(featurePath,"parquet")
      //.map(x => (x(0).toString.toInt, x(1).toString.toInt, x(2).toString.toDouble, new DenseVector(x.getSeq[Double](3).toArray)))
      .toDF("user_id", "business_id", "s", "topicDistribution")
      .withColumn("class", lit(1))
    //training.show()
    println("TrainingNum:"+training.count())
    if(modelType.equals("LR"))
    UserLinearRegression.userModelLinearRegressionClusterFunctional(training,userModelPath)
    else
    UserLinearRegression.userModelGBTClusterFunctional(modelType, training, userModelPath)
  }

  /**
    * 训练社交网络模型
    */
  def SocialResultTransform{

    //CNM
    val cnm =
      analysis.getData(cnmResult, "csv1")
        .toDF("user_id","class1")
    var cnmresult = cnm.withColumn("class", cnm.col("class1").cast(IntegerType)).select("user_id","class")
    //result.show(500)

    //coda
    if(socialAl.equals("coda")) {
      val group = analysis.getData(codaResult, "text")
      val presocial = analysis.getdata.transGroupNum(group)
      cnmresult = presocial.join(presocial.groupBy("class").count(), "class")
        .filter(col("count").gt(10)).select("user_id", "class")
      //group.show(false)
      //cnmresult.show(2000)
    }
    analysis.outputResult(cnmresult,"parquet", 1,socialPath)
    val cluster = cnmresult
    //println("---------------Social Data---------------")
    //cluster.show(false)
    val training = analysis.getData(featurePath, "parquet")
      .toDF("user_id", "business_id", "s", "topicDistribution")
    val result = training.join(cluster, "user_id").cache()
    //println("---------------Training Data---------------")
    //result.sort("business_id").show(false)
    if(modelType.equals("LR"))
      UserLinearRegression.userModelLinearRegressionClusterFunctional(result,socialModelPath)
    else
      UserLinearRegression.userModelGBTClusterFunctional(modelType, result, socialModelPath)

  }

  //预测
  def prediction: Unit ={

    val output1 = analysis.getData(testPath,"parquet")
        .select("user_id", "business_id", "stars")
      .map(x => (x(0).toString.toInt, x(1).toString.toInt, x(2).toString.toDouble.toInt))
      .toDF("user_id", "business_id", "s")
      .withColumn("class", lit(1))
    //output1.show()
    val output2 = analysis.getData(userFeaturePath,"parquet")
      .toDF("user_id", "user_feature")
    //output2.show()
    val output3 = analysis.getData(itemFeaturePath,"parquet")
      .toDF("business_id", "business_feature")
    //output3.show()
    val prediction = UserLinearRegression.userItemClusterVectorPrediction(modelType, userModelPath, output1, output2, output3)
    val fprediction = prediction.map(row => Feature4(row.getString(0), row.getString(1),  row.getInt(2),
      row.getAs[DenseVector](3), row.getAs[DenseVector](4), row.getAs[DenseVector](5), row.getInt(6), row.getDouble(7)))
    val session = analysis.getdata.ss
    val predictionResult = session.createDataFrame(fprediction)
    //analysis.outputResult(predictionResult,"parquet",SocialTextTest.predicitonResultPath)
    val selected = predictionResult.select("star", "prediction")
    val filterdata = UserLinearRegression.filterPrediction("star", "prediction", selected)
    //filterdata.show(100)
    val result1 = new LinearRegressionAl().evaluate("rmse", "star", "prediction", filterdata)
    val result2 = new LinearRegressionAl().evaluate("mae", "star", "prediction", filterdata)
    //val writefile = "\nClusterType"+"\nReview Num:"+reviewNum+"\nWord2Vec Num:"+word2VecNum+"\n"+result1+"\n"+result2+"\n"
    //new WriteFile().append("G:/Project/HybridRecommendation/data/word2vec.txt", writefile)
    //println(writefile)
    println("TestNum:"+output1.count())
  }

  def socialPrediction ={
    val testData = analysis.getData(testPath,"parquet")
      .select("user_id", "business_id", "stars")
      .map(x => (x(0).toString.toInt, x(1).toString.toInt, x(2).toString.toDouble.toInt))
      .toDF("user_id", "business_id", "s")

    val clusterData = analysis.getData(socialPath,"parquet")
      .select("user_id", "class")
    //选择其中一个类别预测
    .groupBy("user_id").min("class").toDF("user_id", "class")
    //clusterData.show(150)
    //没有社区的没有做预测，被过滤了
    val output1 = testData.join(clusterData, "user_id")
    //output1.show()
    println("Test Data Count:"+testData.count())
    println("Social Test Count: "+output1.count())
    val output2 = analysis.getData(userFeaturePath,"parquet")
      .toDF("user_id", "user_feature")
    //output2.show(false)
    val output3 = analysis.getData(itemFeaturePath,"parquet")
      .toDF("business_id", "business_feature")
    //output3.show(false)
    //注意修改预测模型
    val prediction = UserLinearRegression.userItemClusterVectorPrediction(modelType, socialModelPath, output1, output2, output3)
    val fprediction = prediction.map(row => Feature4(row.getString(0), row.getString(1),  row.getInt(2),
      row.getAs[DenseVector](3), row.getAs[DenseVector](4), row.getAs[DenseVector](5), row.getInt(6), row.getDouble(7)))
    val session = analysis.getdata.ss
    val predictionResult = session.createDataFrame(fprediction)
    val selected1 = predictionResult.select("user_id", "business_id","star", "prediction")
    val selected2 = selected1
      //使用user所在类别的平均分预测
            .groupBy("user_id","business_id", "star").avg("prediction")
            .select("star", "avg(prediction)")
            .toDF("star", "prediction")
      //选择所有类别预测
      //.select("star", "prediction")
    val filterdata = UserLinearRegression.filterPrediction("star", "prediction", selected2)
    //filterdata.show(1000)
    //println("PredictNum:"+selected2.count())
    //println("PredictUserNum:"+selected1.select("user_id").distinct().count())
    val result1 = new LinearRegressionAl().evaluate("rmse", "star", "prediction", filterdata)
    val result2 = new LinearRegressionAl().evaluate("mae", "star", "prediction", filterdata)
    println(result1)
    println(result2)
    //val writefile = "\nSocialType:"+socialAl+"\nReview Num:"+reviewNum+"\nWord2Vec Num:"+word2VecNum+"\n"+result1+"\n"+result2+"\n"
    //new WriteFile().append("/home/pi/software/WORKSPACES/github_demo/alicloud-ams-demo/Scala2/data/word2vec.txt", writefile)
  }
  def main(args: Array[String]) {

    var p = ""
    var n = 0
    val params: Array[String] = args
    for (p <- params) {
      if (n % 2 == 1){
        n+=1
        //println("n+1")
      }
      else if (p.equals("--root_path")) {
        rootPath = params.apply(n + 1)
        n += 1
        println("--root_path: "+rootPath)
        analysis = new DataAnalysis(rootPath)
      }
      else if (p.equals("--task")) {
        val task = params.apply(n + 1)
        if (task.equals("DataSplit")) {
        println("1.Start Split Data...")
        splitData
        println("1.Split Data Finish!")
      }
        n += 1
      }
      else if (p.equals("--model_type")) {
        modelType = params.apply(n + 1)
        n += 1
        println("--model_type: "+modelType)
      }
      else if (p.equals("--word2vec_num")){
        word2VecNum = params.apply(n+1).toInt
        n += 1
        println("--word2vec_num: "+word2VecNum)
      }
      else if (p.equals("--review_num")){
        reviewNum = params.apply(n+1).toInt
        n += 1
        println("--review_num: "+reviewNum)
      }
      else if (p.equals("--min_count")){
        minCount = params.apply(n+1).toInt
        n += 1
        println("--min_count: "+minCount)
      }
      else if (p.equals("--window_num")){
        windowNum = params.apply(n+1).toInt
        n += 1
        println("--window_num: "+windowNum)
      }
      else if (p.equals("--social_type")){
        socialAl = params.apply(n+1)
        n += 1
        println("--social_type: "+socialAl)
      }
      else if (p.equals("--cnm_result")){
        cnmResult = params.apply(n+1)
        n += 1
        println("--cnm_result: "+cnmResult)
      }
      else if (p.equals("--coda_result")){
        codaResult = params.apply(n+1)
        n += 1
        println("--coda_result: "+codaResult)
      }
      else {
        println("Wrong params, please check!")
        return
      }
    }

    while (word2VecNum < 60) {
      println("Word2vec number:", word2VecNum)
      println("2. Start generateWordFeature...")
      generateWordFeature
      println("2. GenerateWordFeature Finish!")
      println("3. Start userItemFeature...")
      userItemFeature
      println("3. UserItemFeature Finish!")
      println("4. Start trainingUserModel...")
      trainingUserModel
      println("4. Training ClusterModel Finish!")
//      println("5.1 Start ClusterModel prediction...")
//      prediction
//      println("5.1 Prediction ClusterModel finish!")
//      socialAl = "cnm"
//      println("5.2 Start "+socialAl +" prediction...")
//      SocialResultTransform
//      socialPrediction
//      println("5.2 Prediction "+socialAl +" finish!")
//      socialAl = "coda"
      println("5.2 Start "+socialAl +" prediction...")
      if(socialAl.equals("coda"))
      SocialResultTransform
      else SocialResultTransform
      socialPrediction
      println("5.3 Prediction "+socialAl +" finish!")
      word2VecNum += 10
      featurePath = "output/YelpTextUserReviewMorethan"+reviewNum+"Feature"+word2VecNum+"Window"+windowNum
      userFeaturePath = "output/YelpTextUserReviewMorethan"+reviewNum+"UserFeature"+word2VecNum+"Window"+windowNum
      itemFeaturePath = "output/YelpTextUserReviewMorethan"+reviewNum+"ItemFeature"+word2VecNum+"Window"+windowNum
    }

  }


}

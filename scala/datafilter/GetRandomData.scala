package scala.datafilter

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

import scala.algorithm.LinearRegressionAl

/**
  * Created by pi on 17-7-1.
  */
class GetRandomData(base:String) {

  var basedir = base

  System.setProperty("hadoop.home.dir",
    "G:\\Libraries\\hadoop-common-2.2.0-bin-master")
  //本地运行模式
  val ss = SparkSession.builder().appName("Yelp Rating")
    .master("local[*]").getOrCreate()

  //集群运行模式
//  var conf = new SparkConf().setAppName("Yelp LDA").setMaster("spark://172.31.34.183:7077")
//      .set("spark.submit.deployMode","client")
//      .set("spark.memory.fraction","0.4")
//                  .set("spark.driver.cores","8")
//                  .set("spark.driver.memory","4g")
//                  .set("spark.executor.memory","32g")
//                  .set("spark.executor.cores","8")
//  val ss = SparkSession.builder().config(conf).getOrCreate()

//  以下参数不用设置
//            .setExecutorEnv(Array(("spark.driver.memory","2g"),("spark.executor.memory","8g")
//            ,("spark.driver.extraJavaOptions","-Xmx10000m -Xms10000m")
//            ,("spark.executor.extraJavaOptions","-Xmx10000m -Xms10000m")))
//    .setJars(Array("/home/pi/software/WORKSPACES/github_demo/alicloud-ams-demo/Scala2/out/artifacts/Scala2_jar/Scala2.jar"))



  import ss.implicits._
  Logger.getLogger("org").setLevel(Level.WARN)

  def getLibsvmData(path:String):DataFrame = {
    // Loads data.
    val dataset = ss.read.format("libsvm")
    .load(basedir+path)
    return dataset
  }

  def transGroupNum(dataFrame: DataFrame)={
    val split = dataFrame.map(row => row.toString().replace("[", "").replace("]", "").split("\t"))
    split.withColumn("class", monotonically_increasing_id.cast(IntegerType))
      .withColumn("user_id", explode($"value")).select("user_id", "class")
  }

  def textToFeatureArray(dataFrame: DataFrame, col:String):DataFrame={

    val input = dataFrame.withColumn("replace",regexp_replace(dataFrame.col(col),"\\p{Punct}",""))
    val token = new Tokenizer().setInputCol("replace").setOutputCol("token")
    val data = token.transform(input)
    val remover1 = new StopWordsRemover().setInputCol("token").setOutputCol("remove1").setStopWords(Array(" ",""))
    val remove1 = remover1.transform(data)
    val remover2 = new StopWordsRemover().setInputCol("remove1").setOutputCol("remove2")
    val remove2 = remover2.transform(remove1)
    return remove2
  }

  def textToVector(dataFrame: DataFrame,col: String):DataFrame={

    val input = dataFrame.withColumn("replace",regexp_replace(dataFrame.col(col),"\\p{Punct}",""))
    val token = new Tokenizer().setInputCol("replace").setOutputCol("token")
    val data = token.transform(input)
    val remover1 = new StopWordsRemover().setInputCol("token").setOutputCol("remove1").setStopWords(Array(" ",""))
    val remove1 = remover1.transform(data)
    val remover2 = new StopWordsRemover().setInputCol("remove1").setOutputCol("remove2")
    val remove2 = remover2.transform(remove1)
    val transformer = new CountVectorizer().setInputCol("remove2").setOutputCol("vector")
    val cvModel = transformer.fit(remove2)
    val vocabulary = cvModel.vocabulary
//    vocabulary.foreach(x => {
//      var i = 0
//      print(i+":"+x.toString+" ")
//      i=i+1})
    val session = ss
    val voc = session.createDataset(vocabulary)
      .withColumn("index", monotonically_increasing_id.cast(IntegerType)).toDF()
    voc.show(1000, false)
    writeData(voc, "csv", "outputdata/VocabularyUR10LDA10")
    val vector = cvModel.transform(remove2)
    return vector
  }

  def getTextData(path:String):DataFrame = {
    // Loads data.
    val dataset = ss.read.format("text")
      .load(basedir+path)
    return dataset
  }

  def getJsonData(path:String):DataFrame = {
    // Loads data.
    val dataset = ss.read.format("json")
      .load(basedir+path)
    return dataset
  }

  def getCsvData(path:String):DataFrame = {
    // Loads data.
    val dataset = ss.read.format("csv")
      .load(basedir+path)
    return dataset
  }
  def getCsv1Data(path:String):DataFrame = {
    // Loads data.
    val dataset = ss.read.format("csv").option("sep","\t")
      .load(basedir+path)
    return dataset
  }
  def getCsv2Data(path:String):DataFrame = {
    // Loads data.
    val dataset = ss.read.format("csv").option("sep"," ")
      .load(basedir+path)
    return dataset
  }

  def getParquetData(path:String):DataFrame = {
    // Loads data.
    val dataset = ss.read.format("parquet")
      .load(basedir+path)
    return dataset
  }

  def userRegression(input:DataFrame,n:Int):String={

    val regression = new LinearRegressionAl()
//    var result = regression.transform(
//      input.filter($"user_id" === "2"), "topicDistribution", "s")
//    var result = sc.parallelize(Array("","")).toDF("topicDistribution","user_id","business_id","s")
    var vector = new DenseVector(Array(1,2,3,4,5))
    var data = List{(vector, "user","item",0,0)}
    var result = ss.createDataFrame(data).toDF("topicDistribution","user_id","business_id","s","prediction")
    println("---------result1-----------")
    result.show()
    input.select("user_id","s").distinct.collect().foreach(x => {
      val userid = x.getAs[String]("user_id")
      val rate = x.getAs[Int]("s")
      val selected = input.filter($"user_id" === userid)
      if(selected.count()>n) {
        val Array(training,testing) = selected.randomSplit(Array(0.7,0.3))
        if(training.count()>0) {
          val lr = regression.transform(testing,regression.fit(training,"topicDistribution","s"))
          val filter = rangeColume(lr,"prediction")
          result = result.union(filter)
        }
        var vector = new DenseVector(Array(1,2,3,4,5))
        //Union:3.768, Join:3.781
        var data = List{(vector, userid,"business_id",rate,3.781)}
        var r = ss.createDataFrame(data).toDF("topicDistribution","user_id","business_id","s","prediction")
        result = result.union(r)
      }
      else {
        var vector = new DenseVector(Array(1,2,3,4,5))
        var data = List{(vector, userid,"business_id",rate,3.781)}
        var r = ss.createDataFrame(data).toDF("topicDistribution","user_id","business_id","s","prediction")
        result = result.union(r)
      }
    })
    println("---------result2-----------")
    result.show()
    return regression.evaluate(result)
  }


  def itemRegression(input:DataFrame,n:Int):String={

    val regression = new LinearRegressionAl()
    //    var result = regression.transform(
    //      input.filter($"user_id" === "2"), "topicDistribution", "s")
    //    var result = sc.parallelize(Array("","")).toDF("topicDistribution","user_id","business_id","s")
    var vector = new DenseVector(Array(1,2,3,4,5))
    var data = List{(vector, "user","item",0,0)}
    var result = ss.createDataFrame(data).toDF("topicDistribution","user_id","business_id","s","prediction")
    println("---------result1-----------")
    result.show()
    input.select("business_id","s").distinct.collect().foreach(x => {
      val business_id = x.getAs[String]("business_id")
      val rate = x.getAs[Int]("s")
      val selected = input.filter($"business_id" === business_id)
      if(selected.count()>n) {
        val Array(training,testing) = selected.randomSplit(Array(0.7,0.3))
        if(training.count()>0) {
          val lr = regression.transform(testing,regression.fit(training,"topicDistribution","s"))
          val filter = rangeColume(lr,"prediction")
          result = result.union(filter)
        }
        else {
          var vector = new DenseVector(Array(1,2,3,4,5))
          var data = List{(vector, "user_id",business_id,rate,3.781)}
          var r = ss.createDataFrame(data).toDF("topicDistribution","user_id","business_id","s","prediction")
          result = result.union(r)
        }
      }
      else {
        var vector = new DenseVector(Array(1,2,3,4,5))
        var data = List{(vector, "user_id",business_id,rate,3.781)}
        var r = ss.createDataFrame(data).toDF("topicDistribution","user_id","business_id","s","prediction")
        result = result.union(r)
      }
    })
    println("---------result2-----------")
    result.show()
    return regression.evaluate(result)
  }

  def rangeColume(input:DataFrame, col:String):DataFrame={
    input.createOrReplaceTempView("table")
    val filter1 = ss.sql("select * from table where prediction >=1 and prediction <=5")
      .toDF("topicDistribution","user_id","business_id","s",col)
    val filter2 = ss.sql("select * from table where prediction <1 ")
      .toDF("topicDistribution","user_id","business_id","s","prediction<1")
    val filter3 = ss.sql("select * from table where prediction >5 ")
      .toDF("topicDistribution","user_id","business_id","s","prediction>5")
    val filter4 = filter2.withColumn(col, lit(1))
      .select("topicDistribution","user_id","business_id","s",col)
    val filter5 = filter3.withColumn(col, lit(5))
      .select("topicDistribution","user_id","business_id","s",col)
    return filter1.union(filter4).union(filter5)
  }

  /**
    * 获得百分比yelp用户对商户评分数据，求平均
    *
    * @param readpath 不含base的路径
    * @param writepath
    * @param percent
    */

  def outputYelpPercentData(readpath:String, writepath:String, percent:Double): Unit ={

    var yelpRating = getRawPercentData(readpath,percent)
    val data = getUserItemAvgrating(yelpRating,"user_id","business_id","stars")
    data.show()
    data.repartition(1).write.mode("overwrite").csv(base+writepath)
  }

  /**
    * 按照比例随机选取商家和用户（训练集和测试集无重复用户-商家对）
    * @param frame 所有数据
    * @param user 用户id名称
    * @param business 商家id名称
    * @param per 训练集百分比
    */
  def getPercentUserAndBusiness(frame: DataFrame, user:String, business:String, per:Double): Array[DataFrame] ={
    val alldata =  frame
    val userbusiness = frame.select(user, business).distinct();
    val Array(training, testing) = userbusiness.randomSplit(Array(per, 1-per))
    val trainingData = training.join(alldata, Array(user, business))
    val testingData = testing.join(alldata, Array(user, business))
    println("All Reviews:"+alldata.count()+" User-Item Pairs:"+userbusiness.count()+" Training:"+training.count()+" Testing:"+testing.count())
    return Array(trainingData, testingData)
  }

  /**
    * 读取给定路径的数据集，获得用户对商户评分并计算平均分，输出到指定路径
    *
    * @param readpath 不含base的路径
    * @param writepath
    * @param user
    * @param item
    * @param rate
    * @param per
    */
  def outputUserItemRatePecentData(readpath:String, writepath:String,
                                   user:String,item:String,rate:String,per:Double): Unit ={

    var yelpRating = getRawPercentData(readpath,per)
    val data = getUserItemAvgrating(yelpRating,user,item,rate)
    data.show()
    //data.repartition(1).write.mode("overwrite").csv(base+writepath)
    writeData(data, 1, writepath)
  }

  def writeData(input:DataFrame, par:Int, writepath:String): Unit ={
    input.repartition(par).write.mode("overwrite").csv(base+writepath)
  }

  def writeData(input:DataFrame, format:String, par:Int, writepath:String): Unit ={
    if(format.equals("csv"))
    input.repartition(par).write.mode("overwrite").csv(base+writepath)
    else if(format.equals("csv1"))
      input.repartition(par).write.option("sep","\t").mode("overwrite").csv(base+writepath)
    else if(format.equals("json"))
      input.repartition(par).write.mode("overwrite").json(base+writepath)
    else if(format.equals("parquet"))
      input.repartition(par).write.mode("overwrite").parquet(base+writepath)
  }

  def writeData(input:DataFrame, format:String, writepath:String): Unit ={
    if(format.equals("csv"))
      input.write.mode("overwrite").csv(base+writepath)
    else if(format.equals("json"))
      input.write.mode("overwrite").json(base+writepath)
    else if(format.equals("parquet"))
      input.write.mode("overwrite").parquet(base+writepath)
  }
  /**
    * 获得百分比原始数据
    *
    * @param readpath 原始数据集路径
    * @param per 百分比
    * @return 数据表
    */
  def getRawPercentData(readpath:String, per:Double) :DataFrame = {
    var yelpRating = ss.read.json(basedir+readpath)
    val Array(training,testing) = yelpRating.randomSplit(Array(per,1-per))
    return training

  }

  def getCsvRawPercentData(readpath:String, sep:String, per:Double):DataFrame = {
    val data = ss.read.format("csv").option("sep",sep)
      .csv(basedir+readpath)
    val Array(training,testing) = data.randomSplit(Array(per,1-per))
    return training
  }

  def getParquetRawPercentData(readpath:String, per:Double):DataFrame = {
    val data = ss.read.format("parquet")
      .csv(basedir+readpath)
    val Array(training,testing) = data.randomSplit(Array(per,1-per))
    return training
  }

  def getYelpUserFriendsTrustData(input:DataFrame, user:String, friends:String): DataFrame ={
    val data = input.select(user, friends)
    //data.show()
    val explodedata = data.withColumn(friends, explode($"friends"))
    //explodedata.show()
    val trust = explodedata.withColumn("trust", lit(1)).toDF("_1","_2","_3")
    //trust.show()
    //val result = getUserItemRating(trust,"user_id","friends","trust")
    return trust
  }


  /**
    * 将信任值转化为一
    *
    * @param input
    * @param user1
    * @param user2
    * @return
    */
  def transformToTrust1(input:DataFrame, user1:String, user2:String, trust:String):DataFrame ={
    val data = input.createOrReplaceTempView("table")
    val filter = ss.sql("select * from table where "+trust+" >0")
    //filter.show()
    val result = filter.select(user1,user2).withColumn("trust", lit(1))
    return result;
  }

  /**
    * 获得用户对商户的评分数据，求平均分
    *
    * @param input
    * @param user
    * @param item
    * @param rate
    * @return 数据表
    */
  def getUserItemAvgrating(input:DataFrame, user:String, item:String, rate:String):DataFrame = {
    val inputdata = getUserItemRating(input,user,item,rate)
    val data = inputdata.groupBy("_1","_2").avg("_3")
          .toDF("_1","_2","_3")
    return data
  }

  def getUserItemAvg(input:DataFrame,user:String, item:String, rate:String):DataFrame = {
    val data = input.groupBy(user, item).avg(rate).toDF("_1","_2","_3")
    return data
  }
  /**
    * 根据某列计数
    *
    * @param input
    * @param ob
    * @return
    */
  def getGroupbyCount(input:DataFrame,ob:String):DataFrame = {
    val data = input.groupBy(ob).count().toDF("_1","_2")
    return data
  }

  /**
    * 根据两列计数
    *
    * @param input
    * @param ob1
    * @param ob2
    * @return
    */
  def getGroupbyCount(input:DataFrame,ob1:String,ob2:String):DataFrame = {
    val data = input.groupBy(ob1,ob2).count().toDF("_1","_2","_3")
    return data
  }

  def selectData(input:DataFrame, col1:String, col2:String, col3:String):DataFrame= {
    return input.select(col1,col2,col3).toDF("_1","_2","_3")
  }
  def selectData(input:DataFrame, col1:String, col2:String):DataFrame= {
    return input.select(col1,col2).toDF("_1","_2")
  }

  def selectData(input:DataFrame, col1:String, col2:String, col3:String, col4:String):DataFrame= {
    return input.select(col1,col2,col3,col4).toDF("_1","_2","_3","_4")
  }

  def selectData(input:DataFrame, col1:String, col2:String, col3:String, col4:String, col5:String):DataFrame= {
    return input.select(col1,col2,col3,col4,col5).toDF("_1","_2","_3","_4","_5")
  }
  /**
    * 获得用户对商户的评分数据，不求平均分
    *
    * @param input 原始数据集表
    * @param user 原始数据集user列名
    * @param item 原始数据集item列名
    * @param rate 原始数据集rate列名
    * @return 数据表
    */
  def getUserItemRating(input:DataFrame, user:String, item:String, rate:String):DataFrame = {
    val select = input.select(user,item,rate)
    val data = transformUseridandItemidOne(select, user, item, rate)
    return data
  }
  def getUserItemRatingText(input:DataFrame, user:String, item:String, rate:String, text:String):DataFrame = {
    val select = input.select(user,item,rate,text)
    val data = transformUseridItemidRatingText(select, user, item, rate, text)
    return data
  }

  def getUserItemlalon(input:DataFrame, user:String, item:String, la:String, lon:String):DataFrame = {
    val select = input.select(user,item,la,lon)
    val data = transformUseridandItemidTwo(select,user,item, la, lon)
    return data
  }

  /**
    * transform userid and itemid into integer
    *
    * @param input input dataframe
    * @param user userid
    * @param item itemid
    * @return dataframe using integer id
    */
  def transformUseridandItemidOne(input:DataFrame, user:String, item:String, rate:String):DataFrame={
    val indexed1 = getIndexer(input,user).transform(input).sort(user+"(indexed)")
    val indexed2 = getIndexer(indexed1,item).transform(indexed1).sort(user+"(indexed)",item+"(indexed)")
//    val indexer = getIndexer(input, user)
//    val indexed1 = indexer.transform(input)
//    indexer.setInputCol(item)
//      .setOutputCol(item+"(indexed)")
//      .setHandleInvalid("skip")
//    val indexed2 = indexer.transform(indexed1).sort(user+"(indexed)",item+"(indexed)")
    //indexed2.show()
    val data = indexed2.select(user+"(indexed)",item+"(indexed)",rate)
      .map(r=>(r(0).toString.toDouble.toInt,r(1).toString.toDouble.toInt,r(2).toString.toDouble))
      .toDF("_1","_2","_3")
    return data
  }

  def transformUseridandItemid(input:DataFrame, user:String, item:String):DataFrame={
//    val indexed1 = getIndexer(input,user).transform(input).sort(user+"(indexed)")
//    val indexed2 = getIndexer(indexed1,item).transform(indexed1).sort(user+"(indexed)",item+"(indexed)")
    val indexer = getIndexer(input, user)
    val indexed1 = indexer.transform(input)
    //indexed1.show(false)
    indexer.setInputCol(item)
      .setOutputCol(item+"(indexed)")
      .setHandleInvalid("skip")
    val indexed2 = indexer.transform(indexed1).sort(user+"(indexed)",item+"(indexed)")
    //indexed2.show(false)
    val data = indexed2.select(user+"(indexed)",item+"(indexed)")
      .map(r=>(r(0).toString.toDouble.toInt,r(1).toString.toDouble.toInt))
      .toDF("_1","_2")
    return data
  }

  /**
    * 通过评论数据中用户ID数字化社交网络的用户ID，保持ID一致性
    * @param reviewData 评论数据
    * @param socialData 展开的用户好友数据
    * @param user 用户列名
    * @param friend 朋友列名
    * @return
    */
  def transformSocialNetwork(reviewData:DataFrame, socialData:DataFrame, user:String, friend:String):DataFrame={
    val indexer1 = getIndexer(reviewData.select(user), user)
    indexer1.setHandleInvalid("skip")
    val indexedSocialUser = indexer1.transform(socialData)
    //indexedSocialUser.show()
    //val indexer2 = getIndexer(reviewData.select(user).toDF(friend), friend)
    val indexer2 = indexer1
      .setInputCol(friend)
      .setOutputCol(friend+"(indexed)")
    val indexedSocialFriend = indexer2.transform(indexedSocialUser)
    //indexedSocialFriend.show()
    val result = indexedSocialFriend.select(user+"(indexed)",friend+"(indexed)")
      //.map(r=>(r(0).toString.toDouble.toInt,r(1).toString.toDouble.toInt))
//      .toDF(user,friend)
    val intResult = result.withColumn(user, result.col(user+"(indexed)").cast(IntegerType))
      .withColumn(friend, result.col(friend+"(indexed)").cast(IntegerType))
    return intResult.select(user,friend)
  }

  def transformUseridItemidRatingText(input:DataFrame, user:String, item:String, rate:String, text:String):DataFrame={
    val indexed1 = getIndexer(input,user).transform(input).sort(user+"(indexed)")
    val indexed2 = getIndexer(indexed1,item).transform(indexed1).sort(user+"(indexed)",item+"(indexed)")
    val data = indexed2.select(user+"(indexed)",item+"(indexed)",rate,text)
      .map(r=>(r(0).toString.toDouble.toInt,r(1).toString.toDouble.toInt,r(2).toString.toDouble,r(3).toString))
      .toDF("_1","_2","_3","4")
    return data
  }

  def transformUseridItemidRatingTextTime(input:DataFrame, user:String, item:String, rate:String, text:String, time:String):DataFrame={
    val indexed1 = getIndexer(input,user).transform(input).sort(user+"(indexed)")
    val indexed2 = getIndexer(indexed1,item).transform(indexed1).sort(user+"(indexed)",item+"(indexed)")
    val data = indexed2.select(user+"(indexed)",item+"(indexed)",rate,text,time)
      .map(r=>(r(0).toString.toDouble.toInt,r(1).toString.toDouble.toInt,r(2).toString.toDouble,r(3).toString,r(4).toString))
      .toDF("_1","_2","_3","4","5")
    return data
  }

  def getIndexingData(input:DataFrame, indexer:StringIndexerModel):DataFrame={
      val result1 = indexer.transform(input).withColumnRenamed("_1(indexed)","_11(indexed)")
    val input2 = result1.withColumnRenamed("_1","_11").withColumnRenamed("_2","_1")
          .withColumnRenamed("_11","_2")
    val result2 = indexer.transform(input2).withColumnRenamed("_1(indexed)","_2(indexed)")
      .withColumnRenamed("_11(indexed)","_1(indexed)").select("_1(indexed)","_2(indexed)","_3")
          .map(r=>(r(0).toString.toDouble.toInt,r(1).toString.toDouble.toInt,r(2).toString.toDouble))
          .toDF("_1","_2","_3")
    //result2.show()
    return result2
  }

  def getIndexer(input:DataFrame,col: String): StringIndexerModel={
    val indexer = new StringIndexer()
      .setInputCol(col)
        .setOutputCol(col+"(indexed)")
    val result = indexer.fit(input)
    return result
  }

  def transformUseridandItemidTwo(input:DataFrame, user:String, item:String, la:String, lon:String):DataFrame={
    val indexed1 = getIndexer(input,user).transform(input).sort(user+"(indexed)")
    val indexed2 = getIndexer(indexed1,item).transform(indexed1).sort(user+"(indexed)",item+"(indexed)")
    val data = indexed2.select("userid","itemid",la,lon)
      .map(r=>(r(0).toString.toDouble.toInt,r(1).toString.toDouble.toInt,
        r(2).toString.toDouble, r(3).toString.toDouble))
      .toDF("_1","_2","_3","_4")
    return data
  }

  /**
    * 对一列进行group然后计数列进行过滤
    *
    * @param input
    * @param ob
    * @param num
    * @return
    */
  def getUserCheckinMoreThan(input:DataFrame, ob:String, num:Int): DataFrame = {
    val select = getGroupbyCount(input,ob)
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where _2 > "+num)
    return data
  }

  def getUserCheckinLessThan(input:DataFrame, ob:String, num:Int): DataFrame = {
    val select = getGroupbyCount(input,ob)
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where _2 < "+num)
    return data
  }

  def getUserCheckinEqualWith(input:DataFrame, ob:String, num:Int): DataFrame = {
    val select = getGroupbyCount(input,ob)
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where _2 = "+num)
    return data
  }

  def getUserTrustMoreThan(input:DataFrame, ob:String, num:Int): DataFrame = {
    val select = input
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where "+ob+" > "+num)
    return data
  }
  def getUserTrustEqualWith(input:DataFrame, ob:String, num:Int): DataFrame = {
    val select = input
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where "+ob+" = "+num)
    return data
  }
  def getUserTrustLessThan(input:DataFrame, ob:String, num:Int): DataFrame = {
    val select = input
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where "+ob+" < "+num)
    return data
  }

  /**
    * 对两列进行group然后对计数列进行过滤
    *
    * @param input
    * @param ob1
    * @param ob2
    * @param num
    * @return
    */
  def getUserItemCheckinMoreThan(input:DataFrame, ob1:String, ob2:String, num:Int): DataFrame = {
    val select = getGroupbyCount(input,ob1,ob2)
    //select.show()
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where _3 > "+num)
    return data
  }

  def getUserItemCheckinLessThan(input:DataFrame, ob1:String, ob2:String, num:Int): DataFrame = {
    val select = getGroupbyCount(input,ob1,ob2)
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where _3 < "+num)
    return data
  }

  def getUserItemCheckinEqualWith(input:DataFrame, ob1:String, ob2:String, num:Int): DataFrame = {
    val select = getGroupbyCount(input,ob1,ob2)
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where _3 = "+num)
    return data
  }

}

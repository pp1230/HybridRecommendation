package scala.algorithm

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.linalg.{Vectors, DenseVector}
import org.apache.spark.ml.regression._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SparkSession, Row, DataFrame}

import scala.socialtext.{SocialTextTest, LRModel, ItemFeature}

/**
  * Created by pi on 17-9-12.
  */
object UserLinearRegression {
  val ss = SparkSession.getActiveSession.get
  import ss.implicits._

  def filterByUserId(userid:String, data:DataFrame):DataFrame = {
    data.filter("user_id == '"+userid+"'")
  }

  def filterByGroup(group:Int, data:DataFrame):DataFrame = {

    data.filter("class == "+group)
  }

  def filterByGroupAnd999(group:Int, data:DataFrame):DataFrame = {
    //添加没有被划分的用户一起训练
    if(group == 999)
      data
    else if(group == 1 || group == 2 || group == 3 || group == 5)
      data.filter("class == "+group)
    else
      data
      //data.filter("class == "+group+" OR class == 999")
  }

  def filterByItemId(itemid:String, data:DataFrame):DataFrame = {
    data.filter("business_id == '"+itemid+"'")
  }

  def getUserId(data:DataFrame):Array[Row] = {
    data.select("user_id").dropDuplicates().collect()
  }

  def getUserGroup(data:DataFrame):Array[Row] = {
    data.select("class").dropDuplicates().collect()
  }

  def getUserIdFrame(dataFrame: DataFrame):DataFrame={
    dataFrame.select("user_id").dropDuplicates().toDF()
  }

  def getGroupIdFrame(dataFrame: DataFrame):DataFrame={
    dataFrame.select("class").dropDuplicates().toDF()
  }
  def getItemId(data:DataFrame):Array[Row] = {
    data.select("business_id").dropDuplicates().collect()
  }

  def getUserId1(data:DataFrame):DataFrame = {
    data.select("user_id").dropDuplicates()
  }

  def userLinearRegression(dataFrame: DataFrame):Array[Row]={
    val rows = getUserId(dataFrame)
    var arr:Array[Row] = new Array[Row](rows.length)
    var lr = new LinearRegressionAl()
    for(i <- 0 to rows.length - 1) {
      val userid = rows.apply(i).getString(0)
      val frame = filterByUserId(userid, dataFrame)
      if(frame.count() > 1) {
        val model = lr.fit(frame, "topicDistribution", "s")
        arr.update(i, Row(model.coefficients, model.intercept.toDouble, userid))
      }
      else arr.update(i, Row(Vectors.dense(Array(0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
        0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0))
        , frame.take(1).apply(0).getInt(3).toDouble, userid))
    }
    arr
  }

  def userLinearRegressionPrediection(dataFrame: DataFrame):Array[DataFrame] = {
    val data = dataFrame.toDF("feature", "user_id")
    val rows = getUserId(data)
    var arr:Array[DataFrame] = new Array[DataFrame](rows.length)
    for(i <- 0 to rows.length - 1) {
      val userid = rows.apply(i).getString(0)
      val frame = filterByUserId(userid, data)
      val userModel = LinearRegressionModel.load("/home/pi/doc/dataset/output/usermodel/"+userid)
      if(userModel!=null) {
        userModel.setFeaturesCol("feature")
        val prediction = userModel.transform(frame)
        arr.update(i, prediction)
      }
    }
    arr
  }


  def userModelLinearRegression(dataFrame: DataFrame):Array[(String, LinearRegressionModel)]={
    val rows = getUserId(dataFrame)
    var arr:Array[(String, LinearRegressionModel)] = new Array[(String, LinearRegressionModel)](rows.length)
    var lr = new LinearRegressionAl()
    for(i <- 0 to rows.length - 1) {
      val userid = rows.apply(i).getString(0)
      val frame = filterByUserId(userid, dataFrame)
//      if(frame.count() > 1) {
        val model = lr.fit(frame, "topicDistribution", "s")
        arr.update(i, (userid, model))
//      }
//      else {
//        var model = new LinearRegressionModel()
//        model
//        arr.update(i, (userid, LinearRegressionModel))
//      }
    }
    arr
  }

  def userModelLinearRegressionFunctional(dataFrame: DataFrame)={
    val rows = getUserIdFrame(dataFrame)
    val lr = new LinearRegressionAl()
    val result = rows.collect().foreach(row => {
      val userid = row.getString(0)
      val frame = filterByUserId(userid ,dataFrame)
      //LRModel(userid, lr.fit(frame, "topicDistribution", "s"))
      val model = lr.fit(frame, "topicDistribution", "s")
      model.save(SocialTextTest.userModelPath + userid)
    })
  }

  def userModelLinearRegressionClusterFunctional(dataFrame: DataFrame)={
    val rows = getGroupIdFrame(dataFrame)
    //println("-------------Groups-------------")
    rows.show(false)
    val lr = new LinearRegressionAl()
    println("Group number:"+rows.count())
    val result = rows.collect().foreach(row => {
      val userid = row.getInt(0)
      val frame = filterByGroupAnd999(userid ,dataFrame)
      //LRModel(userid, lr.fit(frame, "topicDistribution", "s"))
      //非时间模型
      val model = lr.fit(frame, "topicDistribution", "s")
      //时间模型
      //val model = lr.fit(frame, "topicDistribution", "s", "weight")
      model.write.overwrite().save(SocialTextTest.userModelPath + userid)
      //println("usermodel"+userid)
    })
  }

  def userModelLinearRegressionClusterFunctional(dataFrame: DataFrame, userModelPath:String)={
    val rows = getGroupIdFrame(dataFrame)
    //println("-------------Groups-------------")
    //rows.show(false)
    val lr = new LinearRegressionAl()
    //println("Group number:"+rows.count())
    val result = rows.collect().foreach(row => {
      val userid = row.getInt(0)
      val frame = filterByGroupAnd999(userid ,dataFrame)
      //LRModel(userid, lr.fit(frame, "topicDistribution", "s"))
      //非时间模型
      val model = lr.fit(frame, "topicDistribution", "s")
      //时间模型
      //val model = lr.fit(frame, "topicDistribution", "s", "weight")
      model.write.overwrite().save(userModelPath + userid)
      //println("usermodel"+userid)
    })
  }

  def userModelGBTClusterFunctional(modelType:String, dataFrame: DataFrame, userModelPath:String)={
    val rows = getGroupIdFrame(dataFrame)
    val GBT = new GBTRegressor()
      .setFeaturesCol("topicDistribution")
      .setLabelCol("s")


    //RFR
    val RFT = new RandomForestRegressor()
      .setLabelCol("s")
      .setFeaturesCol("topicDistribution")
      .setNumTrees(100)

    println("group number:"+rows.count())
    if(modelType.equals("GBT")) {
      val result = rows.collect().foreach(row => {
        val userid = row.getInt(0)
        val frame = filterByGroup(userid, dataFrame)
        //LRModel(userid, lr.fit(frame, "topicDistribution", "s"))
        val model = GBT.fit(frame)
        model.write.overwrite().save(userModelPath + userid)
        println("usermodel" + userid)
      })
    }
    else {
      val result = rows.collect().foreach(row => {
        val userid = row.getInt(0)
        val frame = filterByGroup(userid, dataFrame)
        //LRModel(userid, lr.fit(frame, "topicDistribution", "s"))
        val model = RFT.fit(frame)
        model.write.overwrite().save(userModelPath + userid)
        println("usermodel" + userid)
      })
    }
  }

  def userModelGBTClusterFunctional(modelType:String, dataFrame: DataFrame)={
    val rows = getGroupIdFrame(dataFrame)
    val GBT = new GBTRegressor()
      .setFeaturesCol("topicDistribution")
      .setLabelCol("s")


    //RFR
    val RFT = new RandomForestRegressor()
      .setLabelCol("s")
      .setFeaturesCol("topicDistribution")
      .setNumTrees(100)

    println("group number:"+rows.count())
    if(modelType.equals("GBT")) {
      val result = rows.collect().foreach(row => {
        val userid = row.getInt(0)
        val frame = filterByGroup(userid, dataFrame)
        //LRModel(userid, lr.fit(frame, "topicDistribution", "s"))
        val model = GBT.fit(frame)
        model.write.overwrite().save(SocialTextTest.userModelPath + userid)
        println("usermodel" + userid)
      })
    }
    else {
      val result = rows.collect().foreach(row => {
        val userid = row.getInt(0)
        val frame = filterByGroup(userid, dataFrame)
        //LRModel(userid, lr.fit(frame, "topicDistribution", "s"))
        val model = RFT.fit(frame)
        model.write.overwrite().save(SocialTextTest.userModelPath + userid)
        println("usermodel" + userid)
      })
    }
  }

  def userModelLRTestFunctional(dataFrame: DataFrame)={
    dataFrame.select("user_id").dropDuplicates().collect().foreach(row =>{
      dataFrame.filter("user_id == '"+row.getString(0)+"'")
    })
  }


//  def userFeature(dataFrame: DataFrame):Array[Row] = {
//    val rows = getUserId(dataFrame)
//    var arr:Array[Row] = new Array[Row](rows.length)
//    for(i <- 0 to rows.length - 1) {
//      val userid = rows.apply(i).getString(0)
//      val frame = filterByUserId(userid, dataFrame)
////      println("-----------"+i+"-----------")
////      frame.show(false)
//      if(frame.count() >= 2) {
//        val result = frame.reduce((row1, row2) => {
//          //if (row1.size > 3) {
//            val v1 = row1.getAs[DenseVector](0)
//            val v2 = row2.getAs[DenseVector](0)
//            val v3 = new Array[Double](v1.size)
//            for (i <- 0 to v3.length - 1) {
//              v3.update(i, (v1.apply(i) + v2.apply(i))/2.0)
//            }
//            Row(Vectors.dense(v3), userid)
//          //}
////          else {
//////            println(i+":"+row1.getAs[DenseVector](0)+"////"+row1.getString(1))
//////            println(i+":"+row2.getAs[DenseVector](0)+"////"+row2.getString(1))
////            println(i+row1.toString())
////            println(row2.toString())
////            if(row2.size > 3)
////            Row(row2.getAs[DenseVector](0), userid)
////            else Row(Vectors.dense(Array(0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
////              0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0)),userid)
////          }
//        })
//        arr.update(i, result)
//        println("user:"+i)
//      }
//      else {
//        val result = frame.take(1)
//        arr.update(i, Row(result.apply(0).getAs[DenseVector](0), userid))}
//
//    }
//    arr
//  }

//  def itemFeature(dataFrame: DataFrame):Array[Row] = {
//    val rows = getItemId(dataFrame)
//    var arr:Array[Row] = new Array[Row](rows.length)
//    for(i <- 0 to rows.length - 1) {
//      val itemid = rows.apply(i).getString(0)
//      val frame = filterByItemId(itemid, dataFrame)
//      //      println("-----------"+i+"-----------")
//      //      frame.show(false)
//      if(frame.count() > 2) {
//        val result = frame.reduce((row1, row2) => {
//          //if (row1.size > 3) {
//            val v1 = row1.getAs[DenseVector](0)
//            val v2 = row2.getAs[DenseVector](0)
//            val v3 = new Array[Double](v1.size)
//            for (i <- 0 to v3.length - 1) {
//              v3.update(i, (v1.apply(i) + v2.apply(i))/2.0)
//            }
//            Row(Vectors.dense(v3), itemid)
//          //}
////          else {
//////            println(i+":"+row1.getAs[DenseVector](0)+"////"+row1.getString(1))
//////            println(i+":"+row2.getAs[DenseVector](0)+"////"+row2.getString(1))
////            if(row2.size > 3)
////              Row(row2.getAs[DenseVector](0), itemid)
////            else Row(Vectors.dense(Array(0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
////              0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0)),itemid)
////          }
//        })
//        arr.update(i, result)
//        println("item:"+i)
//      }
//      else {
//        val result = frame.take(1)
//        arr.update(i, Row(result.apply(0).getAs[DenseVector](0), itemid))}
//
//    }
//    arr
//  }

  case class Feature1(business_id: String, user_id: String, topic: DenseVector,
                     star: Int, user_feature:DenseVector, business_feature:DenseVector)
  case class Feature2(business_id: String, user_id: String,
                     star: Int, user_feature:DenseVector, business_feature:DenseVector, feature: DenseVector)
  def userItemPrediction(userModelPath:String, test:DataFrame, userFeature:DataFrame, itemFeature:DataFrame):Array[Row]={
    val total = test.join(userFeature, "user_id").join(itemFeature, "business_id")
    total.show()
    val data = total.map(row =>
      Feature1(row.getString(0), row.getString(1), row.getAs[DenseVector](2), row.getInt(3),
        row.getAs[DenseVector](4), row.getAs[DenseVector](5)))
      .map(row =>{
      val userid = row.user_id
      val itemid = row.business_id
      val star = row.star
        val topic = row.topic
      val usfeature = row.user_feature
      val bufeature = row.business_feature
      val arr = new Array[Double](usfeature.size)
      var total = 0.0
      for(i <- 0 to usfeature.size - 1){
        val k = usfeature.apply(i)*bufeature.apply(i)
        total= total + k
        arr.update(i, k)
      }
      for(i <- 0 to usfeature.size - 1){
        arr.update(i, arr.apply(i)/total)
      }
      //Row(userid, itemid, usfeature, bufeature, arr, star)
        Feature2(itemid, userid, star, usfeature, bufeature, Vectors.dense(arr).toDense)
    }).toDF("business_id", "user_id", "star", "usfeature", "bufeature", "feature")

    //聚类后需要修改，获得类别
    val rows = getUserId(data)
    val frames:Array[DataFrame] = new Array[DataFrame](rows.length)
    for(i <- 0 to rows.length - 1) {
      val userid = rows.apply(i).getString(0)
      //聚类后需要修改，获得相同类别的frame
      val frame = filterByUserId(userid, data)
      val userModel = LinearRegressionModel.load(userModelPath+userid)
      if(userModel!=null) {
        println(i+": predicte "+userid)
        userModel.setFeaturesCol("feature")
        val prediction = userModel.transform(frame)
        frames.update(i, prediction)
      }
    }
//    var frame1 = frames.apply(0)
//    for(i <- 1 to frames.size - 1)
//      frame1 = frame1.union(frames.apply(i))
//    frame1

    var frameArr = new Array[Row](total.count().toInt)
    var n = 0
    for(i <- 0 to frames.size - 1){
      val frame = frames.apply(i)
      val size = frame.count().toInt
      val rows = frame.take(size)
      for(j <- 0 to size - 1 ){
        frameArr.update(n, rows.apply(j))
        n+=1
      }
      println("Frame " + i)
    }
    frameArr
  }

  case class Feature5(business_id: String, user_id: String,
                      star: Int, user_feature:DenseVector, business_feature:DenseVector, group:Int)
  case class Feature6(business_id: String, user_id: String,
                      star: Int, user_feature:DenseVector, business_feature:DenseVector, feature: DenseVector, group:Int)
  def userItemClusterPrediction(modelType:String, userModelPath1:String, userModelPath2:String, test:DataFrame, userFeature:DataFrame, itemFeature:DataFrame):Array[Row]={
    val total = test.join(userFeature, "user_id").join(itemFeature, "business_id")
      .select("business_id", "user_id", "s", "user_feature", "business_feature", "class")
    total.show()
    val data = total
      .map(row =>
//      Feature5(row.getString(0), row.getString(1), row.getAs[DenseVector](2), row.getInt(3),
//        row.getAs[DenseVector](4), row.getAs[DenseVector](5), row.getInt(6)))
        //数字id
        Feature5(row.getInt(0).toString, row.getInt(1).toString, row.getInt(2),
          row.getAs[DenseVector](3), row.getAs[DenseVector](4), row.getInt(5)))
      .map(row =>{
        val userid = row.user_id
        val itemid = row.business_id
        val star = row.star
        //val topic = row.topic
        val usfeature = row.user_feature
        val bufeature = row.business_feature
        val group = row.group
        val arr = new Array[Double](usfeature.size)
        var total = 0.0
        for(i <- 0 to usfeature.size - 1){
          val k = usfeature.apply(i)*bufeature.apply(i)
          total= total + k
          arr.update(i, k)
        }
        for(i <- 0 to usfeature.size - 1){
          arr.update(i, arr.apply(i)/total)
        }
        //Row(userid, itemid, usfeature, bufeature, arr, star)
        Feature6(itemid, userid, star, usfeature, bufeature, Vectors.dense(arr).toDense, group)
      }).toDF("business_id", "user_id", "star", "usfeature", "bufeature", "feature", "class")
      .cache()
    data.show()

    //聚类后需要修改，获得类别
//    val rows = getUserIdAndGroup(data)
//    val frames:Array[DataFrame] = new Array[DataFrame](rows.length)
//    for(i <- 0 to rows.length - 1) {
//      val userid = rows.apply(i).getString(0)
//      val group = rows.apply(i).getInt(1)
//      //聚类后需要修改，获得相同类别的frame
//      val frame = filterByUserId(userid, data)
//      val userModel = LinearRegressionModel.load(userModelPath+group)
//      if(userModel!=null) {
//        println(i+": predicte "+userid)
//        userModel.setFeaturesCol("feature")
//        val prediction = userModel.transform(frame)
//        frames.update(i, prediction)
//      }
//    }

    val rows = getUserGroup(data)
    val num  = total.count().toInt
    var sum = 0
    val frames:Array[Row] = new Array[Row](num)
    for(i <- 0 to rows.length - 1) {
      //val userid = rows.apply(i).getString(0)
      val group = rows.apply(i).getInt(0)
      //聚类后需要修改，获得相同类别的frame
      val frame = filterByGroup(group, data)


      if(modelType.equals("RFT")) {
        val RFTModel = RandomForestRegressionModel.load(userModelPath1+group)
        println(i+": predicte "+group)
        val prediction = RFTModel.transform(frame
          .toDF("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution", "class"))
          .select("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution","class", "prediction")

        prediction.collect().foreach(row => {
                  frames.update(sum, row)
                  sum+=1
                })
      }
      else if(modelType.equals("GBT")){
        val GBTModel = GBTRegressionModel.load(userModelPath1 + group)
        println(i+": predicte "+group)
        val prediction = GBTModel.transform(frame
          .toDF("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution", "class"))
          .select("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution","class", "prediction")

        prediction.collect().foreach(row => {
          frames.update(sum, row)
          sum+=1
        })
      }
      else  {
        var LRModel = LinearRegressionModel.load(userModelPath1+group)
        println(i+": predicte "+group)
        //出现大量用户聚集在少数社区启用此项
//        if(frame.select("user_id").dropDuplicates().count()<3) {
//          LRModel = LinearRegressionModel.load(userModelPath2 + 1)
//          println(i+": change to "+1)
//        }

        val prediction = LRModel.transform(frame
          .toDF("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution", "class"))
          .select("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution","class", "prediction")

        prediction.collect().foreach(row => {
          frames.update(sum, row)
          sum+=1
        })
      }
      //println("row number"+frames.count())
    }
    frames

  }

  def userItemClusterVectorPrediction(modelType:String, userModelPath:String, test:DataFrame, userFeature:DataFrame, itemFeature:DataFrame):Array[Row]={
    val total = test.join(userFeature, "user_id").join(itemFeature, "business_id")
      .select("business_id", "user_id", "s", "user_feature", "business_feature", "class")
    //total.show(false)
    val data = total
      .map(row =>
        //      Feature5(row.getString(0), row.getString(1), row.getAs[DenseVector](2), row.getInt(3),
        //        row.getAs[DenseVector](4), row.getAs[DenseVector](5), row.getInt(6)))
        //数字id
        Feature5(row.getInt(0).toString, row.getInt(1).toString, row.getInt(2),
          row.getAs[DenseVector](3), row.getAs[DenseVector](4), row.getInt(5)))
      .map(row =>{
        val userid = row.user_id
        val itemid = row.business_id
        val star = row.star
        //val topic = row.topic
        val usfeature = row.user_feature
        val bufeature = row.business_feature
        val group = row.group
        val arr = new Array[Double](usfeature.size)
        var total = 0.0
        for(i <- 0 to usfeature.size - 1){
          //向量中有负数此处用加
          val x = usfeature.apply(i)
          val y = bufeature.apply(i)
          var k = 0.0

//          if(x>0 && y>0){
//            k = x*y
//            //total = total + k
//          }
//          else if(x>0 && y<=0){
//            k = x+y
//            //total = total + (x-y)
//          }
//          else if(x<0 && y>0){
//            k = x+y
//            //total = total + (y-x)
//          }
//          else {
//            k = -x * y
//            //total = total + -k
//          }



          k = x+y
//          if(k>0)
//            total += k
//          else total += -k

          arr.update(i, k)
        }
        //不进行归一化处理?
        for(i <- 0 to usfeature.size - 1){
          //if(!total.equals(0.0))
          arr.update(i, arr.apply(i)/2.0)
        }
        //Row(userid, itemid, usfeature, bufeature, arr, star)
        Feature6(itemid, userid, star, usfeature, bufeature, Vectors.dense(arr).toDense, group)
      }).toDF("business_id", "user_id", "star", "usfeature", "bufeature", "feature", "class")
      .cache()
    //data.show(false)

    //聚类后需要修改，获得类别
    //    val rows = getUserIdAndGroup(data)
    //    val frames:Array[DataFrame] = new Array[DataFrame](rows.length)
    //    for(i <- 0 to rows.length - 1) {
    //      val userid = rows.apply(i).getString(0)
    //      val group = rows.apply(i).getInt(1)
    //      //聚类后需要修改，获得相同类别的frame
    //      val frame = filterByUserId(userid, data)
    //      val userModel = LinearRegressionModel.load(userModelPath+group)
    //      if(userModel!=null) {
    //        println(i+": predicte "+userid)
    //        userModel.setFeaturesCol("feature")
    //        val prediction = userModel.transform(frame)
    //        frames.update(i, prediction)
    //      }
    //    }

    val rows = getUserGroup(data)
    val num  = total.count().toInt
    var sum = 0
    val frames:Array[Row] = new Array[Row](num)
    for(i <- 0 to rows.length - 1) {
      //val userid = rows.apply(i).getString(0)
      val group = rows.apply(i).getInt(0)
      //聚类后需要修改，获得相同类别的frame
      val frame = filterByGroup(group, data)

      if(modelType.equals("RFT")) {
        val RFTModel = RandomForestRegressionModel.load(userModelPath+group)
        println(i+": predicte "+group)
        val prediction = RFTModel.transform(frame
          .toDF("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution", "class"))
          .select("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution","class", "prediction")

        prediction.collect().foreach(row => {
          frames.update(sum, row)
          sum+=1
        })
      }
      else if(modelType.equals("GBT")){
        val GBTModel = GBTRegressionModel.load(userModelPath+ group)
        println(i+": predicte "+group)
        val prediction = GBTModel.transform(frame
          .toDF("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution", "class"))
          .select("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution","class", "prediction")

        prediction.collect().foreach(row => {
          frames.update(sum, row)
          sum+=1
        })
      }
      else  {
        var LRModel = LinearRegressionModel.load(userModelPath+group)
        //println(i+": predicte "+group)
        //出现大量用户聚集在少数社区启用此项
        //        if(frame.select("user_id").dropDuplicates().count()<3) {
        //          LRModel = LinearRegressionModel.load(userModelPath2 + 1)
        //          println(i+": change to "+1)
        //        }

        val prediction = LRModel.transform(frame
          .toDF("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution", "class"))
          .select("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution","class", "prediction")

        prediction.collect().foreach(row => {
          frames.update(sum, row)
          sum+=1
        })
      }

      //println("row number"+frames.count())
    }
    frames

  }

  /**
    * 社交混合预测，混合整体模型和社交模型的预测分值
    *
    * @param per
    * @param userModelPath1
    * @param userModelPath2
    * @param test
    * @param userFeature
    * @param itemFeature
    * @return
    */

  def userItemClusterPrediction(per:Double, userModelPath1:String, userModelPath2:String,test:DataFrame, userFeature:DataFrame, itemFeature:DataFrame):Array[Row]={
    val total = test.join(userFeature, "user_id").join(itemFeature, "business_id")
      .select("business_id", "user_id", "s", "user_feature", "business_feature", "class")
    total.show()
    val data = total
      .map(row =>
        //      Feature5(row.getString(0), row.getString(1), row.getAs[DenseVector](2), row.getInt(3),
        //        row.getAs[DenseVector](4), row.getAs[DenseVector](5), row.getInt(6)))
        //数字id
        Feature5(row.getInt(0).toString, row.getInt(1).toString, row.getInt(2),
          row.getAs[DenseVector](3), row.getAs[DenseVector](4), row.getInt(5)))
      .map(row =>{
        val userid = row.user_id
        val itemid = row.business_id
        val star = row.star
        //val topic = row.topic
        val usfeature = row.user_feature
        val bufeature = row.business_feature
        val group = row.group
        val arr = new Array[Double](usfeature.size)
        var total = 0.0
        for(i <- 0 to usfeature.size - 1){
          val k = usfeature.apply(i)*bufeature.apply(i)
          total= total + k
          arr.update(i, k)
        }
        for(i <- 0 to usfeature.size - 1){
          arr.update(i, arr.apply(i)/total)
        }
        //Row(userid, itemid, usfeature, bufeature, arr, star)
        Feature6(itemid, userid, star, usfeature, bufeature, Vectors.dense(arr).toDense, group)
      }).toDF("business_id", "user_id", "star", "usfeature", "bufeature", "feature", "class")
      .cache()
    data.show()

    val rows = getUserGroup(data)
    val num  = total.count().toInt
    var sum = 0
    val frames:Array[Row] = new Array[Row](num)
    for(i <- 0 to rows.length - 1) {
      //val userid = rows.apply(i).getString(0)
      val group = rows.apply(i).getInt(0)
      //聚类后需要修改，获得相同类别的frame
      val frame = filterByGroup(group, data)

        val LRModelSocial = LinearRegressionModel.load(userModelPath1+group)
        val LRModelCluster = LinearRegressionModel.load(userModelPath2+1)
        println(i+": predicte "+group)
        val prediction1 = LRModelSocial.transform(frame
          .toDF("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution", "class"))
          .select("business_id", "user_id", "star","prediction")
        .toDF("business_id", "user_id", "star","prediction1").dropDuplicates()
      //prediction1.show()
      //println(prediction1.count())

      val prediction2 = LRModelCluster.transform(frame
        .toDF("business_id", "user_id", "star", "usfeature", "bufeature", "topicDistribution", "class"))
        .select("business_id", "user_id", "star","prediction")
        .toDF("business_id", "user_id", "star","prediction2").dropDuplicates()

      //prediction2.show()
      //println(prediction2.count())

      implicit val encoder = org.apache.spark.sql.Encoders.kryo[Row]
      val prediction = prediction1.join(prediction2,Array("business_id", "user_id", "star"))
          .map(row =>{
            val user_id = row.getString(1)
            val business_id = row.getString(0)
            val star = row.getInt(2)
            val prediction1 = row.getDouble(3)
            val prediction2 = row.getDouble(4)
            val prediction = prediction1*per + prediction2*(1-per)
            Row(user_id, business_id, star, prediction)
          })

      //println(prediction.count())

        prediction.collect().foreach(row => {
          frames.update(sum, row)
          sum+=1
          //println(frames.apply(sum-1).toString())
        })

      //println("Row number:"+frames.size)
    }
    //println(frames.apply(0).toString())
    frames

  }

  case class Evaluate(star:Double, prediction:Double)
  def filterPrediction(star:String, prediction:String, dataFrame: DataFrame):DataFrame = {
    val data = dataFrame.select(star, prediction)
      .map(row => {
        val star = row.getInt(0)
        var prediction = row.getDouble(1)
        if(prediction < 0.0)
          prediction = 1.0
        else if(prediction > 5.0)
          prediction = 5.0
        else if(prediction.equals(Double.NaN)) {
          prediction = 3.7707160637796866
          println("Evaluate NaN")
        }
        Evaluate(star, prediction)
      })
    data.toDF()
  }

  case class Line(c1:String)
  def userLinearRegression1(dataFrame: DataFrame):DataFrame = {

    val rows = getUserId1(dataFrame)
    rows.map(row => {
      //val line = Line(row.getString(0))
      val id = row.getString(0)
      println(id)
      if (id != null) {
      val frame = filterByUserId(id, dataFrame)
      val model = new LinearRegressionAl().fit(frame, "topicDistribution", "s")
      model.coefficients.toString + "," + model.intercept
    }
      else ""
    }).toDF()

  }


}

package scala.datafilter.test

import java.sql.Date

import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.algorithm._
import scala.datafilter.{WriteFile, GetRandomData, DataAnalysis}

/**
  * Created by pi on 7/3/17.
  */
class DatatoolTest {

}

object DatatoolTest{
  def main(args: Array[String]) {
    println("----------DatatoolTest----------")
    YelpTextRegression2.main(args)
  }
}
/**
  * 随机抽取给定百分比的数据
  */
object GetRandomDataTest{
  def main (args: Array[String] ) {
    val randomdata = new GetRandomData("/home/pi/doc/dataset/")
    randomdata.outputYelpPercentData("textdata/yelp_academic_dataset_review.json",
      "outputdata/yelp_randomdata3",0.005)
  }
}

/**
  * 分析Gowalla数据的稀疏度
  */
object DataAnalysisGowallaTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val result = analysis.analyseSparsity(analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
      "_c0","_c4","_c2","_c3","csv",1))
    println("DataAnalysisGowallaTest"+result)
  }
}

/**
  * 分析yelp数据集的稀疏度
  */
object DataAnalysisYelpTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val result = analysis.analyseSparsity(analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1))
    println("DataAnalysisYelpTest"+result)
  }
}

/**
  * 输出yelp数据集的用户商家和评分
  */
object DataAnalysisYelpOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val result = analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1)
    result.show()
    //analysis.outputResult(result, 1, "outputdata/YelpUserItemRatingAll")
  }
}

object DataAnalysisYelpTrustOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val result = analysis.userandFriendTrustAnalysis("textdata/yelp_academic_dataset_user.json",
      "user_id","friends", 1)
    result.show()
    analysis.outputResult(result, 1, "outputdata/DataAnalysisYelpUserItemTrust")
  }
}
/**
  * 按照用户的checkin数目过滤
  */
object DataAnalysisTest1{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_1",">",10).toDF("userid","count")
    val data = analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    val output = data.join(filter,"userid").toDF("_1","_2","_3","_4")
    val result = analysis.analyseSparsity(output)
    println("DataAnalysisTest1"+result)
  }

}

/**
  * 按照用户checkin数目过滤后输出
  */
object DataFilterYelpOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_1",">",10).toDF("userid","count")
    val data = analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    val output = data.join(filter,"userid").toDF("_1","_2","_3","_4")
    analysis.outputResult(output, 1, "outputdata/YelpUserCheckinMorethan10")
  }
}

/**
  * 按照用户和商家checkin数目过滤后分析输出
  */
//object DataFilterYelpUserItemOutput{
//  def main(args: Array[String]) {
//    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
//    val filter = analysis.userItemRateFilterAnalysis(
//      analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
//        "user_id","business_id","stars","json",1),"_1","_2",">",10).toDF("userid","itemid","count")
//    val data = analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
//      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
//    val output = data.join(filter,Seq("userid","itemid")).toDF("_1","_2","_3","_4")
//    val result = analysis.analyseSparsity(output)
//    println("DataFilterYelpUserItemOutput"+result)
//    analysis.outputResult(output, 1, "outputdata/YelpUserItemCheckinMorethan10")
//  }
//}

/**
  * 按照用户和商家checkin数目过滤后分析输出
  */
object DataFilterYelpUserandItemOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_1",">",10).toDF("userid","count")
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_2",">",10).toDF("itemid","count")
    val data = analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    val output = data.join(filter1,"userid").join(filter2,"itemid")toDF("_1","_2","_3","_4","_5")
    val result = analysis.analyseSparsity(output)
    println("DataFilterYelpUserandItemOutput"+result)
    analysis.outputResult(output, 1, "outputdata/YelpUserandItemCheckinMorethan10")
  }
}

/**
  * 按照用户的checkin数目过滤
  */
object DataAnalysisGowallaFilter{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("Gowalla/Gowalla_totalCheckins.txt",
        "_c0","_c4","_c2","csv",1),"_1",">",10).toDF("userid","count")
    val data = analysis.userItemRateAnalysis("Gowalla/Gowalla_totalCheckins.txt",
      "_c0","_c4","_c2","csv",1).toDF("userid","itemid","starts")
    val output = data.join(filter,"userid").toDF("_1","_2","_3","_4")
    val result = analysis.analyseSparsity(output)
    println("GowallaFilterTest"+result)
  }
}
/**
  * 按照地点的checkin数目过滤
  */
object DataAnalysisGowallaItemFilter{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("Gowalla/Gowalla_totalCheckins.txt",
        "_c0","_c4","_c2","csv",1),"_2",">",10).toDF("itemid","count")
    val data = analysis.userItemRateAnalysis("Gowalla/Gowalla_totalCheckins.txt",
      "_c0","_c4","_c2","csv",1).toDF("userid","itemid","starts")
    val output = data.join(filter,"itemid").toDF("_1","_2","_3","_4")
    val result = analysis.analyseSparsity(output)
    println("DataAnalysisGowallaItemFilter"+result)
  }
}
/**
  * 按照用户checkin数目过滤后输出
  */
object DataFilterGowallaOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
        "_c0","_c4","_c2","_c3","csv",1),"_1",">",10).toDF("userid","count")
    val data = analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
      "_c0","_c4","_c2","_c3","csv",1).toDF("userid","itemid","la","lon")
    val output = data.join(filter,"userid").toDF("_1","_2","_3","_4","_5")
    analysis.outputResult(output,1,"outputdata/GowallaUserCheckinMoreThan10")
  }
}


object DataFilterGowallaUserandItemOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
        "_c0","_c4","_c2","_c3","csv",1),"_1",">",10).toDF("userid","count")
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
        "_c0","_c4","_c2","_c3","csv",1),"_2",">",10).toDF("itemid","count")
    val data = analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
      "_c0","_c4","_c2","_c3","csv",1).toDF("userid","itemid","la","lon")
    val output1 = data.join(filter1,"userid")
    val output2 = filter2.join(output1,"itemid").toDF("_1","_2","_3","_4","_5","_6")
    val result = analysis.analyseSparsity(output2)
    println("DataFilterGowallaUserandItemOutput"+result)
    analysis.outputResult(output2, 1, "outputdata/DataFilterGowallaUserandItemMorethan10")
  }
}
/**
  * 按照地点checkin数目过滤后输出
  */
object DataFilterGowallaItemOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
        "_c0","_c4","_c2","_c3","csv",1),"_2",">",10).toDF("itemid","count")
    val data = analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
      "_c0","_c4","_c2","_c3","csv",1).toDF("userid","itemid","la","lon")
    val output = data.join(filter,"itemid").toDF("_1","_2","_3","_4","_5")
    analysis.outputResult(output,1,"outputdata/GowallaItemCheckinMoreThan10")
  }
}

/**----------------------------------------------------------------------------------
  * 使用测试数据
  */
object YelpUserItemRatingTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val result = analysis.userItemRateAnalysis("input/useritemrating.json",
      "user_id","business_id","stars","json",1)
    result.show()
    analysis.outputResult(result, 1, "output/YelpUserItemRatingAll")
  }
}

object YelpUserFriendTrustTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val result = analysis.userandFriendTrustAnalysis("input/userfriends.json",
      "user_id","friends", 1)
    result.show()
    analysis.outputResult(result, 1, "output/DataAnalysisYelpUserItemTrust")
  }

}

object YelpOneFilterTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_2",">",3).toDF("itemid","count")
    filter.show()
    val data = analysis.userItemRateAnalysis("input/useritemrating.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    data.show()
    //inner join
    val output = data.join(filter,"itemid").toDF("_1","_2","_3","_4")
    output.show()
    analysis.outputResult(output, 1, "output/YelpUserCheckinMorethan10")
  }
}

object YelpTwoFilterTest1{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_1",">",3).toDF("userid","count")
    filter1.show()
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_2",">",1).toDF("itemid","count")
    filter2.show()
    val data = analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    data.show()
    //inner join
    val output1 = data.join(filter1,"userid").join(filter2,"itemid").toDF("_1","_2","_3","_4","_5")
    output1.show()
    val output2 = analysis.transformId(output1,"_2","_1","_3")
    output2.show()
    val output3 = analysis.getAvg(output2,"_1","_2","_3")
    output3.show()
    val result = analysis.analyseSparsity(output2)
    println("DataFilterYelpUserandItemOutput"+result)
    analysis.outputResult(output2, 1, "output/1YelpUserandItemCheckinMorethan10")
  }
}

object YelpTwoFilterTest2{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_1",">",3).toDF("userid","count")
    filter1.show()
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_2",">",1).toDF("itemid","count")
    filter2.show()
    val data = analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    data.show()
    val output1 = data.join(filter1,"userid")
    val output2 = filter2.join(output1,"itemid").toDF("_1","_2","_3","_4","_5")
    output2.show()
    val output3 = analysis.transformId(output2,"_3","_1","_4")
    output3.show()
    val result = analysis.analyseSparsity(output3)
    println("DataFilterYelpUserandItemOutput"+result)
    analysis.outputResult(output3, 1, "output/2YelpUserandItemCheckinMorethan10")
  }
}

object YelpTwoFilterRatingandTrustOutputTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_1",">",1).toDF("userid","count")
    filter1.show()
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_2",">",1).toDF("itemid","count")
    filter2.show()
    val data = analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    data.show()
    val output1 = data.join(filter1,"userid").join(filter2,"itemid").toDF("_1","_2","_3","_4","_5")
    output1.show()
    val friends = analysis.userandFriendTrustAnalysis("input/userfriends.json",
      "user_id","friends", 1)
    friends.show()
    //    val indexrating = analysis.getAvg(analysis.transformId(output1,"_2","_1","_3")
    //      ,"_1","_2","_3")
    val dropre = analysis.getAvg(output1.select("_2","_1","_3").toDF("_1","_2","_3"),"_1","_2","_3")
    dropre.show()
    val indexrating = analysis.transformId(dropre,"_1","_2","_3")
    val users = dropre.select("_1").groupBy("_1").count()
    users.show()
    val filteruser = friends.join(users.select("_1"),"_1")
      .join(users.select("_1").toDF("_2"),"_2")
    filteruser.show()
    val indextrust = analysis.transformIdUsingIndexer(output1.select("_2").toDF("_1"), "_1", filteruser)
    indexrating.show()
    indextrust.show()
    //analysis.outputResult(indexrating, 1, "output/YelpTwoFilterRatingandTrustOutputTest1")
    //analysis.outputResult(indextrust, 1, "output/YelpTwoFilterRatingandTrustOutputTest2")
  }
}

object YelpRatingandTrustOutputTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val rating = analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
      "user_id","business_id","stars","json",1)
    rating.show()
    val friends = analysis.userandFriendTrustAnalysis("input/userfriends.json",
      "user_id","friends", 1)
    friends.show()
    val indexrating = analysis.getAvg(analysis.transformId(rating,"_1","_2","_3")
      ,"_1","_2","_3")
    val indextrust = analysis.transformIdUsingIndexer(rating, "_1", friends)
    indexrating.show()
    indextrust.show()
    //analysis.outputResult(indexrating, 1, "output/DataAnalysisYelpUserItemTrust1")
    //analysis.outputResult(indextrust, 1, "output/DataAnalysisYelpUserItemTrust2")
  }
}

object YelpLalonTest {
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val rating = analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
      "business_id", "user_id", "stars", "json", 1)
    val lalon = analysis.itemLaLonAnalysisNotrans("input/buslalo.json",
      "business_id", "latitude", "longitude", "json", 1)
    val userlalon = rating.join(lalon, "_1").toDF("itemid", "userid", "rating", "la", "lon")
      .select("userid", "la", "lon")
    userlalon.show()
    val useravg = userlalon.groupBy("userid").avg()
    useravg.show()

    val friends = analysis.userandFriendTrustAnalysis("input/userfriends.json",
      "user_id", "friends", 1)
    friends.show()
    val user1lalon = useravg.toDF("_1", "la1", "lon1").join(friends, "_1")
    user1lalon.show()
    val user2lalon = user1lalon.join(useravg.toDF("_2", "la2", "lon2"), "_2")
    user2lalon.show()
    val result = user2lalon.select("_1", "_2", "la1", "lon1", "la2", "lon2")
    result.show()

    //    val ss = SparkSession.builder().appName("Yelp Rating")
    //      .master("local[*]").getOrCreate()
    //    import ss.implicits._
    //result.withColumn("x",pow($"la1"-$"la2")).show()
    import org.apache.spark.sql.functions.lit
    val avgrating = analysis.getAvg(rating,"_1","_2","_3").toDF("_2","_1","_3")
    avgrating.show()
    val loresult = result.withColumn("_3", sqrt(pow((result.col("la1") - result.col("la2")), 2)+
      pow((result.col("lon1") - result.col("lon2")),2))).select("_1","_2","_3")
    loresult.show()
    val indexer = analysis.getTransformIndexer(avgrating, "_1")
    val indexedresult = analysis.transformIdUsingIndexer(indexer, loresult)
    indexedresult.show()
    //    val calresult1 = indexedresult.withColumn("_4", lit(1))
    //    calresult1.show()
    val calresult = indexedresult.withColumn("_4", round(pow(exp(indexedresult.col("_3")*10)+1,-1)*2*10, 3)).select("_1","_2","_4")
    calresult.show(false)
    //将userid和itemid重复的记录计算平均分

    val indexrating = analysis.transformId(avgrating, "_1","_2","_3")
    indexrating.show()
    //analysis.outputResult(result, 1, "output/DataAnalysisYelpUserItemLocTrust10")
  }
}

object LDAPersonalLinearResgressionTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val dataset1 = analysis.getData("input/personal_lda_data","csv").toDF("user_id","business_id","stars","text")

    val dataset = dataset1.withColumn( "s", dataset1.col("stars").cast(IntegerType))

    val vector = analysis.transTextToVector(dataset,"text")
    vector.show(false)
    val lda = new LDAText().run(vector,"vector",5,10,3).select("topicDistribution","user_id","business_id","s")
    //    analysis.outputResult(lda,"parquet", 1,"output/lda")
    //    val result = new LinearRegressionAl().run(lda,"topicDistribution","s")
    analysis.regression(lda,"user",1)
  }
}

object LDALinearResgressionTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val dataset1 = analysis.getData("input/personal_lda_data","csv").toDF("user_id","stars","text")
    val dataset2 = analysis.getData("input/lda_data.txt","csv").toDF("stars","text")
    dataset2.show()
    val dataset = dataset2.withColumn( "s", dataset2.col("stars").cast(IntegerType))
    //    val result = new LinearRegressionAl().run(dataset1,"features","label")
    val vector = analysis.transTextToVector(dataset,"text")
    vector.show(false)
    val lda = new LDAText().run(vector,"vector",5,10,3)
    analysis.outputResult(lda,"parquet", 1,"output/lda")
    val result = new LinearRegressionAl().run(lda,"topicDistribution","s")
  }
}

object LinearRegressionTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val dataset1 = analysis.getData("output/lda/part-00000-6966d8fe-ac12-487b-a14f-7502280a18c5.snappy.parquet","parquet")
    dataset1.show(false)
    val result = new LinearRegressionAl().run(dataset1,"topicDistribution","s")
  }
}

object WriteLog{
  def main(args: Array[String]) {
    val s = "test write file2"
    new WriteFile().write("./src/data/output","testlog",s+"\n"+"!!!")
  }
}

object SocialCluster{
  def main(args: Array[String]) {

    new SocialCluster().run()
  }
}

//-------------------------------------------------------------------------------------------

/**
  * 在user评论>10的基础上过滤item评论>10
  */
object YelpTextRegression11{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")

    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_1",">",10).toDF("user_id","count")
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_2",">",10).toDF("business_id","count")
    val data = analysis.userItemRateTextAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","text","json",1).toDF("user_id","business_id","stars","text")
    val output1 = data.join(filter1,"user_id").join(filter2,"business_id").select("user_id","business_id","stars","text")
//    analysis.outputResult(filter1.select("user_id"),"parquet", 1, "output/YelpTextMorethan10User")
//    analysis.outputResult(filter2.select("business_id"),"parquet", 1, "output/YelpTextMorethan10Item")
    //analysis.outputResult(output1,"parquet", 1, "output/YelpTextMorethan10Join")
    val result = analysis.analyseSparsity(output1.toDF("_1","_2","_3","_4"))
    println("YelpTextRegression1"+result)
    //new WriteFile().write("./src/data/output/","YelpTextRegression11",result)
  }
}

/**
  * 从原始数据上取出user评论>10和item>10的两个部分，做union并去重
  */
object YelpTextRegression12{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")

    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_1",">",10).toDF("user_id","count")
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_2",">",10).toDF("business_id","count")
    val data = analysis.userItemRateTextAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","text","json",1).toDF("user_id","business_id","stars","text")
    val output1 = data.join(filter1,"user_id").select("user_id","business_id","stars","text")
    val output2 = data.join(filter2,"business_id").select("user_id","business_id","stars","text")
    val output3 = output1.union(output2).dropDuplicates()
//    analysis.outputResult(filter1.select("user_id"),"parquet", 1, "output/YelpTextMorethan10User")
//    analysis.outputResult(filter2.select("business_id"),"parquet", 1, "output/YelpTextMorethan10Item")
    //analysis.outputResult(output3,"parquet", 1, "output/YelpTextMorethan10Union")
    val result = analysis.analyseSparsity(output3.toDF("_1","_2","_3","_4"))
    println("YelpTextRegression1"+result)
    //new WriteFile().write("./src/data/output/","YelpTextRegression12",result)
  }
}

/**
  * 仅按user的评论数过滤
  */
object YelpTextUserFilter{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_1",">",20).toDF("user_id","count")
    println(filter1.count())
    println(filter1.select("user_id").distinct().count())
    val data = analysis.userItemRateTextAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","text","json",1).toDF("user_id","business_id","stars","text")
    val output1 = data.join(filter1,"user_id").select("user_id","business_id","stars","text")
    println(output1.count())
    //analysis.outputResult(output1,"parquet", 1, "output/YelpTextUserReviewMorethan20")
  }
}

/**
  * 中文论文
  * 仅按user的评论数过滤,并划分训练集和测试集
  * 保证User-Item对在训练集和测试集不重复
  * 取过滤后用户的社交网络
  */
object YelpTextUserFilterNoDulplicate{
  val yelp2016_review = "textdata/yelp_academic_dataset_review.json"
  val yelp2016_user = "textdata/yelp_academic_dataset_user.json"
  val yelp2017_review = "yelp2018/review.json"
  val yelp2017_user = "yelp2018/user.json"
  val yelptest_review = "yelptest/review.json"
  val yelptest_user = "yelptest/user.json"
  val SplitPer = 0.8
  val OutputPath = "output/EnglishPaper/Yelp2016UserBusinessStarReview10"
  val OutputTraining = "output/EnglishPaper/Yelp2016UserBusinessStarReview10Training"+SplitPer
  val OutputTesting = "output/EnglishPaper/Yelp2016UserBusinessStarReview10Testing"+(1-SplitPer)
  val OutputSocial = "output/EnglishPaper/Yelp2016UserBusinessStarReview10SocialSkip"
  val MoreOrLess = ">"
  val ReviewNum = 10
  def main(args: Array[String]) {
    println("----------------"+OutputPath+"-------------------")
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans(yelp2016_review,
        "user_id","business_id","stars","json",1),"_1",MoreOrLess,ReviewNum).toDF("user_id","count")
    val reviewData = analysis.userItemRateTextAnalysisNotrans(yelp2016_review,
      "user_id","business_id","stars","text","json",1).toDF("user_id","business_id","stars","text")
    val userData = analysis.userItemRateTextAnalysisNotrans(yelp2016_user,
      "user_id","yelping_since","review_count","friends","json",1).toDF("user_id","yelping_since","review_count","friends")
    val filteredUser = userData.join(filter1,"user_id").select("user_id","yelping_since","review_count","friends")
    val allUser = userData.select("user_id","yelping_since","review_count","friends")
    //获得过滤后用户的社交网络
    val friends1 = analysis.getdata.getYelpUserFriendsTrustData(filteredUser,
      "user_id","friends").select("_1", "_2").toDF("user_id","friends")
    //获得所有用户的社交网络
    val friends2 = analysis.getdata.getYelpUserFriendsTrustData(allUser,
      "user_id","friends").select("_1", "_2").toDF("user_id","friends")
    //filteredUser(error) or filter1(error)
    val socialNum = analysis.getdata.transformSocialNetwork(filter1, friends2, "user_id", "friends")
      //去除重复的好友关系
      .dropDuplicates()
    socialNum.show(1000, false)
    analysis.outputResult(socialNum,"csv", 1, OutputSocial)
    println("Friends Number:" +friends2.count()+"=?"+socialNum.count())
    //获得过滤的评论
    val filteredReview = reviewData.join(filter1,"user_id").select("user_id","business_id","stars","text")
    val numericReview = analysis.transformId(filteredReview, "user_id","business_id","stars","text")
      .toDF("user_id","business_id","stars","text")
    analysis.outputResult(numericReview,"parquet", 1, OutputPath)
    val result = analysis.analyseSparsity(numericReview.toDF("_1","_2","_3","_4"))
    println("Yelp2016UserBusinessStarReview10 "+result)

    val Array(training, testing) = analysis.getTrainingAndTesting(numericReview, "user_id","business_id", SplitPer)
    training.show()
    analysis.outputResult(training,"parquet", 1, OutputTraining)
    testing.show()
    analysis.outputResult(testing,"parquet", 1, OutputTesting)
    println("Training:"+training.count()+" Testing:"+testing.count())
    println("MoreOrLess:"+MoreOrLess)
    println("IsNumericID:Yes")
    println("ReviewNum:"+ReviewNum)
    println("----------------"+new Date(System.currentTimeMillis())+"-------------------")
  }
}

/**
  * 中文论文
  * 仅按user的评论数过滤,并划分训练集和测试集
  * 保证User-Item对在训练集和测试集不重复
  * 取全部社交网络
  */
object YelpTextUserFilterAllSocial{
  val yelp2016_review = "textdata/yelp_academic_dataset_review.json"
  val yelp2016_user = "textdata/yelp_academic_dataset_user.json"
  val yelp2017_review = "yelp2018/review.json"
  val yelp2017_user = "yelp2018/user.json"
  val yelptest_review = "yelptest/review.json"
  val yelptest_user = "yelptest/user.json"
  val SplitPer = 0.8
  val OutputPath = "output/ChinesePaper/AllSocial/Yelp2016UserBusinessStarReview10Num"
  val OutputTraining = "output/ChinesePaper/AllSocial/Yelp2016UserBusinessStarReview10TrainingNum"+SplitPer
  val OutputTesting = "output/ChinesePaper/AllSocial/Yelp2016UserBusinessStarReview10TestingNum"+(1-SplitPer)
  val OutputSocial = "output/ChinesePaper/AllSocial/Yelp2016UserBusinessStarReview10SocialKeepAll"
  val MoreOrLess = ">"
  val ReviewNum = 10
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    //获得所有评论
    val reviewData = analysis.userItemRateTextAnalysisNotrans(yelp2016_review,
      "user_id","business_id","stars","text","json",1).toDF("user_id","business_id","stars","text")
    //获得所有用户
    val userData = analysis.userItemRateTextAnalysisNotrans(yelp2016_user,
      "user_id","yelping_since","review_count","friends","json",1).toDF("user_id","yelping_since","review_count","friends")
    //获得所有用户的社交网络
    val friends = analysis.getdata.getYelpUserFriendsTrustData(userData,
      "user_id","friends").select("_1", "_2").toDF("user_id","friends")
    //filteredUser(error) or filter1(error)
    //使用所有评论的StringIndexer来转换社交网络，达到user_id的一致性
    val socialNum = analysis.getdata.transformSocialNetwork(reviewData, friends, "user_id", "friends")
    socialNum.show(100)
    analysis.outputResult(socialNum,"csv", 1, OutputSocial)
    //将所有评论的用户和商家转换为数字id
    val numericReview = analysis.transformId(reviewData, "user_id","business_id","stars","text")
      .toDF("user_id","business_id","stars","text")
    numericReview.show(100)
    //println("Friends Number:" +friends.count()+"=?"+socialNum.dropDuplicates().count())
    //println("Review Data Count:"+numericReview.count())
    //按条件过滤得到符合要求的用户
    val filter1 = analysis.userItemRateFilterAnalysis(numericReview,"user_id",MoreOrLess,ReviewNum).toDF("user_id","count")
    //将用户的评论通过join选取出来
    val filteredReview = numericReview.join(filter1,"user_id").select("user_id","business_id","stars","text")
    filteredReview.show()
    //保存过滤后的评论
    analysis.outputResult(filteredReview,"parquet", 1, OutputPath)
    //随机划分训练集和测试集并保存，相同user-item对不同时出现在训练集和测试集中
    val Array(training, testing) = analysis.getTrainingAndTesting(filteredReview, "user_id","business_id", SplitPer)
    training.show()
    analysis.outputResult(training,"parquet", 1, OutputTraining)
    testing.show()
    analysis.outputResult(testing,"parquet", 1, OutputTesting)

  }
}

/**
  * 输出按user条件过滤的用户商家评分及社交网络
  */
object YelpOneFilterRatingandTrustOutput{
  def main(args: Array[String]) {
//    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
//    val filter1 = analysis.userItemRateFilterAnalysis(
//      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
//        "user_id","business_id","stars","json",1),"_1",">",20).toDF("userid","count")
//    val filter2 = analysis.userItemRateFilterAnalysis(
//      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
//        "user_id","business_id","stars","json",1),"_2",">",20).toDF("itemid","count")
//    val data = analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
//      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
//    val output1 = data.join(filter1,"userid").join(filter2,"itemid").toDF("_1","_2","_3","_4","_5")

    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_1","<",5).toDF("user_id","count")
    val data1 = analysis.userItemRateTextAnalysisNotrans("textdata/yelp_academic_dataset_user.json",
      "user_id","yelping_since","review_count","friends","json",1).toDF("user_id","yelping_since","review_count","friends")
    val output1 = data1.join(filter1,"user_id").select("user_id","yelping_since","review_count","friends")
    analysis.outputResult(output1, "parquet", 1, "output/YelpUserReviewLessthan5Friends")
    output1.show()
    println("DataUserNum:"+output1.select("user_id").distinct().count())

//输出社交网络
    val friends = analysis.getdata.getYelpUserFriendsTrustData(output1,
      "user_id","friends")
    val socialNoTrust = friends.select("_1", "_2")
    socialNoTrust.show()
    analysis.outputResult(socialNoTrust, 1, "output/YelpUserReviewLessthan5Social")
    val socialNum = analysis.getdata.transformUseridandItemid(socialNoTrust, "_1", "_2")
      //添加信任
      //.withColumn("_3", lit(1))
    socialNum.show()
    println("UserNum:"+socialNum.select("_1").distinct().count())
    analysis.outputResult(socialNum, "csv1", 1, "output/YelpUserReviewLessthan5SocialNumTrust")

//输出商家评分和评论
//    val data2 = analysis.userItemRateTextAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
//      "user_id","business_id","stars","text","json",1).toDF("user_id","business_id","stars","text")
//    data2.show()
//    val output2 = data2.join(socialNoTrust.select("_1").distinct().toDF("user_id"),"user_id")
//      .select("user_id","business_id","stars","text")
//    val output3 = analysis.getdata.transformUseridItemidRatingText(output2, "user_id","business_id","stars","text")
//      .toDF("user_id","business_id","stars","text")
//    output3.show()
//    println("UserNum:"+output3.select("user_id").distinct().count())
//    analysis.outputResult(output3,"parquet", 1, "output/YelpTextUserReviewLessthan5Num")

//    val dropre = analysis.getAvg(output1.select("_2","_1","_3").toDF("_1","_2","_3"),"_1","_2","_3")
//    dropre.show()
//    val indexrating = analysis.transformId(dropre,"_1","_2","_3")
//    val users = dropre.select("_1").groupBy("_1").count()
//    users.show()
//    val filteruser = friends.join(users.select("_1"),"_1")
//      .join(users.select("_1").toDF("_2"),"_2")
//    val indextrust = analysis.transformIdUsingIndexer(output1.select("_2").toDF("_1"), "_1", filteruser)
//
//    analysis.outputResult(indexrating, 1, "output/YelpTwoFilterUserandItemMoretan20Rating")
//    analysis.outputResult(indextrust, 1, "output/YelpTwoFilterUserandItemMoretan20Trust")
  }
}

object YelpFriendsAnalysis{
  def main(args: Array[String]) {
    //    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    //    val filter1 = analysis.userItemRateFilterAnalysis(
    //      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
    //        "user_id","business_id","stars","json",1),"_1",">",20).toDF("userid","count")
    //    val filter2 = analysis.userItemRateFilterAnalysis(
    //      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
    //        "user_id","business_id","stars","json",1),"_2",">",20).toDF("itemid","count")
    //    val data = analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
    //      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    //    val output1 = data.join(filter1,"userid").join(filter2,"itemid").toDF("_1","_2","_3","_4","_5")

    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_1",">",20).toDF("user_id","count")
    val data2 = analysis.userItemRateTextAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","text","json",1).toDF("user_id","business_id","stars","text")
    val userItem = filter1.join(data2, "user_id").select("user_id", "business_id")
    userItem.show()
    val itemCount = userItem.select("business_id").distinct().count()
    println("ReviewNum:"+filter1.select("count").reduce((a,b) => Row(a.getLong(0)+b.getLong(0))))
    //println("ReviewNum:"+data2.count())
    println("ItemNum:"+itemCount)
    val data1 = analysis.userItemRateTextAnalysisNotrans("textdata/yelp_academic_dataset_user.json",
      "user_id","yelping_since","review_count","friends","json",1).toDF("user_id","yelping_since","review_count","friends")
    val output1 = data1.join(filter1,"user_id").select("user_id","yelping_since","review_count","friends")
    //analysis.outputResult(output1, "parquet", 1, "output/YelpUserReviewLessthan5Friends")
    output1.show()
    println("UserNum:"+filter1.select("user_id").distinct().count())

    val friends = analysis.getdata.getYelpUserFriendsTrustData(output1,
      "user_id","friends")
    val socialNoTrust = friends.select("_1", "_2")
    socialNoTrust.show()
    val countFriends = socialNoTrust.groupBy("_1").count().orderBy(desc("count"))
    countFriends.show()
    val groupFriends = countFriends.select("count").toDF("FriendNum").groupBy("FriendNum").count()
    groupFriends.orderBy("FriendNum").show(300)
    //analysis.outputResult(groupFriends, "csv", 1, "output/YelpFriendsNumCountRev20")
  }
}
/**
  * 使用评论、社交网络、时间和评分，按照用户评论数过滤
  */
object YelpTextStarTimeTrustUserNumberFilter{
  def main(args: Array[String]) {

    val realReviewFile = "textdata/yelp_academic_dataset_review.json"
    val testReviewFile = "textdata/yelp_review_test.json"
    val realUserFile = "textdata/yelp_academic_dataset_user.json"
    val testUserFile = "textdata/yelp_user_test.json"
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans(realReviewFile,
        "user_id","business_id","stars","json",1),"_1",">",20).toDF("user_id","count")
    val data1 = analysis.userItemRateTextAnalysisNotrans(realUserFile,
      "user_id","yelping_since","review_count","friends","json",1).toDF("user_id","yelping_since","review_count","friends")
    val output1 = data1.join(filter1,"user_id").select("user_id","yelping_since","review_count","friends")
    //analysis.outputResult(output1, "parquet", 1, "output/YelpUserReviewMorethan20Friends")
    output1.show(false)
    println("DataUserNum:"+output1.select("user_id").distinct().count())

    //输出社交网络
    val friends = analysis.getdata.getYelpUserFriendsTrustData(output1,
      "user_id","friends")
    val socialNoTrust = friends.select("_1", "_2")
    socialNoTrust.show()
    analysis.outputResult(socialNoTrust, 1, "output/YelpUserReviewMorethan20Social")
    val socialNum = analysis.getdata.transformUseridandItemid(socialNoTrust, "_1", "_2")
      //添加信任
      //.withColumn("_3", lit(1))
    socialNum.show()
    println("UserNum:"+socialNum.select("_1").distinct().count())
    //analysis.outputResult(socialNum, "csv1", 1, "output/YelpUserReviewMorethan20SocialNumTrust")

    //输出商家评分、评论和时间
    val data2 = analysis.userItemRateTextTimeAnalysisNotrans(realReviewFile,
      "user_id","business_id","stars","text","date","json",1).toDF("user_id","business_id","stars","text","date")
    data2.show()
    val output2 = data2.join(socialNoTrust.select("_1").distinct().toDF("user_id"),"user_id")
      .select("user_id","business_id","stars","text","date")
    val output3 = analysis.getdata.transformUseridItemidRatingTextTime(output2, "user_id","business_id","stars","text","date")
      .toDF("user_id","business_id","stars","text","date").sort("date")
    output3.show()
    println("UserNum:"+output3.select("user_id").distinct().count())
    //analysis.outputResult(output3,"parquet", 1, "output/YelpTextTimeUserReviewMorethan20Num")
  }
}
//使用高斯函数处理时间获得权重
object DateProcessing{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val data1 = analysis.getData("output/YelpTextTimeUserReviewMorethan20Num","parquet").orderBy("date")
    data1.show(false)

//    println("DataCount:"+data1.count())
//    println("90%Date"+data1.take(770022).apply(770021).toString())

    val data2 = data1.withColumn("start", lit("2015-06-04"))
    //val data2 = data1.withColumn("start", lit("2015-12-29"))
    data2.show()
    val data3 = data2.filter(datediff(data2.col("start"), data2.col("date"))>=0)
        .filter(datediff(data2.col("start"), data2.col("date"))<=1825)
    val testing1 = data2.filter(datediff(data2.col("start"), data2.col("date"))<0)
        .filter(datediff(data2.col("date"), data2.col("start"))<=365)
    val testing = testing1.withColumn("s", testing1("stars").cast(IntegerType))
    data3.sort(desc("date")).show()
    //val training = data3.withColumn("weight", GussianDistribution.calculate(5000,0,datediff(data3.col("start"), data3.col("date"))))
    //val training = data3.withColumn("weight", Exp.calculate(10000,datediff(data3.col("start"), data3.col("date"))))
    val training = data3.withColumn("weight", Logistic.calculate(0.0009, 900,datediff(data3.col("start"), data3.col("date"))))
    //val training = data3.withColumn("weight", Linear.calculate(2000,datediff(data3.col("start"), data3.col("date"))))
    //val training = data3.withColumn("weight", Curve.quaFunction(5000,datediff(data3.col("start"), data3.col("date"))))
    val filterTraining = training.filter(training.col("weight")>0)
    training.select("date","start","weight").dropDuplicates().show(1000,false)
    testing.show()
    println("TrainingCount:"+training.count())
    println("TestingCount:"+testing.count())
    analysis.outputResult(filterTraining,"parquet", 1, "output/YelpTextTimeLog0009.900UserReviewMorethan20Num1825")
    analysis.outputResult(testing,"parquet", 1, "output/YelpTextTimeWeightFeatureRev20Em365NumSo")
  }
}
//获得LDA向量
object GetLDAResult{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    //训练集
    val data1 = analysis.getData("output/YelpTextTimeLog0009.900UserReviewMorethan20Num1825","parquet")
      .select("date","weight").dropDuplicates().orderBy("date")
    data1.show()
    //训练获得的LDA向量
    val data2 = analysis.getData("output/YelpTextTime300FeatureLDA30JoinEmRev20Num","parquet")
      .select("topicDistribution", "user_id", "business_id", "s", "date")
        //.withColumn("weight", lit(1))
    data2.show()
    println("InitCount:"+data2.count())
    val result = data1.join(data2, "date")
    result.show()
    println("FinalCount:"+result.count())
    //具有LDA向量的训练集
    analysis.outputResult(result,"parquet", 1, "output/YelpTextTimeLog0009.900LDA30JoinEmRev20Num1825")
  }
}

object ConvertToCsv{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
//    val alldata = analysis.getData("output/YelpTextTimeUserReviewMorethan20Num","parquet")
//      .select("user_id", "business_id", "stars")
//    alldata.show()
    val trainingData = analysis.getData("output/YelpTextTimeLog001.1000UserReviewMorethan20Num1825","parquet")
      .select("user_id", "business_id", "stars")
    trainingData.show()
    val testingData = analysis.getData("output/YelpTextTimeWeightFeatureRev20Em365NumSo","parquet")
      .select("user_id", "business_id", "stars")
    testingData.show()
    analysis.outputResult(trainingData.union(testingData),"csv", 1, "output/YelpTextTimeFeatureRev20Em1Num1825365So")
    analysis.outputResult(trainingData,"csv", 1, "output/YelpTextTimeFeatureRev20Em1825NumSo")
    analysis.outputResult(testingData,"csv", 1, "output/YelpTextTimeFeatureRev20Em365NumSo")
  }
}
/**
  * 仅按user的评论数过滤,转换用户为数字id
  */
object YelpTextUserFilterNumberic{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
//    val filter1 = analysis.userItemRateFilterAnalysis(
//      analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
//        "user_id","business_id","stars","json",1),"_1",">",20).toDF("user_id","count")
//    filter1.show()
//    val data = analysis.userItemRateTextAnalysis("textdata/yelp_academic_dataset_review.json",
//      "user_id","business_id","stars","text","json",1).toDF("user_id","business_id","stars","text")
//    data.show()
//    val output1 = data.join(filter1,"user_id").select("user_id","business_id","stars","text")
//    println(output1.select("user_id").distinct().count())
//    //analysis.outputResult(output1,"parquet", 1, "output/YelpTextUserReviewMorethan20Num")

  val output1 = analysis.getData("output/YelpTextUserReviewLessthan5Num","parquet").toDF("user_id","business_id","stars","text")
    output1.show()
//    val output2 = analysis.getData("output/YelpTextUserReviewMorethan10","parquet")
//    output2.show()

    val data = output1.select("user_id","business_id","stars")
    analysis.outputResult(data,"csv", 1, "output/YelpTextUserReviewLessthan5NumRemoveText")
  }
}

/**
  * 使用LDA计算特征分布，线性回归训练并测试模型
  */
object YelpTextRegression2{
  def main(args: Array[String]) {
//    val analysis = new DataAnalysis("./src/data/")
    val analysis = new DataAnalysis("hdfs://172.31.34.183:9000/")
//val analysis = new DataAnalysis("/home/pi/doc/dataset/")
//    val output1 = analysis.getData("output/YelpTextMorethan10Union/part-00000-7d28e5cb-5fd7-4cb5-a746-4dadbbc912a1.snappy.parquet","parquet")
//    output1.show(false)
  val output1 = analysis.getData("inputdata/Yelp2016UserBusinessStarReview20Training0.8","parquet")
//val output1 = analysis.getData("output/YelpTextUserReviewMorethan10Num","parquet")
        //output1.show(false)
//    val output1 = analysis.getData("output/YelpTextMorethan10Union/part-00000-f0a6fd64-d1cc-4a53-82b8-8bb50987d7f7.snappy.parquet","parquet")
//    output1.show(false)
    val Array(training,testing) = output1.randomSplit(Array(1,0))
    //    val rating = analysis.userItemRateTextAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
    //      "user_id","business_id","stars","text","json",0.0001).toDF("user_id","business_id","stars","text")
    //    rating.show()
    val dataset = training.withColumn( "s", training.col("stars").cast(IntegerType)).cache()
    val vector = analysis.transTextToVector(dataset,"text")
    //vector.select("remove2", "vector").show(false)
    val lda = new LDAText().run(vector,"vector",25,100,30).select("topicDistribution","user_id","business_id","s")
    lda.show()
    analysis.outputResult(lda,"parquet", "outputdata/ChinesePaper/Yelp2016UserBusinessStarReview20Training0.8LDA25Iter100")
//    println("--------TotalRegression----------")
//    val result1 = new LinearRegressionAl().run(lda,"topicDistribution","s")
//    println("--------UserRegression----------")
//    val result2 = analysis.regression(lda,"user",10)
//    val result3 = analysis.regression(lda,"item",10)
//    new WriteFile().write("./src/data/output/","YelpTextRegression2-0.1","0.1Union:"+"\n"+result1+"\n"+result2+"\n"+result3)
//    new WriteFile().write("./src/data/output/","YelpTextRegression2Total","Union:"+"\n"+result1)
//    new WriteFile().write("./src/data/output/","YelpTextRegression2Total","Union:"+"\n"+result1)
  }
}

object YelpLocalLDA{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val output1 = analysis.getData("output/YelpTextUserReviewMorethan10","parquet")
    output1.show(100, false)
//    val Array(training,testing) = output1.randomSplit(Array(0.001,1))
//    val dataset = training.withColumn( "s", training.col("stars").cast(IntegerType)).cache()
//    val vector = analysis.transTextToVector(dataset,"text")
//    println(System.currentTimeMillis())
//    val lda = new LDAText().run(vector,"vector",30,100,30).select("topicDistribution","user_id","business_id","s")
//    lda.show()
//    println(System.currentTimeMillis())
//    //analysis.outputResult(lda,"parquet", "output/YelpTestLDA30Rev20Num0.01")
  }
}

object ShowData{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("hdfs://172.31.34.183:9000/")
        val output1 = analysis.getData("inputdata/YelpTextUserReviewMorethan10","parquet")
        output1.show(false)
    println(analysis.analyseSparsity(output1.toDF("_1","_2","_3","_4")))
  }
}

object YelpFriendsCluster{
  def main(args: Array[String]) {

    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val output1 = analysis.getData(
      "output/YelpTextMorethan10Union/part-00000-088deffa-a8ed-4bd2-b6e8-703eb5fb464e-c000.snappy.parquet","parquet")
      .select("user_id").dropDuplicates()
    output1.show(false)
    println("Uniondata:"+output1.count())
    val friends = analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_user.json",
      "user_id","friends","json", 1).select("_1","_2").toDF("user_id","friends")
    friends.show()
    println("Alldata:"+friends.count())

    val filter = friends.join(output1,"user_id")
    filter.show()
    println("Filterdata:"+filter.count())
    val result = analysis.userandFriendTrustAnalysis(filter, "user_id","friends", 1).select("_1","_2").toDF("user1","user2")
    result.show()
    analysis.outputResult(result,"parquet", 1, "output/YelpFriendsUnion")
    println("Resultdata:"+result.count())
  }
}

object YelpAvg{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("hdfs://172.31.34.14:9000/user/spark/src/data/")
    val output1 = analysis.getData("output/YelpTextMorethan10Join","parquet")
    output1.show(false)
    val result = output1.select("stars").groupBy().avg().first().toString()
    println("JoinAvg:"+result)
    //new WriteFile().write("./src/data/output/","YelpAvg","JoinAvg:"+result)
  }
}

/**
  * Total预测模型
  */
object YelpTextTotalRegression {
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("hdfs://172.31.34.14:9000/user/spark/src/data/")
    val output1 = analysis.getData("output/YelpTextFeatureJoinEm0.01", "parquet")
    output1.show(false)
    val result1 = new LinearRegressionAl().run(output1,"topicDistribution","s")
  }
}

/**
  * 基于User的个性化预测模型
  */
object YelpTextUserRegression {
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("hdfs://172.31.34.14:9000/user/spark/src/data/")
    val output1 = analysis.getData("output/YelpTextFeatureJoinOn0.01", "parquet")
    output1.show(false)
    //    val Array(training, testing) = output1.randomSplit(Array(1, 0))
    val result2 = analysis.regression(output1,"user",10)
  }
}

/**
  * 基于Item的个性化预测模型
  */
object YelpTextItemRegression {
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("hdfs://172.31.34.14:9000/user/spark/src/data/")
    val output1 = analysis.getData("output/YelpTextFeatureJoinOn0.01", "parquet")
    output1.show(false)
    //    val Array(training, testing) = output1.randomSplit(Array(1, 0))
    val result2 = analysis.regression(output1,"item",10)
  }
}

/**
  * 输出所有的Yelp用户商家评分及用户社交网络（trust=1）
  * 注意：将StringID转换为IntegerID时，应该最后转换，中间应使用唯一标识的原始StringID计算。
  * 防止在中途出现ID无法对应的情况。例如，在计算评分表和社交表的时候，
  * 两表的用户ID数量不同，不能先转换再join，应该先join最后再transformIdUsingIndexer
  */
object YelpRatingandTrustOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val rating = analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1)
    rating.show()
    val friends = analysis.userandFriendTrustAnalysis("textdata/yelp_academic_dataset_user.json",
      "user_id","friends", 1)
    friends.show()
    //将userid和itemid重复的记录计算平均分
    val indexrating = analysis.getAvg(analysis.transformId(rating,"_1","_2","_3")
      ,"_1","_2","_3")
    val indextrust = analysis.transformIdUsingIndexer(rating, "_1", friends)
    indexrating.show()
    indextrust.show()
    analysis.outputResult(indexrating, 1, "output/DataAnalysisYelpUserItemAll")
    analysis.outputResult(indextrust, 1, "output/DataAnalysisYelpUserTrust")
  }
}

/**
  * 输出按两个条件过滤的用户商家评分及社交网络
  */
object YelpTwoFilterRatingandTrustOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_1",">",20).toDF("userid","count")
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_2",">",20).toDF("itemid","count")
    val data = analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    val output1 = data.join(filter1,"userid").join(filter2,"itemid").toDF("_1","_2","_3","_4","_5")
    val friends = analysis.userandFriendTrustAnalysis("textdata/yelp_academic_dataset_user.json",
      "user_id","friends", 1)
    val dropre = analysis.getAvg(output1.select("_2","_1","_3").toDF("_1","_2","_3"),"_1","_2","_3")
    dropre.show()
    val indexrating = analysis.transformId(dropre,"_1","_2","_3")
    val users = dropre.select("_1").groupBy("_1").count()
    users.show()
    val filteruser = friends.join(users.select("_1"),"_1")
      .join(users.select("_1").toDF("_2"),"_2")
    val indextrust = analysis.transformIdUsingIndexer(output1.select("_2").toDF("_1"), "_1", filteruser)

    analysis.outputResult(indexrating, 1, "output/YelpTwoFilterUserandItemMoretan20Rating")
    analysis.outputResult(indextrust, 1, "output/YelpTwoFilterUserandItemMoretan20Trust")
  }
}


object DataAnalysisYelpUserandItem10{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val result = analysis.analyseSparsity(analysis.userItemRateAnalysis(
      "output/YelpTwoFilterUserandItemMoretan20Rating/part-r-00000-bbc6b22c-a761-4a35-adb2-dabe04b43877.csv",
      "_c0","_c1","_c2","csv1",1))
    println("DataAnalysisYelpTest"+result)
  }
}
object TrustTransform{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val result = analysis.transformTrustValueToOne(analysis.userItemRateAnalysisNotrans(
      "output/DataAnalysisYelpUserItemLocRating1-10All/part-00000-6b3c6717-ad37-4497-9b74-333e661948fc.csv",
      "_c0","_c1","_c2","csv1",1),"_1","_2","_3")
    result.show()
    analysis.outputResult(result, 1, "output/DataAnalysisYelpUserItemLocTrust1All")
  }
}

object DataAnalysisYelpUserItemLocation{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val rating = analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
      "business_id", "user_id", "stars", "json", 1)
    val lalon = analysis.itemLaLonAnalysisNotrans("textdata/yelp_academic_dataset_business.json",
      "business_id", "latitude", "longitude", "json", 1)
    val userlalon = rating.join(lalon, "_1").toDF("itemid", "userid", "rating", "la", "lon")
      .select("userid", "la", "lon")
    userlalon.show()
    val useravg = userlalon.groupBy("userid").avg()
    useravg.show()

    val friends = analysis.userandFriendTrustAnalysis("textdata/yelp_academic_dataset_user.json",
      "user_id", "friends", 1)
    friends.show()
    val user1lalon = useravg.toDF("_1", "la1", "lon1").join(friends, "_1")
    user1lalon.show()
    val user2lalon = user1lalon.join(useravg.toDF("_2", "la2", "lon2"), "_2")
    user2lalon.show()
    val result = user2lalon.select("_1", "_2", "la1", "lon1", "la2", "lon2")
    result.show()

    //    val ss = SparkSession.builder().appName("Yelp Rating")
    //      .master("local[*]").getOrCreate()
    //    import ss.implicits._
    //result.withColumn("x",pow($"la1"-$"la2")).show()
    val avgrating = analysis.getAvg(rating,"_1","_2","_3").toDF("_2","_1","_3")
    avgrating.show()
    val loresult = result.withColumn("_3", sqrt(pow((result.col("la1") - result.col("la2")), 2)+
      pow((result.col("lon1") - result.col("lon2")),2))).select("_1","_2","_3")
    loresult.show()
    val indexer = analysis.getTransformIndexer(avgrating, "_1")
    val indexedresult = analysis.transformIdUsingIndexer(indexer, loresult)
    indexedresult.show()
    //    val calresult1 = indexedresult.withColumn("_4", lit(1))
    //    calresult1.show()
    val calresult = indexedresult.withColumn("_4", round(pow(exp(indexedresult.col("_3")*10)+1,-1)*2*10, 3)).select("_1","_2","_4")
    calresult.show(false)
    //将userid和itemid重复的记录计算平均分

    val indexrating = analysis.transformId(avgrating, "_1","_2","_3")
    indexrating.show()
    analysis.outputResult(calresult, 1, "output/DataAnalysisYelpUserItemLocTrust1-10All")
    analysis.outputResult(indexrating, 1, "output/DataAnalysisYelpUserItemLocRating1-10All")
  }
}

object Testall {
  def main(args: Array[String]) {
    DataAnalysisYelpTrustOutput.main(args)
    DataFilterGowallaUserandItemOutput.main(args)
  }
}

object cnmdataFilter{
  def main(args: Array[String]) {
    val cnmResult = "output/EnglishPaper/Yelp2016UserBusinessStarReview"+10+"cnm1.txt"
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val allSocialdata = analysis.getData(cnmResult, "csv1")
      .toDF("user_id", "group")
    allSocialdata.show()
    val filteredSocial = allSocialdata.join(allSocialdata.groupBy("group").count(), "group")
      .filter(col("count").gt(10))
    filteredSocial.orderBy("count").show(1000)
    analysis.outputResult(filteredSocial.select("user_id", "group"),"csv1" , 1, "output/Yelp2016UserBusinessStarReview"+10+"cnm2")
  }
}
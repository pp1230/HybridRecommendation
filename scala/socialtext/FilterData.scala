package scala.socialtext

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import scala.datafilter.DataAnalysis
import scala.util.Random


/**
  * Created by pi on 18-3-16.
  */

object FilterData {

  def main(args: Array[String]) {
    val analysis = new DataAnalysis(SocialTextTest.rootPath)
    val spark = analysis.getdata.ss
    import spark.implicits._

//    val photoList = analysis.getData("textdata/photo_id_to_business_id.json", "json")
//    photoList.show()
//    val fileList = analysis.getData("textdata/file_list", "csv").toDF("photo_path")
//    fileList.show(false)
//    val businessData = analysis.getData("/systemdata/SampleBusiness5000/FilterdBusiness.json", "json").select("business_id")
//    businessData.show()
//    val filtedPhoto = photoList.join(businessData, "business_id")
//    filtedPhoto.show()
//    analysis.outputResult(filtedPhoto,"json", 1,"systemdata/filtedPhoto")
//    val filtedFileList = filtedPhoto.select("photo_id").map(row => "./"+(row(0).toString+".jpg"))
//      .toDF("photo_path")
//      .join(fileList, "photo_path")
//    filtedFileList.show(false)
//    analysis.outputResult(filtedFileList,"csv", 1,"systemdata/filtedFileList")
//    println("TotalPhoto:"+photoList.count()+"\nFiltedPhoto:"+filtedPhoto.count()
//    +"\nFileList:"+fileList.count()+"\nFiltedFileList:"+filtedFileList.count())

    /**
      * 随机抽取商家
      */
//    //按照评论数过滤商家
//    val businessdata = analysis.getData("/systemdata/Business/filtedBusiness", "json")
//    val sampleBusiness = businessdata.sample(false, 0.42)
//    sampleBusiness.show()
//    println("TotalBusiness:"+businessdata.count()+"\nSampleBusiness:"+sampleBusiness.count())
//    //analysis.outputResult(sampleBusiness,"json", 1,"systemdata/filtedBusiness")
//
//    //取商家的评论
//    val reviewdata = analysis.getData("textdata/yelp_academic_dataset_review.json", "json")
//    reviewdata.show()
//    val filtedReview = reviewdata.join(sampleBusiness.select("business_id"),"business_id")
//    println("TotalReview:"+reviewdata.count()+"\nFiltedReview:"+filtedReview.count())
//    //analysis.outputResult(filtedReview,"json", 1,"systemdata/filtedReview")
////
////    //取所有用户
//    val userdata = analysis.getData("textdata/yelp_academic_dataset_user.json","json")
//    val filtedUser = userdata.join(filtedReview.select("user_id").distinct(), "user_id")
//    filtedUser.show
//    println("TotalUser:"+userdata.count()+"\nFiltedUser:"+filtedUser.count())
//    //analysis.outputResult(filtedUser,"json", 1,"systemdata/filtedUser")

    /**
      * 按照商家的评论过滤
      */
//    //按照评论数过滤商家
    val businessdata = analysis.getData("textdata/yelp_academic_dataset_business.json", "json")
    val filtedBusiness = businessdata.filter($"review_count" > 110)
    filtedBusiness.show()
    println("TotalBusiness:"+businessdata.count()+"\nFiltedBusiness(Review>50):"+filtedBusiness.count())
    //analysis.outputResult(filtedBusiness,"json", 1,"systemdata/filtedBusiness")
    //取所有用户
    val userdata = analysis.getData("textdata/yelp_academic_dataset_user.json","json")
    val filtedUser = userdata.filter($"review_count" > 100)
    filtedUser.show()
    println("TotalUser:"+userdata.count()+"\nFiltedUser(Review>50):"+filtedUser.count())

    //取商家的评论
    val reviewdata = analysis.getData("textdata/yelp_academic_dataset_review.json", "json")
    reviewdata.show()
    val filtedReview = reviewdata.join(filtedBusiness.select("business_id"),"business_id")
      .join(filtedUser.select("user_id"), "user_id")
    println("TotalReview:"+reviewdata.count()+"\nFiltedReview:"+filtedReview.count())
    //analysis.outputResult(filtedReview,"json", 1,"systemdata/filtedReview")


//    val filtedUser = userdata.join(filtedReview.select("user_id").distinct(), "user_id")
//    filtedUser.show
//    println("TotalUser:"+userdata.count()+"\nFiltedUser:"+filtedUser.count())
    //analysis.outputResult(filtedUser,"json", 1,"systemdata/filtedUser")

//
//        //生成推荐列表
//        val filtedBusiness = analysis.getData("/systemdata/SampleBusiness5000/FilterdBusiness.json", "json").select("business_id")
//    filtedBusiness.show()
//        var recbusiness = filtedBusiness.select("business_id").sample(true, 0.003).withColumn("id", lit(0))
//        for(i <- 1 to 100){
//          recbusiness = recbusiness
//            .union(filtedBusiness.select("business_id").sample(true, 0.003).withColumn("id", lit(i)))
//        }
//        recbusiness.show(100)
//    val filtedUser = analysis.getData("/systemdata/SampleBusiness5000/FilteredUser.json", "json").select("business_id")
//    filtedUser.show()
//        val recuser = filtedUser.select("user_id").withColumn("id", lit(1))
//          .map(row => (row.getString(0), new Random().nextInt(100))).toDF("user_id", "id")
//        recuser.show(100)
//
//        val recommendation = recuser.join(recbusiness, "id")
//          .drop("id")
//          .orderBy("user_id")
//        recommendation.show(100)
//        println("RocommendationListCount:"+recommendation.count())
//    analysis.outputResult(recommendation,"json", 1,"systemdata/recommendation")

    /**
      * 按照用户的评论过滤
      */
    //    //通过用户表过滤,评论数大于10的用户
//    val output1 = analysis.getData("textdata/yelp_academic_dataset_user.json","json")
//    //output1.show()
//    val userMoreThan10 = output1.filter(output1.col("review_count")>20)
//    userMoreThan10.show()
//    println("TotalUser:"+output1.count()+"\nReviewMoreThan10 User:"+userMoreThan10.count())
//    //analysis.outputResult(userMoreThan10,"json", 1,"systemdata/userMoreThan10")
//
//    //通过评论表过滤,评论数大于10用户的评论
//    val output2 = analysis.getData("textdata/yelp_academic_dataset_review.json", "json")
//    //output2.show()
//    val userNum = output2.select("user_id").distinct().count()
//    println("TotalReview:"+output2.count()+"\nReviewUser:"+userNum)
//    val output3 = output2.join(userMoreThan10.select("user_id"), "user_id")
//    output3.show()
//    println("MoreThan10User Review:"+output3.count())
//    //analysis.outputResult(output3,"json", 1,"systemdata/reviews")
//
//    //通过评论表中的商家过滤商家表的数据
//    val output4 = analysis.getData("textdata/yelp_academic_dataset_business.json", "json")
//    //output4.show()
//    val filtedBusiness = output4.join(output3.select("business_id").distinct(), "business_id")
//    filtedBusiness.show()
//    println("TotalBusiness:"+output4.count()+"\nFiltedBusiness:"+filtedBusiness.count())
//    //analysis.outputResult(filtedBusiness,"json", 1,"systemdata/filtedBusiness")
//
//    //生成推荐列表
//    var recbusiness = filtedBusiness.select("business_id").sample(true, 0.0002).withColumn("id", lit(0))
//    for(i <- 1 to 50){
//      recbusiness = recbusiness
//        .union(filtedBusiness.select("business_id").sample(true, 0.0002).withColumn("id", lit(i)))
//    }
//    recbusiness.show(100)
//    val recuser = userMoreThan10.select("user_id").withColumn("id", lit(1))
//      .map(row => (row.getString(0), new Random().nextInt(50))).toDF("user_id", "id")
//    recuser.show(100)
//
//    val recommendation = recuser.join(recbusiness, "id")
//      .drop("id")
//      .orderBy("user_id")
//    recommendation.show(1000)
//    println("RocommendationListCount:"+recommendation.count())
//    analysis.outputResult(recommendation,"json", 1,"systemdata/recommendationlist")
  }

}

package scala.datafilter

import org.apache.spark.ml.feature.{StringIndexerModel, StringIndexer}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by pi on 7/3/17.
  */
class DataAnalysis(read:String) {

  var getdata = new GetRandomData(read)

  def getData(path:String,format:String):DataFrame={
    if(format.equals("libsvm"))
    return getdata.getLibsvmData(path)
    else if(format.equals("text"))
      return getdata.getTextData(path)
    else if(format.equals("csv"))
      return getdata.getCsvData(path)
    else if(format.equals("csv1"))
      return getdata.getCsv1Data(path)
    else if(format.equals("csv2"))
      return getdata.getCsv2Data(path)
    else if(format.equals("json"))
      return getdata.getJsonData(path)
    else if(format.equals("parquet"))
      return getdata.getParquetData(path)
    else null
  }

  def transTextToVector(dataFrame: DataFrame,col:String):DataFrame={
    getdata.textToVector(dataFrame,col)
  }

  def textToFeatureArray(dataFrame: DataFrame, col:String):DataFrame={
    getdata.textToFeatureArray(dataFrame,col)
  }


  /**
    * 从给定路径的数据集中获取用户商户id和评分
    *
    * @param datapath 数据集路径
    * @param user 用户列名
    * @param item 商户列名
    * @param rate 评分列名
    * @param per 分析数据比例（随机抽取）
    * @return 数据表
    */
  def userItemRateAnalysis(datapath:String,user:String,item:String,rate:String,format:String,per:Double): DataFrame ={
    var format1 = getdata.getRawPercentData(datapath,per)
    if(format.equals("csv"))
      format1 = getdata.getCsvRawPercentData(datapath,"\t",per)
    else if(format.equals("csv1"))
      format1 = getdata.getCsvRawPercentData(datapath,",",per)
    val data = getdata.getUserItemRating(format1,user,item,rate)
    return data
  }

  /**
    * 从给定路径的数据集中获取用户商户id,rating,text
    *
    * @param datapath 数据集路径
    * @param user 用户列名
    * @param item 商户列名
    * @param rate 评分列名
    * @param text 评分列名
    * @param per 分析数据比例（随机抽取）
    * @return 数据表
    */
  def userItemRateTextAnalysis(datapath:String,user:String,item:String,rate:String,text:String,format:String,per:Double): DataFrame ={
    var format1 = getdata.getRawPercentData(datapath,per)
    if(format.equals("csv"))
      format1 = getdata.getCsvRawPercentData(datapath,"\t",per)
    else if(format.equals("csv1"))
      format1 = getdata.getCsvRawPercentData(datapath,",",per)
    val data = getdata.getUserItemRatingText(format1,user,item,rate,text)
    return data
  }

  def getTrainingAndTesting(datapath:String,user:String,item:String,rate:String,text:String,format:String,per:Double):Array[DataFrame]={
    val rawData = userItemRateTextAnalysis(datapath, user, item, rate, text, format, 1)
      .toDF(user, item, rate, text)
    return getdata.getPercentUserAndBusiness(rawData, user, item, per)

  }

  def getTrainingAndTesting(data:DataFrame,user:String,item:String,per:Double):Array[DataFrame]={
    return getdata.getPercentUserAndBusiness(data, user, item, per)

  }


  /**
    * 从给定路径的数据集中获取用户商户id和评分，不做id转换
    *
    * @param datapath 数据集路径
    * @param user 用户列名
    * @param item 商户列名
    * @param rate 评分列名
    * @param format 格式
    * @param per 提取数据比例（随机抽取）
    * @return 数据表
    */
  def userItemRateAnalysisNotrans(datapath:String, user:String,item:String,rate:String,format:String,per:Double): DataFrame ={
    var format1 = getdata.getRawPercentData(datapath,per)
    if(format.equals("csv"))
      format1 = getdata.getCsvRawPercentData(datapath,"\t",per)
    else if(format.equals("csv1"))
      format1 = getdata.getCsvRawPercentData(datapath,",",per)
    val data = getdata.selectData(format1,user,item,rate)
    return data
  }

  def userItemRateAnalysisNotrans(datapath:String, user:String,rate:String,format:String,per:Double): DataFrame ={
    var format1 = getdata.getRawPercentData(datapath,per)
    if(format.equals("csv"))
      format1 = getdata.getCsvRawPercentData(datapath,"\t",per)
    else if(format.equals("csv1"))
      format1 = getdata.getCsvRawPercentData(datapath,",",per)
    val data = getdata.selectData(format1,user,rate)
    return data
  }

  def userItemRateTextAnalysisNotrans(datapath:String, user:String,item:String,rate:String,text:String, format:String,per:Double): DataFrame ={
    var format1 = getdata.getRawPercentData(datapath,per)
    if(format.equals("csv"))
      format1 = getdata.getCsvRawPercentData(datapath,"\t",per)
    else if(format.equals("csv1"))
      format1 = getdata.getCsvRawPercentData(datapath,",",per)
    val data = getdata.selectData(format1,user,item,rate,text)
    return data
  }

  def userItemRateTextTimeAnalysisNotrans(datapath:String, user:String,item:String,rate:String,text:String,time:String, format:String,per:Double): DataFrame ={
    var format1 = getdata.getRawPercentData(datapath,per)
    if(format.equals("csv"))
      format1 = getdata.getCsvRawPercentData(datapath,"\t",per)
    else if(format.equals("csv1"))
      format1 = getdata.getCsvRawPercentData(datapath,",",per)
    else if(format.equals("parquet"))
      format1 = getdata.getParquetRawPercentData(datapath ,per)
    val data = getdata.selectData(format1,user,item,rate,text,time)
    return data
  }

  def itemLaLonAnalysisNotrans(datapath:String,item:String,la:String,lon:String, format:String,per:Double): DataFrame ={
    var format1 = getdata.getRawPercentData(datapath,per)
    if(format.equals("csv"))
      format1 = getdata.getCsvRawPercentData(datapath,"\t",per)
    else if(format.equals("csv1"))
      format1 = getdata.getCsvRawPercentData(datapath,",",per)
    val data = getdata.selectData(format1,item,la,lon)
    return data
  }

  /**
    * 将id转换为Integer
    *
    * @param input
    * @param col1
    * @param col2
    * @param col3
    * @return
    */
  def transformId(input:DataFrame, col1:String, col2:String, col3:String): DataFrame ={
    return getdata.transformUseridandItemidOne(input,col1,col2,col3)
  }
  def transformId(input:DataFrame, col1:String, col2:String, col3:String, col4:String): DataFrame ={
    return getdata.transformUseridItemidRatingText(input,col1,col2,col3,col4)
  }

  /**
    * 使用特定数据集训练的Indexer转换给定的数据集，用于转换社交网络用户id
    *
    * @param trainingdata 训练Indexer的数据集
    * @param trainingcol 训练的列名
    * @param input 需要转换的数据集（转换前两列）
    * @return
    */
  def transformIdUsingIndexer(trainingdata:DataFrame, trainingcol:String, input:DataFrame):DataFrame={
    val indexer = getdata.getIndexer(trainingdata, trainingcol)
    val result = getdata.getIndexingData(input,indexer)
    return result
  }

  def transformIdUsingIndexer(indexer:StringIndexerModel, input:DataFrame):DataFrame={
    val result = getdata.getIndexingData(input,indexer)
    return result
  }

  def getTransformIndexer(trainingdata:DataFrame, trainingcol: String): StringIndexerModel = {
    return getdata.getIndexer(trainingdata, trainingcol)
  }

  /**
    * 将用户的多个好友展开成一一对应的列
    *
    * @param datapath 包含好友关系的数据集
    * @param user 用户列名
    * @param friends 好友数组列名
    * @param per 提取数据比例
    * @return 好友关系表
    */
  def userandFriendTrustAnalysis(datapath:String, user:String, friends:String, per:Double):DataFrame ={
    var raw = getdata.getRawPercentData(datapath,per)
    val data = getdata.getYelpUserFriendsTrustData(raw,user,friends)
    return data
  }

  def userandFriendTrustAnalysis(dataFrame: DataFrame, user:String, friends:String, per:Double):DataFrame ={
    val data = getdata.getYelpUserFriendsTrustData(dataFrame,user,friends)
    return data
  }

  def transformTrustValueToOne(dataFrame: DataFrame, user1:String, user2:String, trust:String):DataFrame = {
    val result = getdata.transformToTrust1(dataFrame, user1, user2, trust)
    return result
  }

  /**
    * 从给定数据集中获取用户和商户的id和经纬度
    *
    * @param datapath
    * @param user
    * @param item
    * @param la
    * @param lon
    * @param format 默认json，若使用csv则使用\t作为隔断
    * @param per
    * @return
    */
  def userItemlalonAnalysis(datapath:String,user:String,item:String,la:String, lon:String, format:String,per:Double): DataFrame ={
    var format1 = getdata.getRawPercentData(datapath,per)
    if(format.equals("csv"))
      format1 = getdata.getCsvRawPercentData(datapath,"\t",per)
    val data = getdata.getUserItemlalon(format1,user,item,la,lon)
    return data
  }

  /**
    * 求平均，转换id
    *
    * @param datapath
    * @param user
    * @param item
    * @param rate
    * @param per
    * @return
    */
  def userItemAvgratingAnalysis(datapath:String,user:String,item:String,rate:String,per:Double):DataFrame = {
    val data = getdata.getUserItemAvgrating(getdata.getRawPercentData(datapath,per)
      ,user,item,rate)
    return data
  }

  /**
    * 求平均，不转换id
    *
    * @param input
    * @param user
    * @param item
    * @param rate
    * @return
    */
  def getAvg(input:DataFrame, user:String, item:String, rate:String):DataFrame={
    return getdata.getUserItemAvg(input,user,item,rate)
  }

  /**
    * 计数并条件过滤（选择一列满足条件）
    *
    * @param input 输入数据表
    * @param ob 过滤的列名
    * @param filter 过滤条件
    * @param num 过滤边界
    * @return 含有过滤列和计数列的数据表
    */
  def userItemRateFilterAnalysis(input:DataFrame, ob:String, filter:String, num:Int):DataFrame={
    if(filter.equals(">")){
      val data = getdata.getUserCheckinMoreThan(input,ob,num)
      return data
    }
    else if(filter.equals("<")){
      val data = getdata.getUserCheckinLessThan(input,ob,num)
      return data
    }
    else if(filter.equals("=")){
      val data = getdata.getUserCheckinEqualWith(input,ob,num)
      return data
    }
    else return null

  }

  def userTrustFilterAnalysis(input:DataFrame, ob:String, filter:String, num:Int):DataFrame={
    if(filter.equals(">")){
      val data = getdata.getUserTrustMoreThan(input,ob,num)
      return data
    }
    else if(filter.equals("<")){
      val data = getdata.getUserTrustLessThan(input,ob,num)
      return data
    }
    else if(filter.equals("=")){
      val data = getdata.getUserTrustEqualWith(input,ob,num)
      return data
    }
    else return null

  }

  /**
    * 计数并条件过滤（两列同时满足）
    *
    * @param input
    * @param ob1
    * @param ob2
    * @param filter
    * @param num
    * @return
    */
  def userItemRateFilterAnalysis(input:DataFrame, ob1:String, ob2:String, filter:String, num:Int):DataFrame={
    if(filter.equals(">")){
      val data = getdata.getUserItemCheckinMoreThan(input,ob1,ob2,num)
      return data
    }
    else if(filter.equals("<")){
      val data = getdata.getUserItemCheckinLessThan(input,ob1,ob2,num)
      return data
    }
    else if(filter.equals("=")){
      val data = getdata.getUserItemCheckinEqualWith(input,ob1,ob2,num)
      return data
    }
    else return null

  }

  /**
    * 个性化回归
    * @param input 含有tipic分布和label
    * @param t 根据user还是item
    * @param n 评论大于n
    * @return
    */
  def regression(input:DataFrame, t:String,n:Int):String ={
    if(t.equals("user"))
    getdata.userRegression(input,n)
    else getdata.itemRegression(input,n)
  }

  /**
    * 分析数据表
    *
    * @param input 数据表
    * @return 稀疏度结果
    */
  def analyseSparsity(input:DataFrame): String ={
    val usernum = input.groupBy("_1").count().count()
    val itemnum = input.groupBy("_2").count().count()
    //计算稀疏性时，需要将checkin中的重复列去除
    val totalnum = input.groupBy("_1","_2").count().count()
    val result = totalnum.toDouble/(usernum * itemnum)
    return "Sparsity is : "+ result*100+"%, "+"User "+usernum + " item "+itemnum +" total "+totalnum
  }

  /**
    * 输出结果（DataFrame）到指定目录
    *
    * @param input
    * @param partition
    * @param writepath
    */
  def outputResult(input:DataFrame, partition:Int, writepath:String): Unit ={
    getdata.writeData(input,partition,writepath)
  }

  def outputResult(input:DataFrame, format:String, partition:Int, writepath:String): Unit ={
    getdata.writeData(input,format,partition,writepath)
  }

  def outputResult(input:DataFrame, format:String, writepath:String): Unit ={
    getdata.writeData(input,format,writepath)
  }


}

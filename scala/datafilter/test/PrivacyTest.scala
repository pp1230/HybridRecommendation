package scala.datafilter.test

import org.apache.hadoop.hdfs.web.resources.Param
import org.apache.spark.ml.param.{ParamPair, ParamMap}
import org.apache.spark.ml.regression.LinearRegressionModel

import scala.algorithm.LinearRegressionAl
import scala.datafilter.DataAnalysis

/**
  * Created by pi on 17-12-27.
  */
object PrivacyTest {
  val rootPath = "/home/pi/doc/dataset/output/"
  val datapath = "YelpTextFeatureLDA30Rev20JoinEm0.7NumItem"
  def main(args: Array[String]) {
    val analysis = new DataAnalysis(rootPath)
    val output1 = analysis.getData(datapath,"parquet")
    output1.show(false)
    println("Total Number:"+output1.count())

    val Array(training,testing) = output1.randomSplit(Array(8,2))
    val LR = new LinearRegressionAl
    val model = LR.fit(training, "topicDistribution", "s")
    println("Param:"+model.coefficients+model.intercept)
    val result = model.transform(testing)
    result.show()
    val evaluate = LR.evaluate(result)
  }
}

package scala.algorithm

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{LinearRegressionModel, LinearRegression}
import org.apache.spark.sql._

/**
  * Created by pi on 17-7-29.
  */
class LinearRegressionAl extends Serializable{

  def run(data:DataFrame,featureCol:String,labelCol:String):DataFrame={

    val result = transform(data,fit(data,featureCol,labelCol))

    //val e = evaluate(result)

    return result
  }

  def fit(dataFrame: DataFrame,featureCol:String,labelCol:String):LinearRegressionModel={
    val lr = new LinearRegression()
      .setMaxIter(50)
      .setRegParam(0.01)
      .setElasticNetParam(0.0)
      .setFeaturesCol(featureCol).setLabelCol(labelCol)

    // Fit the model
    val lrModel = lr.fit(dataFrame)
    return lrModel
  }

  def fit(dataFrame: DataFrame,featureCol:String,labelCol:String,weightCol:String):LinearRegressionModel={
    val lr = new LinearRegression()
      .setMaxIter(30)
      .setRegParam(0.001)
      .setElasticNetParam(0.8)
      .setFeaturesCol(featureCol).setLabelCol(labelCol).setWeightCol(weightCol)

    // Fit the model
    val lrModel = lr.fit(dataFrame)
    return lrModel
  }

  def transform(data:DataFrame,lrModel:LinearRegressionModel):DataFrame={

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    val result = lrModel.transform(data)
    result.show()
    return result
  }

  def evaluate(result:DataFrame):String={
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("s")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(result)
    println(s"Root-mean-square error = $rmse")
    evaluator.setMetricName("mae")
    val mae = evaluator.evaluate(result)
    println(s"Mean-absolute error = $mae")
    return "RMSE:"+rmse+"MAE:"+mae
  }

  def evaluate(metric:String, label:String, prediction:String, data:DataFrame):String={
    val evaluator = new RegressionEvaluator()
      .setMetricName(metric)
      .setLabelCol(label)
      .setPredictionCol(prediction)
    val result = evaluator.evaluate(data)
    //println(s"Root-mean-square error = $result")
    return metric.toUpperCase()+":"+result
  }
}

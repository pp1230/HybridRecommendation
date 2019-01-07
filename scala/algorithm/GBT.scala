package scala.algorithm

import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.DataFrame

/**
  * Created by pi on 17-9-29.
  */
class GBT {
  def fit(data:DataFrame, featureCol:String, label:String):PipelineModel={
    val featureIndexer = new VectorIndexer()
      .setInputCol(featureCol)
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    val gbtr = new GBTRegressor()
        .setLabelCol(label)
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, gbtr))
    pipeline.fit(data)

  }

  def transform(dataFrame: DataFrame,gmm:PipelineModel):DataFrame={
    gmm.transform(dataFrame)
  }
}

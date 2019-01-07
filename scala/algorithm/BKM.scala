package scala.algorithm

import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.sql.DataFrame

/**
  * Created by pi on 17-9-28.
  */
class BKM {
  def fit(data:DataFrame, featureCol:String, num:Int):BisectingKMeansModel={
    val gmm = new BisectingKMeans()
      .setK(num)
        .setSeed(1)
      .setFeaturesCol(featureCol)
    gmm.fit(data)

  }

  def transform(dataFrame: DataFrame, featureCol:String, gmm:BisectingKMeansModel):DataFrame={
    gmm.setFeaturesCol(featureCol).transform(dataFrame)
  }
}

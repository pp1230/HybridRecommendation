package scala.algorithm

import org.apache.spark.ml.clustering.{GaussianMixtureModel, GaussianMixture}
import org.apache.spark.sql.DataFrame

/**
  * Created by pi on 17-9-25.
  */
class GMM {

  def fit(data:DataFrame, featureCol:String, num:Int):GaussianMixtureModel={
    val gmm = new GaussianMixture()
      .setK(num)
        .setFeaturesCol(featureCol)
      .setMaxIter(500)
    gmm.fit(data)

  }

  def transform(dataFrame: DataFrame, featureCol:String, gmm:GaussianMixtureModel):DataFrame={
    gmm.setFeaturesCol(featureCol).transform(dataFrame)
  }


}

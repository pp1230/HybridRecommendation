package scala.algorithm

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.clustering.LDA

/**
  * Created by pi on 17-7-29.
  */
class LDAText {

  def run(dataset:DataFrame,k:Int,iter:Int,tops:Int):DataFrame={

    // Trains a LDA model.
    val lda = new LDA().setK(k).setMaxIter(iter)
    val model = lda.fit(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(tops)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    // Shows the result.
    val transformed = model.transform(dataset)
    transformed.show(false)

    return transformed;
  }
  def run(dataset:DataFrame,feature:String, k:Int,iter:Int,terms:Int):DataFrame={

    // Trains a LDA model.
    val lda = new LDA().setK(k).setMaxIter(iter)
      .setFeaturesCol(feature).setOptimizer("em")
    val model = lda.fit(dataset)

//    val ll = model.logLikelihood(dataset)
//    val lp = model.logPerplexity(dataset)
//    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
//    println(s"The upper bound bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(terms)
    println("The topics described by their top-weighted terms:")
    topics.show(false)


    // Shows the result.
    val transformed = model.transform(dataset)
    transformed.show(false)

    return transformed;
  }

}

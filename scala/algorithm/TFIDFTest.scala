package scala.algorithm

/**
  * Created by pi on 18-1-30.
  */
import org.apache.spark.ml.feature._

import scala.datafilter.DataAnalysis

object TFIDFTest {
  val analysis = new DataAnalysis("")
  val ss = analysis.getdata.ss
  def main(args: Array[String]) {
    val sentenceData = ss.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java are using case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.show(false)


    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)
      .setBinary(true)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors
    featurizedData.show(false)

//    val cvModel: CountVectorizerModel = new CountVectorizer()
//      .setInputCol("words")
//      .setOutputCol("rawFeatures")
////      .setVocabSize(10)
////      .setMinDF(2)
//      .fit(wordsData)
//
//    val featurizedData = cvModel.transform(wordsData)
//    featurizedData.show(false)


    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show(false)
  }
}

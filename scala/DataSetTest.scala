package scala

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{StructType, StringType, StructField}
import org.apache.spark.sql.{SaveMode, Row, Encoder, SparkSession}


/**
  * Created by pi on 16-8-16.
  */
case class DataSetTest(name:String, age:Int) {

  var this.name = name
  var this.age = age


}


object DataSetTest{

  def main(args: Array[String]) {
    val ss = SparkSession.builder().appName("DataSet Test")
      .master("local[*]").getOrCreate()

    import ss.implicits._
    val simpleDS = Seq(DataSetTest("po",12)).toDS()
    simpleDS.show()
    //在已经定义类的情况下生成DF
    val personDF = ss.sparkContext.textFile("/home/pi/doc/Spark/persondata")
      .map(_.split(" ")).map(data => DataSetTest(data(0),data(1).trim.toInt)).toDF()
    personDF.createOrReplaceTempView("person")
    personDF.show()

    val sqlDF = ss.sql("select name,age from person where age < 18")
    sqlDF.show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String,Any]]
    implicit val stringIntMapEncoder : Encoder[Map[String,Int]] = ExpressionEncoder()
    val value = sqlDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    println(value)

    /**
      * 没有定义类的情况下生成DF分为三个步骤
      * 1.Create an RDD of Rows from the original RDD;
      * 2.Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
      * 3.Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
      */

    //create rdd and convert it to Rows
    val personRdd = ss.sparkContext.textFile("/home/pi/doc/Spark/persondata")
      .map(_.split(" ")).map(x => Row(x(0),x(1).trim))
    personRdd.foreach(i => println(i))
    val schemaString = "name age"
    val fields = schemaString.split(" ").map(StructField(_,StringType,nullable = true))
    val schema = StructType(fields)
    // Apply the schema to the RDD
    val personDFNew = ss.createDataFrame(personRdd,schema)
    personDFNew.show()

    val userParquetRdd = ss.read
      .load("/home/pi/doc/Spark/spark-2.0.0-bin-hadoop2.7/examples/src/main/resources/users.parquet")
    userParquetRdd.show()
    userParquetRdd.write.format("json").mode(SaveMode.Overwrite)
      .save("/home/pi/doc/Spark/spark-2.0.0-bin-hadoop2.7/examples/src/main/resources/userjson")
    val userSqlDF = ss
      .sql("select * from parquet.`/home/pi/doc/Spark/spark-2.0.0-bin-hadoop2.7/examples/src/main/resources/users.parquet`")
    userSqlDF.show()
  }
}

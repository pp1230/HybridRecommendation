package scala.algorithm

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
  * Created by pi on 17-12-1.
  */
object Logistic {
  def calculate(n:Double, t:Int, column: Column):Column ={
    pow(exp(column.minus(t).multiply(n)).plus(1), -1)
  }
}

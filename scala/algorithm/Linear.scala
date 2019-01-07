package scala.algorithm

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
  * Created by pi on 17-12-2.
  */
object Linear {
  def calculate(t:Int, column: Column):Column ={
    column.divide(-t).plus(1)
  }
}

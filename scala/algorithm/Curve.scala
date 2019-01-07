package scala.algorithm

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
  * Created by pi on 17-12-2.
  */
object Curve {
  def quaFunction(t:Int, column: Column):Column ={
    (-pow(column.divide(t), 2)).plus(1)
  }
}

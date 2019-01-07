package scala.algorithm

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
  * Created by pi on 17-12-1.
  */
object Exp {

  def calculate(t0:Int, column: Column):Column ={
    exp(column.divide(-t0))
  }
}

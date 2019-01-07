package scala.algorithm

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
  * Created by pi on 17-11-29.
  */
object GussianDistribution {
  def calculate(theta:Double, mu:Int, column: Column): Column ={
    exp(pow(column.minus(mu), 2).divide(-2*theta*theta)).divide(2.506628274631*theta)
  }
}

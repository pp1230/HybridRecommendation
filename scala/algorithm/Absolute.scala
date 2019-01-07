package scala.algorithm

import org.apache.spark.sql.Column

/**
  * Created by pi on 17-7-16.
  */
class Absolute {

  def plus(a:Column, b:Column): Column ={
    return a+b
  }
}

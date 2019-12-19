package api

import api.Mask
import api.PDB
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.math.sqrt

class Distance {

  def find_dist(at_1: String, res_1: Int, at_2: String, res_2: Int, sampleDF: Dataset[PDB]): Double= {
    var mask = new Mask()
    var df1 = mask.mask_res_no(res_1, sampleDF)
    var df2 = mask.mask_res_no(res_2, sampleDF)
    var df3 = mask.mask(at_1, df1)
    var df4 = mask.mask(at_2, df2)
    //df3.show()
    //df4.show()
    var x1 = df3.select("X").collect()
    var x2 = df4.select("X").collect()
    var y1 = df3.select("Y").collect()
    var y2 = df4.select("Y").collect()
    var z1 = df3.select("Z").collect()
    var z2 = df4.select("Z").collect()

    var a = x2(0)(0).toString.toDouble - x1(0)(0).toString.toDouble
    var b = y2(0)(0).toString.toDouble - y1(0)(0).toString.toDouble
    var c = z2(0)(0).toString.toDouble - z1(0)(0).toString.toDouble

    var distance = sqrt((a*a)+(b*b)+(c*c))
    return distance
  }

  def find_dist(at_1: Int, at_2: Int, df: Dataset[PDB]): Double = {
    var mask = new Mask()
    var df1 = mask.mask_index(at_1, df)
    var df2 = mask.mask_index(at_2, df)
    //df1.show()
    //df2.show()

    var x1 = df1.select("X").collect()
    var x2 = df2.select("X").collect()
    var y1 = df1.select("Y").collect()
    var y2 = df2.select("Y").collect()
    var z1 = df1.select("Z").collect()
    var z2 = df2.select("Z").collect()

    var a = x2(0)(0).toString.toDouble - x1(0)(0).toString.toDouble
    var b = y2(0)(0).toString.toDouble - y1(0)(0).toString.toDouble
    var c = z2(0)(0).toString.toDouble - z1(0)(0).toString.toDouble

    var distance = sqrt((a*a)+(b*b)+(c*c))

    return distance
  }

 def find_dist(df1: Array[Row], df2: Array[Row]):Double = {

    var a = df2(0)(0).toString.toDouble - df1(0)(0).toString.toDouble
    var b = df2(0)(1).toString.toDouble - df1(0)(1).toString.toDouble
    var c = df2(0)(2).toString.toDouble - df1(0)(2).toString.toDouble

    var distance = sqrt((a*a)+(b*b)+(c*c))

    return distance
  }
}

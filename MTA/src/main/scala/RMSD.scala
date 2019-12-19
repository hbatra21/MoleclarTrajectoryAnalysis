package api

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.avg

import org.ejml.simple.SimpleMatrix

class RMSD {

  var Patom : SimpleMatrix = null
  var Pres : SimpleMatrix = null
  var pRead : Boolean = false
  var pReadRes : Boolean = false

  def rmsdUtilAtom(df: Dataset[PDB], atomName: String, startFrame: Int, spark: SparkSession): RDD[Double] = {
    import spark.implicits._
    val sc = spark.sparkContext
    var dfarr = new Array[DataFrame](10)
    for (i <- 0 to 9) {
      val temp = df.where($"atom" === atomName && $"frameNo" === startFrame + i).select($"X", $"Y", $"Z")
      dfarr(i) = temp
    }
    var res = new Array[Double](10)
    var pCentroid = new Array[Double](3)
    if(!pRead) {
      val p: Dataset[CRD] = dfarr(0).as[CRD]
      val temp = p.select(avg($"X"), avg($"Y"), avg($"Z")).collect()(0)
      pCentroid = temp.toString.stripPrefix("[").stripSuffix("]").split(",").map(x => x.toDouble)
      val Ptemp = p.map(row => CRD(row.X - pCentroid(0), row.Y - pCentroid(1), row.Z - pCentroid(2)))
      Patom = matrix(Ptemp)
      Patom = Patom.transpose()
      pRead = true
      res(0) = 0.0
      for (i <- 1 to dfarr.length - 1) {
        val q = dfarr(i).as[CRD]
        val temp = q.select(avg($"X"), avg($"Y"), avg($"Z")).collect()(0)
        val qCentroid = temp.toString.stripPrefix("[").stripSuffix("]").split(",").map(x => x.toDouble)
        val Qtemp: Dataset[CRD] = q.map(row => CRD(row.X - qCentroid(0), row.Y - qCentroid(1), row.Z - qCentroid(2)))
        var Q = matrix(Qtemp)
        Q = Q.transpose()
        val k = new Kabsch(Patom, Q)
        res(i) = k.calculate()
      }
    }

    for (i <- 0 to dfarr.length - 1) {
      val q = dfarr(i).as[CRD]
      val temp = q.select(avg($"X"), avg($"Y"), avg($"Z")).collect()(0)
      val qCentroid = temp.toString.stripPrefix("[").stripSuffix("]").split(",").map(x => x.toDouble)
      val Qtemp: Dataset[CRD] = q.map(row => CRD(row.X - qCentroid(0), row.Y - qCentroid(1), row.Z - qCentroid(2)))
      var Q = matrix(Qtemp)
      Q = Q.transpose()
      val k = new Kabsch(Patom, Q)
      res(i) = k.calculate()
    }
    val rdd = sc.parallelize(res)
    rdd
  }

  def matrix(df: Dataset[CRD]): SimpleMatrix = {
    val arr = df.collect()
    var mat = new SimpleMatrix(arr.length, 3)
    for (i <- 0 to arr.length - 1) {
      mat.set(i, 0, arr(i).X.toString.toDouble)
      mat.set(i, 1, arr(i).Y.toString.toDouble)
      mat.set(i, 2, arr(i).Z.toString.toDouble)
    }
    mat
  }

  def rmsdUtilRes(df: Dataset[PDB], resCnt:  Int,startFrame: Int, spark: SparkSession): RDD[Double] = {
    import spark.implicits._
    val sc = spark.sparkContext
    var dfarr = new Array[DataFrame](10)
    for (i <- 0 to 9) {
      val temp = df.where($"resCount"<=resCnt && $"frameNo" === startFrame + i).select($"X", $"Y", $"Z")
      dfarr(i) = temp
    }
    var res = new Array[Double](10)
    var pCentroid = new Array[Double](3)
    if(!pReadRes) {
      val p: Dataset[CRD] = dfarr(0).as[CRD]
      val temp = p.select(avg($"X"), avg($"Y"), avg($"Z")).collect()(0)
      pCentroid = temp.toString.stripPrefix("[").stripSuffix("]").split(",").map(x => x.toDouble)
      val Ptemp = p.map(row => CRD(row.X - pCentroid(0), row.Y - pCentroid(1), row.Z - pCentroid(2)))
      Pres = matrix(Ptemp)
      Pres = Pres.transpose()
      pReadRes = true
      res(0) = 0.0
      for (i <- 1 to dfarr.length - 1) {
        val q = dfarr(i).as[CRD]
        val temp = q.select(avg($"X"), avg($"Y"), avg($"Z")).collect()(0)
        val qCentroid = temp.toString.stripPrefix("[").stripSuffix("]").split(",").map(x => x.toDouble)
        val Qtemp :Dataset[CRD] = q.map(row => CRD(row.X - qCentroid(0), row.Y - qCentroid(1), row.Z - qCentroid(2)))
        var Q = matrix(Qtemp)
        Q = Q.transpose()
        val k = new Kabsch(Pres, Q)
        res(i) = k.calculate()
      }
    }

    for (i <- 0 to dfarr.length - 1) {
      val q = dfarr(i).as[CRD]
      val temp = q.select(avg($"X"), avg($"Y"), avg($"Z")).collect()(0)
      val qCentroid = temp.toString.stripPrefix("[").stripSuffix("]").split(",").map(x => x.toDouble)
      val Qtemp :Dataset[CRD] = q.map(row => CRD(row.X - qCentroid(0), row.Y - qCentroid(1), row.Z - qCentroid(2)))
      var Q = matrix(Qtemp)
      Q = Q.transpose()
      val k = new Kabsch(Pres, Q)
      res(i) = k.calculate()
    }
    val rdd = sc.parallelize(res)
    rdd
  }
}
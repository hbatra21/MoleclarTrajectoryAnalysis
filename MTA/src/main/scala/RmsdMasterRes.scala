package api

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object RmsdMasterRes {

  def getListOfFiles(dir: String, spark: SparkSession): Array[String] = {
    val sc = spark.sparkContext
    var fileList = new scala.collection.mutable.ArrayBuffer[String]()
    val config = new Configuration()
    val path = new Path(dir)
    val fd = FileSystem.get(config).listStatus(path)
    fd.foreach(x => {
      fileList += x.getPath.toString()
    })
    return fileList.toArray
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.getOrCreate()
    val hconf = spark.sparkContext.hadoopConfiguration
    hconf.setInt("dfs.replication", 1)
    val sc = spark.sparkContext
    val r = new Read2()
    val crdFileDir = args(1)
    val crdFiles = getListOfFiles(crdFileDir,spark).sorted
    val prmTopFile = args(0)
    val outDir = args(2)
    val time = System.currentTimeMillis()
    val arr = r.read_pointers(prmTopFile, spark)
    var dfArr = new Array[Dataset[PDB]](crdFiles.length)
    for (i <- 0 to crdFiles.length - 1) {
      val temp = r.read_prm(prmTopFile, arr(0), arr(1), crdFiles(i).toString, outDir, spark)
      dfArr(i) = temp.persist()
    }
    val rmsdRes = new RMSD
    var finishTime1 : Double = 0
    for (i <- 0 to dfArr.length - 1) {
      val startFrame = i*10 + 1
      val time1 = System.currentTimeMillis()
      val rdd1: RDD[Double] = rmsdRes.rmsdUtilRes(dfArr(i), 801, startFrame, spark)
      val finTime1 : Double = (System.currentTimeMillis()-time1)/1000
      finishTime1 = finishTime1 + finTime1
      rdd1.coalesce(1).saveAsTextFile(outDir+"/Rmsd_"+i)
    }
    val time1 = new Array[Double](1)
    time1(0) = (finishTime1)
    sc.parallelize(time1).coalesce(1).saveAsTextFile(outDir+"/RMSD_Res_time")
  }
}

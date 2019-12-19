package api

import java.io.File
import java.nio.file.Paths

import api.{PDB, Read1}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object RmsdMaster {
  def getListOfFiles1(dir: String): List[File] = {
    val path = Paths.get(dir)
    val file = path.toFile
    if (file.isDirectory) {
      return file.listFiles().toList
    } else {
      List[File]()
    }
  }
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
    var r = new Read2()
    val crd_file_dir = args(1)
    var crd_files = getListOfFiles(crd_file_dir,spark).sorted
    val prm_top_file = args(0)
    val out_dir = args(2)
    var time = System.currentTimeMillis()
    var arr = r.read_pointers(prm_top_file, spark)
    var dfArr = new Array[Dataset[PDB]](crd_files.length)
    for (i <- 0 to crd_files.length - 1) {
      val temp = r.read_prm(prm_top_file, arr(0), arr(1), crd_files(i).toString, out_dir, spark)
      dfArr(i) = temp.persist()
    }
    val rmsdRes = new RMSD
    var atom_name = "N"
    var finishTime : Double = 0
    var finishTime1 : Double = 0
    for (i <- 0 to dfArr.length - 1) {
      val startFrame = i*10 + 1
      /*val time = System.currentTimeMillis()
      val rdd: RDD[Double] = rmsdRes.rmsdUtilAtom(dfArr(i), atom_name, startFrame, spark)
      val finTime : Double = (System.currentTimeMillis()-time)/1000
      finishTime = finishTime + finTime*/
      val time1 = System.currentTimeMillis()
      val rdd1: RDD[Double] = rmsdRes.rmsdUtilRes(dfArr(i), 801, startFrame, spark)
      val finTime1 : Double = (System.currentTimeMillis()-time1)/1000
      finishTime1 = finishTime1 + finTime1
      rdd1.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/data/RMSD_Res_"+i)
    }
    var time1 = new Array[Double](2)
    time1(0) = (finishTime)
    time1(1) = (finishTime1)
    sc.parallelize(time1).coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/data/RMSD_Res_time")
  }
}

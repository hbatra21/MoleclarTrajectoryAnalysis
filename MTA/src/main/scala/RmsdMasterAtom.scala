package api

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object RmsdMasterAtom {

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
    val rmsdAtom = new RMSD
    var atomName =""
    try {
      atomName = args(3)
    }
    catch {
      case e: IllegalArgumentException =>
        println("Pleae enter valid atom name, Error: " + e.getMessage)
    }
    var finishTime : Double = 0

    for (i <- 0 to dfArr.length - 1) {
      val startFrame = i*10 + 1
      val time = System.currentTimeMillis()
      val rdd: RDD[Double] = rmsdAtom.rmsdUtilAtom(dfArr(i), atomName, startFrame, spark)
      val finTime : Double = (System.currentTimeMillis()-time)/1000
      finishTime = finishTime + finTime
      rdd.coalesce(1).saveAsTextFile(outDir+"/RmsdAtom_"+i)
    }
    var time1 = new Array[Double](1)
    time1(0) = (finishTime)
    sc.parallelize(time1).coalesce(1).saveAsTextFile(outDir+"/RMSD_Atom_time")
  }
}

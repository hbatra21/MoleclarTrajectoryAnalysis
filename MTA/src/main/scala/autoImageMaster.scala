package api

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

object autoImageMaster {
  def getListOfFiles(dir: String, spark: SparkSession): Array[String] = {
    var fileList = new scala.collection.mutable.ArrayBuffer[String]()
    val config = new Configuration()
    val path = new Path(dir)
    val fd = FileSystem.get(config).listStatus(path)
    fd.foreach(x => {
      fileList += x.getPath.toString()
    })
    fileList.toArray
  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.getOrCreate()
    val hconf = spark.sparkContext.hadoopConfiguration
    hconf.setInt("dfs.replication", 1)
    import spark.implicits._
    val sc = spark.sparkContext
    val r = new Read2()
    val prmFile = args(0)
    val crdFileDir = args(1)
    val outDir = args(2)
    val crdFiles = getListOfFiles(crdFileDir, spark).sorted
    val image = new Image(r.boxDim, spark)
    val arr = r.read_pointers(prmFile, spark)

    var finishTime : Double = 0.0
    for (i <- 0 to crdFiles.length - 1) {
      val temp = r.readPrmAuto(prmFile, arr(0), arr(1), crdFiles(i).toString, outDir,spark).par
      val time : Double = System.currentTimeMillis()
      val tempdf = temp.map(frame => image.autoImage(frame,r.firstMolCount))
      val finTime : Double = (System.currentTimeMillis()-time)/1000
      finishTime = finishTime + finTime
      temp.foreach(x => x.unpersist())
      for(j<-0 to 9) {
        r.genPdb(tempdf(j).collect(),r.totalAtoms,outDir+"/autoImage",spark)
      }
      tempdf.foreach(x => x.unpersist())
    }
    var time = new Array[Double](1)
    time(0) = (finishTime)
    sc.parallelize(time).coalesce(1).saveAsTextFile(outDir+"/AUTO_IMAGE_time")
  }
}

package api

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

object dihedralMaster {

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
    import spark.implicits._
    val sc = spark.sparkContext
    val r = new Read2()
    val prm_file = args(0)
    val crd_file_dir = args(1)
    val out_dir = args(2)
    var crd_files = getListOfFiles(crd_file_dir, spark).sorted

    //var dfArr = new Array[Array[Dataset[PDB]]](crd_files.length)
    val di = new Dihedral()
    val arr = r.read_pointers(prm_file, spark)

    var finishTime: Double = 0
    for (i <- 0 to crd_files.length - 1) {
      val temp = r.readPrmDih(prm_file, arr(0), arr(1), crd_files(i).toString, out_dir, spark)

      val time = System.currentTimeMillis()
      //temp.map(frame => di.dihedralAngle(frame))
      var dihedralFrame = scala.collection.mutable.ListBuffer[String]()
      var frameNo = i*10+1
      for(frame <- temp)
        {
          val x = di.dihedralAngle(frame)
          dihedralFrame += "Frame"+frameNo+": " +x
          frameNo += 1
        }
      sc.parallelize(dihedralFrame).coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/par/dihedral_angles_"+i)

      val finTime: Double = (System.currentTimeMillis() - time) / 1000
      finishTime = finishTime + finTime
      //tempdf.map(frame => r.genPdb(frame.collect(),r.totalAtoms,out_dir + "/autoImagePdb",spark))
    }


    /*for (i <- 0 to crd_files.length - 1) {
      for(j<-10*i + 1 to 10*i + 10) {
        image.autoImage(dfArr(i),r.firstMolCount)
        //r.genPdb(j, df.collect(), arr(0), out_dir, spark)
      }
    }*/
    var time = new Array[Double](1)
    time(0) = (finishTime)
    sc.parallelize(time).coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/par/Dihedral_time")
  }
}


















































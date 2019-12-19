package api
import api.{Distance, Mask, PDB, Read}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

object distanceMaster {
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
    var r = new Read2()
    val prm_file = args(0)
    val crd_file_dir = args(1)
    val out_dir = args(2)
    var crd_files = getListOfFiles(crd_file_dir, spark).sorted
    var mask = new Mask()
    var dfArr = new Array[Dataset[PDB]](crd_files.length)

    var arr = r.read_pointers(prm_file, spark)
    var distance = new Distance()

    for (i <- 0 to crd_files.length - 1)
    {
      var temp = r.read_prm(prm_file, arr(0), arr(1), crd_files(i).toString, out_dir, spark,false)
      //temp = mask.stripWater(temp)
      dfArr(i) = temp
      dfArr(i).persist()
    }

    var start = System.currentTimeMillis()
    for (i <- 0 to crd_files.length - 1) {
      for(j<-10*i + 1 to 10*i + 10) {
        distance.find_dist(1,11,dfArr(i).where($"frameNo" === j))
      }
    }

    var time = new Array[Double](1)
    time(0) = (System.currentTimeMillis().toDouble - start.toDouble) / 1000
    sc.parallelize(time).coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/distance/distance_time")

  }
}

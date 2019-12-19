package api
import api.{Image, Mask, PDB, Read}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

object avgMaster {
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
    var arr = r.read_pointers(prm_file, spark)
    //var dfArr = new Array[Dataset[PDB]](crd_files.length)
    var df = r.read_prm(prm_file, arr(0), arr(1), crd_files(0).toString, out_dir, spark, false)
    var image = new Image(r.boxDim, spark)

    for (i <- 1 to crd_files.length - 1)
    {
      var temp_df = r.read_prm(prm_file, arr(0), arr(1), crd_files(i).toString, out_dir, spark, false)
      df = df.union(temp_df)
    }
    df.persist()
    var start = System.currentTimeMillis()
    var avg = image.avgStructure(df)
    var time = new Array[Double](1)
    time(0) = (System.currentTimeMillis().toDouble - start.toDouble) / 1000
    sc.parallelize(time).coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/avg/avg_str_time")
    avg.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/avg/avg_str")
  }
}

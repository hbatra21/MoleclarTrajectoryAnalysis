package api
import api.{Image, Mask, PDB, Read}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

object superImposeMaster {
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
    var r = new Read()
    val prm_file = args(0)
    val crd_file_dir = args(1)
    val out_dir = args(2)
    var crd_files = getListOfFiles(crd_file_dir, spark).sorted
    var mask = new Mask()
    var dfArr = new Array[Dataset[PDB]](crd_files.length)

    var arr = r.read_pointers(prm_file, spark)
    var image = new Image(r.boxDim, spark)

    for (i <- 0 to crd_files.length - 1)
    {
      var temp = r.read_prm(prm_file, arr(0), arr(1), crd_files(i).toString, out_dir, (i.toInt * 10)+1, spark)
      dfArr(i) = temp
      dfArr(i).persist()
    }

    var df1 = image.autoImage(dfArr(0),r.firstMolCount)
    for (j <- 2 to 10 )
    {
      var temp = image.autoImage(dfArr(0),r.firstMolCount)
      df1 = df1.union(temp)
    }
    for (i <- 1 to dfArr.length - 1)
    {
      var currFrame = dfArr(i)
      for (j <- 1 to 10 )
      {
        var frame:Int = i*10 + j
        var temp = image.autoImage(currFrame,r.firstMolCount)
        df1 = df1.union(temp)
      }
    }
    df1 = mask.stripWater(df1)
    var start = System.currentTimeMillis()
    image.superImpose(df1, spark)
    var time = new Array[Double](1)
    time(0) = (System.currentTimeMillis().toDouble - start.toDouble) / 1000
    sc.parallelize(time).coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/superImpose_time")
    //df1.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/superImpose")

  }
}

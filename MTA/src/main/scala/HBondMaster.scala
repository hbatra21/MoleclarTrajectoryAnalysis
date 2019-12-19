package api
import api._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}


object HBondMaster {

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
    crd_files.foreach(println)
    var mask = new Mask()
    var dfArr = new Array[Dataset[PDB]](crd_files.length*10)

    var arr = r.read_pointers(prm_file, spark)
    var image = new Image(r.boxDim, spark)

    for (i <- 0 to crd_files.length - 1)
    {
      //dfArr= r.readPrmAuto(prm_file,arr(0),arr(1),crd_files(i).toString,out_dir,spark)
      dfArr(i) = r.read_prm(prm_file, arr(0), arr(1), crd_files(i).toString, out_dir, spark, false)
      //println(dfArr(i).count())
      dfArr(i).persist()
   dfArr(i).count()
    }

  //  dfArr(0).count()
    //dfArr(0).persist()

    /*var df = r.read_prm(prm_file, arr(0), arr(1), crd_files(0).toString, out_dir, spark)
    //var image = new Image(r.boxDim, spark)

    for (i <- 1 to crd_files.length - 1)
    {
      var temp_df = r.read_prm(prm_file, arr(0), arr(1), crd_files(i).toString, out_dir, spark,false)
      df = df.union(temp_df)
    }*/


    //dfArr(0).toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/Hbond/df")
    //var ndf = r.readPrmAuto(prm_file, arr(0), arr(1), crd_files(0).toString, spark)
//    val m = new Mask()
    //val df = m.stripWater(ndf)
    //var df = ndf.where(!$"resLabel".equalTo("WAT"))*/

    //dfArr(0).count()
/*
    val df1 = dfArr(0).where($"frameNo"===1)
    df1.count()
    var st = System.currentTimeMillis()
    var ang = new Angle()
    ang.findAngle(df1,32,43,122)
    var rt = System.currentTimeMillis()
    var time = new Array[Double](1)
    time(0) = (rt-st).toDouble/1000
    sc.parallelize(time).coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/Hbond/Hbond_timeAng1")


 */

    var tf = dfArr.length*10
    var st = System.currentTimeMillis()
    var hbond = new Hydrogen_Bond()
    hbond.H_bond(dfArr,tf,spark)
    var rt = System.currentTimeMillis()
    var time = new Array[Double](1)
    time(0) = (rt-st).toDouble/1000
    sc.parallelize(time).coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/Hbond/Hbond_1000")

    //println("total hband time " + ((rt - st).toDouble / 1000.0))*/
  }

}
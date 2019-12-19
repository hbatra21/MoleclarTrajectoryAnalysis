package api
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import akka.actor.ActorSystem

import scala.concurrent.{Await, ExecutionContext, Future}
import java.io.File

import akka.stream.{ActorMaterializer, KillSwitches, UniqueKillSwitch}
import org.apache.log4j.{Level, Logger}
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
object pdb_df {
  //case class PDB(frameNo:Int, index:Int, atom:String, resLabel:String, resCount:Int,X:Double, Y:Double,Z:Double)
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

   def getListOfFiles1(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def main(args: Array[String]): Unit = {

    val time = System.currentTimeMillis()
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
//    val conf = new SparkConf().setAppName("MTA").set("spark.files.overwrite","true").setMaster("local")
//    val sc1 = new SparkContext(conf)
    val spark = SparkSession.builder.getOrCreate()
    val hconf = spark.sparkContext.hadoopConfiguration
    hconf.setInt("dfs.replication", 1)
    import spark.implicits._
    val sc = spark.sparkContext
    var r = new Read2()
    val prm_file = args(0)
    val crd_file_dir = args(1)
    val out_dir = args(2)
    //println(crd_file_dir)

    var crd_files = getListOfFiles(crd_file_dir,spark).sorted.par

    var dfArr = new Array[Dataset[PDB]](crd_files.length)
    //var df1 : Dataset[PDB]= null
    var count: Int = 0
    var image = new Image(r.boxDim, spark)
    var arr = r.read_pointers(prm_file, spark)
/*
    implicit val system = ActorSystem("read-parallel")
    implicit val materializer = ActorMaterializer()
    implicit val ec: ExecutionContext = ActorSystem().dispatcher

    val sources = crd_files.iterator
    val source = Source.fromIterator[String](() => sources)
    source.mapAsync(10) { value => //-> It will run in parallel 5 reads
      Future {
        println(value)
        var temp = r.read_prm(prm_file, arr(0), arr(1), value.toString, out_dir, spark)
        r.genPdb(temp.collect(), arr(0), out_dir, spark, value.toString)
        Thread.sleep(0)
       // println(s"Process in Thread:${Thread.currentThread().getName}")
        value
      }
    }
      .runWith(Sink.ignore)
      .onComplete( _ => {
        //println("finished")
        system.terminate()
      })

*/

    //val df = r.read_prm(prm_file, arr(0), arr(1), crd_file_dir, out_dir, 1, spark)
    //df.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/df")
    //dfArr(0) = r.read_prm(prm_file, arr(0), arr(1), crd_files(0), out_dir, 1, spark)
    //var df = r.read_prm(prm_file, arr(0), arr(1), crd_files(0), out_dir, 1, spark)
    //temp_df.repartition($"frameNo")
    //temp_df.toJavaRDD.saveAsTextFile("hdfs:///user/ppr.gp2/out/df")
  //  var mask = new Mask()

    /*var crd_files_rdd = sc.parallelize(crd_files,10)
    crd_files_rdd.collect().foreach(println)
    var ds = crd_files_rdd.mapPartitions(x => r.read_prm(prm_file, arr(0), arr(1), x.toString, out_dir, spark))*/
    //ds.foreach(x => println(x.toString()))
    //ds.foreach(x => x.toJavaRDD.coalesce(10).saveAsTextFile("hdfs:///user/ppr.gp2/"+out_dir+"/"+x.toString()))

      /*var temp = crd_files.map(x => r.read_prm(prm_file, arr(0), arr(1), x.toString, out_dir, spark))
      temp.map(x => x.coa)*/
// temp.foreach(x => r.genPdb(x.collect(),arr(0),out_dir,spark))
    //dfArr(0).toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/df_first")
    //dfArr(crd_files.length-1).toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/df_last")
    //df.repartition(102)
    //df.persist()

    /*var img = new Image(r.boxDim, spark)
    val avg_str = img.avgStructure(df)
    avg_str.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/avg")*/


    //df.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/df-5000_frames")
    //df.persist()


  }
}
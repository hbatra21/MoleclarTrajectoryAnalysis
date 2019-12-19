package api

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import java.io.File
import java.nio.file.Paths
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._

object autoImageTest {
  def getListOfFiles(dir: String): List[File] = {
    val path = Paths.get(dir)
    val file = path.toFile
    if (file.isDirectory) {
      return file.listFiles().toList
    } else {
      return List[File]()
    }
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("MTA").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
    val r = new Read1()
    val crd_file = args(1)
    var crd_files = getListOfFiles(crd_file).sorted
    crd_files.foreach(println)
    val prm_top_file = args(0)
    val out_dir = args(2)
    var arr = r.read_pointers(prm_top_file, sc)
    var image = new Image(r.boxDim, spark)
    var dfArr = new Array[Array[Dataset[PDB]]](crd_files.length)
    for (i <- 0 to crd_files.length - 1) {
      var temp = r.readPrmAuto(prm_top_file, arr(0), arr(1), crd_files(i).toString, out_dir,sc)
      dfArr(i) = temp
    }
    var start = System.currentTimeMillis()
    var count = 0
    var df = dfArr.map(tenFrame => tenFrame.map(frame => image.autoImage(frame,r.firstMolCount)))
    var cnt = 0
    //df.foreach(tenFrame => tenFrame.foreach(frame => frame.collect().foreach(println)))
    var time = (System.currentTimeMillis().toDouble - start.toDouble) / 1000
    println(time)
  }
}

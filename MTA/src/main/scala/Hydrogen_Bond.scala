package api
import api._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, lit, _}

class Hydrogen_Bond extends Serializable {


  def H_result(df_frame:Dataset[PDB],h_d:DataFrame,n:Int) = {

    val spark = SparkSession.builder.getOrCreate()
    //val sc = spark.sparkContext
    import spark.implicits._

    df_frame.persist()
    var df_o_n_h = df_frame.filter(x => {
      (x.atom.startsWith("N") || x.atom.startsWith("O") || x.atom.startsWith("H"))
    })
df_o_n_h.persist()
    var ac = df_o_n_h.filter(x => {
      (x.atom.startsWith("N") || x.atom.startsWith("O"))
    })
    ac.persist()
    var ac_D = ac.withColumnRenamed("Index", "donerI").withColumnRenamed("X", "XD").withColumnRenamed("Y", "YD").withColumnRenamed("Z", "ZD").withColumnRenamed("atom", "atomD").withColumnRenamed("resLabel", "resLabelD").withColumnRenamed("resCount", "resCountD")
    var ac_I = ac.withColumnRenamed("Index", "ac_I").withColumnRenamed("X", "XA").withColumnRenamed("Y", "YA").withColumnRenamed("Z", "ZA").withColumnRenamed("atom", "atomA").withColumnRenamed("resLabel", "resLabelA").withColumnRenamed("resCount", "resCountA")
    var h_d_doner = df_o_n_h.join(h_d, "Index")
    var h_d_total = h_d_doner.join(ac_D, "donerI")
    var cross_h_d = h_d_total.crossJoin(ac_I)
    import org.apache.spark.sql.functions.udf
    val dis = udf[Double, Double, Double, Double, Double, Double, Double](Hbond_Dis)
    val ang = udf[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double](angle)

    val Dis_cross_h_d = cross_h_d.withColumn("Distance", dis($"XA", $"YA", $"ZA", $"XD", $"YD", $"ZD")).where($"Distance" <= 3.5).where($"Distance" >= 1.5)
    val DA_cross_h_d = Dis_cross_h_d.withColumn("Angle", ang($"X", $"Y", $"Z", $"XA", $"YA", $"ZA", $"XD", $"YD", $"ZD")).where($"Angle" >= 120)
    val f_DA = DA_cross_h_d.withColumn("H_BOND", (concat($"resLabelA", lit("_"), $"resCountA", lit("@"), $"atomA", lit("-"), $"resLabelD", lit("_"), $"resCountD", lit("@"), $"atomD")))
    var H_bond_data = f_DA.select(f_DA.col("H_BOND"), f_DA.col("Distance"), f_DA.col("Angle"))
    //println(H_bond_data.count())
    //  Hydro_set(n-1) = H_bond_data.select($"H_BOND").collect().toSet
    //All_Hydro_set =  All_Hydro_set.union(Hydro_set(n-1))
    var path = "hdfs:///user/ppr.gp2/Hbond/H_bond_data_file_"+n+".csv"
     H_bond_data.coalesce(1).write.mode("overwrite").format("csv").save(path)

//    var path = "H_bond_data_file_"+n+".csv"
    //H_bond_data.repartition(1).write.mode("overwrite").format("csv").save(path)

  }



  def Hbond_Dis(xa:Double,ya:Double,za:Double,xd:Double,yd:Double,zd:Double): Double = {
    math.sqrt(math.pow(xa - xd, 2)+math.pow(ya - yd, 2)+math.pow(za - zd, 2))
  }

  def angle(x: Double, y: Double, z: Double, xA: Double, yA: Double, zA: Double, xD: Double, yD: Double, zD: Double): Double = {
    scala.math.acos((scala.math.pow(Hbond_Dis(x, y, z, xA, yA, zA), 2) + scala.math.pow(Hbond_Dis(x, y, z, xD, yD, zD), 2) - scala.math.pow(Hbond_Dis(xA, yA, zA, xD, yD, zD), 2)) / (2 * Hbond_Dis(x, y, z, xA, yA, zA) * Hbond_Dis(x, y, z, xD, yD, zD))).toDegrees
  }

  def H_bond(df:Array[Dataset[PDB]],Total_Frame:Int, spark:SparkSession) ={
    import org.apache.spark.sql.expressions.Window
    //val sc = spark.sparkContext
    import spark.implicits._
    var st = System.currentTimeMillis()
    var rt = System.currentTimeMillis()

    //val df1 = df(0)(0)

    val df1 = df(0).where($"frameNo" === 1)
    //df(0).persist()

    df1.persist()
    val df2 = df1.where(!$"resLabel".equalTo("PRO"))

    val f3 = df2.withColumn("atomName", substring(col("atom"), 1, 1)).withColumn("index", df1("index").cast(sql.types.IntegerType))
    f3.persist()


    val w = Window.orderBy("index")
    val hydrogen1 = f3.withColumn("doner", lag($"atomName", 1).over(w)).withColumn("donerI", lag($"index", 1).over(w)).filter($"atomName".equalTo("H")).filter($"doner".isin("N", "O"))
    val hydrogen2 = f3.withColumn("doner", lag($"atomName", 2).over(w)).withColumn("donerI", lag($"index", 2).over(w)).filter($"atomName".equalTo("H")).filter($"doner".isin("N")).filter($"atom".endsWith("2"))
    val hydrogen3 = f3.withColumn("doner", lag($"atomName", 3).over(w)).withColumn("donerI", lag($"index", 3).over(w)).filter($"atomName".equalTo("H")).filter($"doner".isin("N")).filter($"atom".endsWith("3"))
    val hydrogenex = f3.filter($"resLabel".equalTo("U5")).filter($"atom".isin("O5'", "HO5'")).withColumn("doner", lead($"atomName", 1).over(w)).withColumn("donerI", lead($"index", 1).over(w)).filter($"atomName".equalTo("H"))
    val hydrogen = hydrogen1.union(hydrogen2).union(hydrogen3).union(hydrogenex)
    /*
    println("doner 1 " + hydrogen1.count())
    println("doner 2 " + hydrogen2.count())
    println("doner 3 " + hydrogen3.count())
    println("doner 4 " + hydrogenex.count())
    println("total " + hydrogen.count())

     */
    //println(ac.length)
    val h_d = hydrogen.select("Index", "donerI")
    //h_d.show(1000)
    //h_d.count()
    h_d.persist()
    df1.unpersist()
   // var Total_frames = Total_Frame
    st = System.currentTimeMillis()
    var i = List.range(1, Total_Frame)

    i.foreach { n =>

      var df_frame= df((n-1)/10).where($"frameNo" === n)

      H_result(df_frame,h_d,n)

      /*
      //val df_frame = df((n-1)/10)((n-1)%10)
      df_frame.persist()
      val df_o_n_h = df_frame.filter(x => {
        (x.atom.startsWith("N") || x.atom.startsWith("O") || x.atom.startsWith("H"))
      })

      val ac = df_o_n_h.filter(x => {
        (x.atom.startsWith("N") || x.atom.startsWith("O"))
      })
      
      val h_d_doner = df_o_n_h.join(h_d, "Index")
      val ac_D = ac.withColumnRenamed("Index", "donerI").withColumnRenamed("X", "XD").withColumnRenamed("Y", "YD").withColumnRenamed("Z", "ZD").withColumnRenamed("atom", "atomD").withColumnRenamed("resLabel", "resLabelD").withColumnRenamed("resCount", "resCountD")
      val ac_I = ac.withColumnRenamed("Index", "ac_I").withColumnRenamed("X", "XA").withColumnRenamed("Y", "YA").withColumnRenamed("Z", "ZA").withColumnRenamed("atom", "atomA").withColumnRenamed("resLabel", "resLabelA").withColumnRenamed("resCount", "resCountA")
      val h_d_total = h_d_doner.join(ac_D, "donerI")
      val cross_h_d = h_d_total.crossJoin(ac_I)
      import org.apache.spark.sql.functions.udf
      val dis = udf[Double, Double, Double, Double, Double, Double, Double](Hbond_Dis)
      val ang = udf[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double](angle)
      val Dis_cross_h_d = cross_h_d.withColumn("Distance", dis($"XA", $"YA", $"ZA", $"XD", $"YD", $"ZD")).where($"Distance" <= 3.5).where($"Distance" >= 1.5)
      val DA_cross_h_d = Dis_cross_h_d.withColumn("Angle", ang($"X", $"Y", $"Z", $"XA", $"YA", $"ZA", $"XD", $"YD", $"ZD")).where($"Angle" >= 120)
      val f_DA = DA_cross_h_d.withColumn("H_BOND", (concat($"resLabelA", lit("_"), $"resCountA", lit("@"), $"atomA", lit("-"), $"resLabelD", lit("_"), $"resCountD", lit("@"), $"atomD")))
      val H_bond_data = f_DA.select(f_DA.col("H_BOND"), f_DA.col("Distance"), f_DA.col("Angle"))
      println(H_bond_data.count())
      df_frame.unpersist()
      //var path = "hdfs:///user/ppr.gp2/Hbond/H_bond_data_file_"+n+".csv"
     // H_bond_data.coalesce(1).write.mode("overwrite").format("csv").save(path)

       */
    }
    rt = System.currentTimeMillis()
    println("total hband  time" + ((rt - st).toDouble / 1000.0))
  }

}
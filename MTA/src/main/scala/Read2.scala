package api

import java.io.{File, PrintWriter}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession, Row}
import ucar.nc2.NetcdfFile

import scala.util.control.Breaks.{break, breakable}


class Read2 extends java.io.Serializable {

  var boxDim = new Array[Double](3)
  var firstMolCount : Int = 0
  var totalMolecules: Int = 0
  var totalAtoms:Int = 0
  var atPerMol = new Array[Int](1)
  var name_df:Dataset[Row] = null
  var at_per_mol = new Array[Int](totalMolecules)
  var name_arr:Array[String] = null
  var prm_read_flag = false

  def mapper(line:String): PDB=
  {
    val fields = line.split(',')

    val pdb:PDB = PDB(fields(0).toInt, fields(1).toInt, fields(2), fields(3), fields(4).toInt, fields(5).toDouble,fields(6).toDouble,fields(7).toDouble)
    return pdb
  }

  def read_crd(crd_file:String, total_atoms:Int):ucar.ma2.Array = {
    val config = new Configuration()
    val path = new Path(crd_file)
    val fd = FileSystem.get(path.toUri(),config)
    val temp_data_file = File.createTempFile(path.getName(), "")
    temp_data_file.deleteOnExit()
    fd.copyToLocalFile(path, new Path(temp_data_file.getAbsolutePath()))
    var ncfile = NetcdfFile.open(temp_data_file.getAbsolutePath())
    var atom_cnt = ncfile.getDimensions().get(2).toString().split("=")(1).replaceAll(" +","").stripSuffix(";").toInt
    if(atom_cnt != total_atoms)
    {
      println("Any one of the 2 input files are incorrect")
      System.exit(1)
    }
    var crds_temp = ncfile.readSection("coordinates")
    return crds_temp
  }

  def read_crd1(crd_file:String, total_atoms:Int):ucar.ma2.Array = {
    val ncfile = NetcdfFile.open(crd_file)
    var atom_cnt = ncfile.getDimensions().get(2).toString().split("=")(1).replaceAll(" +","").stripSuffix(";").toInt
    if(atom_cnt != total_atoms)
    {
      println("Any one of the 2 input files are incorrect")
      System.exit(1)
    }
    var crds_temp = ncfile.readSection("coordinates")
    return crds_temp
  }

  def read_pointers(prm_top_file:String,spark : SparkSession):Array[Int]=
  {
    var pointer_flag = 0
    var arr:Array[Int] =new Array[Int](2)
    val sc : SparkContext = spark.sparkContext
    breakable
    {
      var c1 = 0
      var atom_flag = 0
      var mol_flag = 0
      val lines = sc.textFile(prm_top_file).toLocalIterator
      for (line <- lines){

        if(pointer_flag == 0 && line.contains("%FLAG POINTERS"))
          pointer_flag = 1

        else if(pointer_flag == 1)
        {
          if(atom_flag == 0 && mol_flag ==0 && line.length > 1 && line(0) != '%')
          {
            arr(0) = line.substring(2,8).replaceAll(" +","").toInt
            totalAtoms = arr(0)
            atom_flag = 1
          }

          else if(atom_flag == 1 && mol_flag ==0 && line.length > 1 && line(0) != '%')
          {
            arr(1) = line.substring(11,16).replaceAll(" +","").toInt
            totalMolecules = arr(1)
            return arr
          }
        }
      }
    }
    return arr

  }

  def read_prm(prm_top_file:String,  total_atoms:Int, total_molecules:Int, crd_file:String, out_dir:String, spark: SparkSession, include_water:Boolean = true) : Dataset[PDB] =
  {
    val sc : SparkContext = spark.sparkContext
    val start_frame = crd_file.split("/").last.split("_")(1).split('.')(1).toInt + 1


    if(!prm_read_flag) {
      var atom_flag = 0
      var mol_name_flag = 0
      var mol_pointer_flag = 0
      var at_per_mol_flag = 0
      var box_dim_flag = 0
      var at_mass_flag = 0
      var atoms = new Array[String](total_atoms)
      var mols = new Array[String](total_molecules)
      var mol_pointer = new Array[Int](total_molecules)
      at_per_mol = new Array[Int](total_molecules)
      var box_dim = new Array[Double](3)
      var at_mass = new Array[Double](total_atoms)
      var atoms_cnt = 0
      var mol_name_cnt = 0
      var mol_pointer_cnt = 0
      var at_per_mol_cnt = 0
      var pointer_flag = 0
      var at_mass_cnt = 0

      breakable {
        var c1 = 0
        val lines = sc.textFile(prm_top_file).toLocalIterator
        for (line <- lines) {
          if (atom_flag == 0 && line.contains("%FLAG ATOM_NAME")) {
            atom_flag = 1
          }

          else if (atom_flag == 1) {
            if (!line.contains("%")) {
              var n = line.length
              for (i <- 0 to 19) {
                if (i * 4 < n) {
                  var x = line.substring(i * 4, i * 4 + 3).replaceAll(" +", "")
                  //x = StringUtils.center(x, 4)
                  atoms(atoms_cnt) = x
                  atoms_cnt += 1
                }
              }
            }
          }

          if (atom_flag == 1 && line.contains("%FLAG CHARGE")) {
            atom_flag = 2
          }

          if (at_mass_flag == 0 && line.contains("%FLAG MASS"))
            at_mass_flag = 1

          if (at_mass_flag == 1) {
            if (!line.contains("%")) {
              for (i <- 0 to 4) {
                at_mass(at_mass_cnt) = line.substring(i * 16 + 2, i * 16 + 16).toDouble
                at_mass_cnt += 1
              }
            }
          }

          if (at_mass_flag == 1 && line.contains("%FLAG ATOM_TYPE_INDEX"))
            at_mass_flag = 2

          if (mol_name_flag == 0 && line.contains("%FLAG RESIDUE_LABEL")) {
            mol_name_flag = 1
          }

          else if (mol_name_flag == 1) {
            if (!line.contains("%")) {
              var n = line.length
              for (i <- 0 to 19) {
                if (i * 4 < n) {
                  var x = line.substring(i * 4, i * 4 + 3).replaceAll(" +", "")
                  mols(mol_name_cnt) = x
                  mol_name_cnt += 1
                }
              }
            }
          }

          if (mol_name_flag == 1 && line.contains("%FLAG RESIDUE_POINTER")) {
            mol_name_flag = 2
            mol_pointer_flag = 1
          }

          else if (mol_pointer_flag == 1) {
            if (!line.contains("%")) {
              var n = line.length
              for (i <- 0 to 9) {
                if (i * 8 < n) {
                  var x = line.substring(i * 8, i * 8 + 8).replaceAll(" +", "")
                  mol_pointer(mol_pointer_cnt) = x.toInt
                  mol_pointer_cnt += 1
                }
              }

            }
          }

          if (mol_pointer_flag == 1 && line.contains("%FLAG BOND_FORCE_CONSTANT")) {
            mol_pointer_flag = 2;

          }

          if (at_per_mol_flag == 0 && line.contains("%FLAG ATOMS_PER_MOLECULE")) {
            at_per_mol_flag = 1
            1
          }
          else if (at_per_mol_flag == 1) {
            if (!line.contains("%")) {
              var n = line.length
              for (i <- 0 to 9) {
                if (i * 8 < n) {
                  var x = line.substring(i * 8, i * 8 + 8).replaceAll(" +", "")
                  if (at_per_mol_cnt != 0)
                    at_per_mol(at_per_mol_cnt) = x.toInt + at_per_mol(at_per_mol_cnt - 1)
                  else
                    at_per_mol(at_per_mol_cnt) = x.toInt
                  at_per_mol_cnt += 1
                }
              }
            }
          }

          if (at_per_mol_flag == 1 && line.contains("%FLAG BOX_DIMENSIONS")) {
            at_per_mol_flag = 2
            box_dim_flag = 1
          }

          if (box_dim_flag == 1) {
            if (line(0) != '%') {
              for (i <- 1 to 3) {
                boxDim(i - 1) = line.substring(i * 16 + 2, i * 16 + 16).replaceAll(" +", "").toDouble
              }
              box_dim_flag = 2
              break
            }
          }

        }
      }

      for (i <- 0 to total_molecules - 2) {
        mol_pointer(i) = mol_pointer(i + 1) - mol_pointer(i)
      }
      mol_pointer(total_molecules - 1) = total_atoms - mol_pointer(total_molecules - 1) + 1


      var final_mols = new Array[String](total_atoms)
      var final_mol_cnt = new Array[Int](total_atoms)
      var cnt = 0
      for (i <- 0 to total_molecules - 1) {
        var n_times = mol_pointer(i)
        for (j <- 0 to n_times - 1) {
          final_mols(cnt) = mols(i)
          final_mol_cnt(cnt) = i + 1
          cnt += 1
        }
      }
      val s_final_mol_cnt = final_mol_cnt.map(_.toString)
      var nmArr = new Array[String](total_atoms)
      for (i <- 0 to total_atoms - 1) {
        nmArr(i) = start_frame + "," + (i + 1) + "," + atoms(i) + "," + final_mols(i) + "," + s_final_mol_cnt(i)
      }
      name_arr = nmArr
      prm_read_flag = true
      atPerMol = at_per_mol
    }

    firstMolCount = at_per_mol(0)
    var crds = read_crd(crd_file, total_atoms)
    var X = Array.ofDim[Float](10,total_atoms)
    var Y = Array.ofDim[Float](10,total_atoms)
    var Z = Array.ofDim[Float](10,total_atoms)

    var cnt = 0

    for(i <- 0 to 9) {
      for(j <- 0 to ((total_atoms-1))) {
        X(i)(j) = crds.getFloat(cnt)
        cnt += 1
        Y(i)(j) = crds.getFloat(cnt)
        cnt += 1
        Z(i)(j) = crds.getFloat(cnt)
        cnt += 1
      }
    }



    var atom = "ATOM"
    var mol: String = null

    import spark.implicits._

    val crdArr = Array(X(0), Y(0), Z(0)).transpose
    val crdRdd = sc.parallelize(crdArr,50).map { case (x) => x.mkString(",") }.zipWithIndex().map{case(a,b) => (b,a)}
    var temp_name_arr = modifyFrame(name_arr,start_frame)
    var name = sc.parallelize(temp_name_arr,50).zipWithIndex().map{case(a,b) => (b,a)}
    var temp = name.join(crdRdd).map{case(a,(b,c)) => (b.toString+","+c.toString)}
    //val temp1 = temp.map(row => row(1).toString +","+row(3).toString)
    //var finalRes :Dataset[PDB] = temp.map(mapper).toDS()
    //finalRes.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/df")
    val tempDs : Dataset[PDB] = temp.map(mapper).toDS()
    var finalRes :Dataset[PDB] =tempDs.filter(tempDs("resLabel").=!=("WAT"))
    for(i<- 1 to 9) {
      val crdArr1 = Array(X(i), Y(i), Z(i)).transpose
      val crdRdd1 = sc.parallelize(crdArr1,50).map { case (x) => x.mkString(",") }.zipWithIndex().map{case(a,b) => (b,a)}
      temp_name_arr = modifyFrame(name_arr,start_frame+i)
      name = sc.parallelize(temp_name_arr,50).zipWithIndex().map{case(a,b) => (b,a)}
      temp = name.join(crdRdd1).map{case(a,(b,c)) => (b.toString+","+c.toString)}
      val temp1 = temp.map(mapper).toDS()
      finalRes = finalRes.union(temp1.filter(temp1("resLabel").=!=("WAT")))
      //finalRes = finalRes.union(temp1)//.orderBy($"frameNo",$"index")
    }

    finalRes = finalRes.orderBy($"frameNo",$"index")
    println("Frames "+start_frame+" to "+(start_frame+10)+" frames read finished")
    var mask = new Mask()
    /*if(include_water)
      return finalRes*/
    //return  mask.stripWater(finalRes)
    return finalRes
  }

  def modifyFrame(arr : Array[String], currFrame: Int): Array[String] = {
    return arr.map(line => currFrame + line.stripPrefix("1"))
  }

  def readPrmAuto(prm_top_file:String,  total_atoms:Int, total_molecules:Int, crd_file:String, out_dir:String, spark: SparkSession) : Array[Dataset[PDB]] =
  {
    val sc = spark.sparkContext
    val start_frame = crd_file.split("/").last.split("_")(1).split('.')(1).toInt + 1


    if(!prm_read_flag) {
      var atom_flag = 0
      var mol_name_flag = 0
      var mol_pointer_flag = 0
      var at_per_mol_flag = 0
      var box_dim_flag = 0
      var at_mass_flag = 0
      var atoms = new Array[String](total_atoms)
      var mols = new Array[String](total_molecules)
      var mol_pointer = new Array[Int](total_molecules)
      at_per_mol = new Array[Int](total_molecules)
      var box_dim = new Array[Double](3)
      var at_mass = new Array[Double](total_atoms)
      var atoms_cnt = 0
      var mol_name_cnt = 0
      var mol_pointer_cnt = 0
      var at_per_mol_cnt = 0
      var pointer_flag = 0
      var at_mass_cnt = 0

      breakable {
        var c1 = 0
        val lines = sc.textFile(prm_top_file).toLocalIterator
        for (line <- lines) {
          if (atom_flag == 0 && line.contains("%FLAG ATOM_NAME")) {
            atom_flag = 1
          }

          else if (atom_flag == 1) {
            if (!line.contains("%")) {
              var n = line.length
              for (i <- 0 to 19) {
                if (i * 4 < n) {
                  var x = line.substring(i * 4, i * 4 + 4).replaceAll(" +", "")
                  //x = StringUtils.center(x, 4)
                  atoms(atoms_cnt) = x
                  atoms_cnt += 1
                }
              }
            }
          }

          if (atom_flag == 1 && line.contains("%FLAG CHARGE")) {
            atom_flag = 2
          }

          if (at_mass_flag == 0 && line.contains("%FLAG MASS"))
            at_mass_flag = 1

          if (at_mass_flag == 1) {
            if (!line.contains("%")) {
              for (i <- 0 to 4) {
                at_mass(at_mass_cnt) = line.substring(i * 16 + 2, i * 16 + 16).toDouble
                at_mass_cnt += 1
              }
            }
          }

          if (at_mass_flag == 1 && line.contains("%FLAG ATOM_TYPE_INDEX"))
            at_mass_flag = 2

          if (mol_name_flag == 0 && line.contains("%FLAG RESIDUE_LABEL")) {
            mol_name_flag = 1
          }

          else if (mol_name_flag == 1) {
            if (!line.contains("%")) {
              var n = line.length
              for (i <- 0 to 19) {
                if (i * 4 < n) {
                  var x = line.substring(i * 4, i * 4 + 3).replaceAll(" +", "")
                  mols(mol_name_cnt) = x
                  mol_name_cnt += 1
                }
              }
            }
          }

          if (mol_name_flag == 1 && line.contains("%FLAG RESIDUE_POINTER")) {
            mol_name_flag = 2
            mol_pointer_flag = 1
          }

          else if (mol_pointer_flag == 1) {
            if (!line.contains("%")) {
              var n = line.length
              for (i <- 0 to 9) {
                if (i * 8 < n) {
                  var x = line.substring(i * 8, i * 8 + 8).replaceAll(" +", "")
                  mol_pointer(mol_pointer_cnt) = x.toInt
                  mol_pointer_cnt += 1
                }
              }

            }
          }

          if (mol_pointer_flag == 1 && line.contains("%FLAG BOND_FORCE_CONSTANT")) {
            mol_pointer_flag = 2;

          }

          if (at_per_mol_flag == 0 && line.contains("%FLAG ATOMS_PER_MOLECULE")) {
            at_per_mol_flag = 1
            1
          }
          else if (at_per_mol_flag == 1) {
            if (!line.contains("%")) {
              var n = line.length
              for (i <- 0 to 9) {
                if (i * 8 < n) {
                  var x = line.substring(i * 8, i * 8 + 8).replaceAll(" +", "")
                  if (at_per_mol_cnt != 0)
                    at_per_mol(at_per_mol_cnt) = x.toInt + at_per_mol(at_per_mol_cnt - 1)
                  else
                    at_per_mol(at_per_mol_cnt) = x.toInt
                  at_per_mol_cnt += 1
                }
              }
            }
          }

          if (at_per_mol_flag == 1 && line.contains("%FLAG BOX_DIMENSIONS")) {
            at_per_mol_flag = 2
            box_dim_flag = 1
          }

          if (box_dim_flag == 1) {
            if (line(0) != '%') {
              for (i <- 1 to 3) {
                boxDim(i - 1) = line.substring(i * 16 + 2, i * 16 + 16).replaceAll(" +", "").toDouble
              }
              box_dim_flag = 2
              break
            }
          }

        }
      }

      for (i <- 0 to total_molecules - 2) {
        mol_pointer(i) = mol_pointer(i + 1) - mol_pointer(i)
      }
      mol_pointer(total_molecules - 1) = total_atoms - mol_pointer(total_molecules - 1) + 1


      //sc.parallelize(atoms).coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/atoms1")


      var final_mols = new Array[String](total_atoms)
      var final_mol_cnt = new Array[Int](total_atoms)
      var cnt = 0
      for (i <- 0 to total_molecules - 1) {
        var n_times = mol_pointer(i)
        for (j <- 0 to n_times - 1) {
          final_mols(cnt) = mols(i)
          final_mol_cnt(cnt) = i + 1
          cnt += 1
        }
      }
      val s_final_mol_cnt = final_mol_cnt.map(_.toString)
      var nmArr = new Array[String](total_atoms)
      for (i <- 0 to total_atoms - 1) {
        nmArr(i) = start_frame + "," + (i + 1) + "," + atoms(i) + "," + final_mols(i) + "," + s_final_mol_cnt(i)
      }
      name_arr = nmArr
      prm_read_flag = true
      atPerMol = at_per_mol
    }

    firstMolCount = at_per_mol(0)
    var crds = read_crd(crd_file, total_atoms)
    var X = Array.ofDim[Float](10,total_atoms)
    var Y = Array.ofDim[Float](10,total_atoms)
    var Z = Array.ofDim[Float](10,total_atoms)

    var cnt = 0

    for(i <- 0 to 9) {
      for(j <- 0 to ((total_atoms-1))) {
        X(i)(j) = crds.getFloat(cnt)
        cnt += 1
        Y(i)(j) = crds.getFloat(cnt)
        cnt += 1
        Z(i)(j) = crds.getFloat(cnt)
        cnt += 1
      }
    }



    var atom = "ATOM"
    var mol: String = null

    /*val conf = new SparkConf().setAppName("optimized_final").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.getOrCreate()*/

    import spark.implicits._

    var finalRes = new Array[Dataset[PDB]](10)
    val crdArr = Array(X(0), Y(0), Z(0)).transpose
    val crdRdd = sc.parallelize(crdArr,50).map { case (x) => x.mkString(",") }.zipWithIndex().map{case(a,b) => (b,a)}
    var temp_name_arr = modifyFrame(name_arr,start_frame)
    var name = sc.parallelize(temp_name_arr,50).zipWithIndex().map{case(a,b) => (b,a)}
    var temp = name.join(crdRdd).map{case(a,(b,c)) => (b.toString+","+c.toString)}
    //val temp1 = temp.map(row => row(1).toString +","+row(3).toString)
    var mask = new Mask()
    finalRes(0) = temp.map(mapper).toDS().orderBy($"index")
    finalRes(0).persist()
    //finalRes.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/df")

    for(i<- 1 to 9) {
      val crdArr1 = Array(X(i), Y(i), Z(i)).transpose
      val crdRdd1 = sc.parallelize(crdArr1,50).map { case (x) => x.mkString(",") }.zipWithIndex().map{case(a,b) => (b,a)}
      temp_name_arr = modifyFrame(name_arr,start_frame+i)
      name = sc.parallelize(temp_name_arr,50).zipWithIndex().map{case(a,b) => (b,a)}
      temp = name.join(crdRdd1).map{case(a,(b,c)) => (b.toString+","+c.toString)}
      val temp1 = temp.map(mapper).toDS()
      finalRes(i) = temp1.orderBy($"index")
      finalRes(i).persist()
    }
    return  finalRes
  }
  def readPrmDih(prm_top_file:String,  total_atoms:Int, total_molecules:Int, crd_file:String, out_dir:String, spark: SparkSession) : Array[Dataset[PDB]] =
  {
    val sc = spark.sparkContext
    val start_frame = crd_file.split("/").last.split("_")(1).split('.')(1).toInt + 1


    if(!prm_read_flag) {
      var atom_flag = 0
      var mol_name_flag = 0
      var mol_pointer_flag = 0
      var at_per_mol_flag = 0
      var box_dim_flag = 0
      var at_mass_flag = 0
      var atoms = new Array[String](total_atoms)
      var mols = new Array[String](total_molecules)
      var mol_pointer = new Array[Int](total_molecules)
      at_per_mol = new Array[Int](total_molecules)
      var box_dim = new Array[Double](3)
      var at_mass = new Array[Double](total_atoms)
      var atoms_cnt = 0
      var mol_name_cnt = 0
      var mol_pointer_cnt = 0
      var at_per_mol_cnt = 0
      var pointer_flag = 0
      var at_mass_cnt = 0

      breakable {
        var c1 = 0
        val lines = sc.textFile(prm_top_file).toLocalIterator
        for (line <- lines) {
          if (atom_flag == 0 && line.contains("%FLAG ATOM_NAME")) {
            atom_flag = 1
          }

          else if (atom_flag == 1) {
            if (!line.contains("%")) {
              var n = line.length
              for (i <- 0 to 19) {
                if (i * 4 < n) {
                  var x = line.substring(i * 4, i * 4 + 3).replaceAll(" +", "")
                  //x = StringUtils.center(x, 4)
                  atoms(atoms_cnt) = x
                  atoms_cnt += 1
                }
              }
            }
          }

          if (atom_flag == 1 && line.contains("%FLAG CHARGE")) {
            atom_flag = 2
          }

          if (at_mass_flag == 0 && line.contains("%FLAG MASS"))
            at_mass_flag = 1

          if (at_mass_flag == 1) {
            if (!line.contains("%")) {
              for (i <- 0 to 4) {
                at_mass(at_mass_cnt) = line.substring(i * 16 + 2, i * 16 + 16).toDouble
                at_mass_cnt += 1
              }
            }
          }

          if (at_mass_flag == 1 && line.contains("%FLAG ATOM_TYPE_INDEX"))
            at_mass_flag = 2

          if (mol_name_flag == 0 && line.contains("%FLAG RESIDUE_LABEL")) {
            mol_name_flag = 1
          }

          else if (mol_name_flag == 1) {
            if (!line.contains("%")) {
              var n = line.length
              for (i <- 0 to 19) {
                if (i * 4 < n) {
                  var x = line.substring(i * 4, i * 4 + 3).replaceAll(" +", "")
                  mols(mol_name_cnt) = x
                  mol_name_cnt += 1
                }
              }
            }
          }

          if (mol_name_flag == 1 && line.contains("%FLAG RESIDUE_POINTER")) {
            mol_name_flag = 2
            mol_pointer_flag = 1
          }

          else if (mol_pointer_flag == 1) {
            if (!line.contains("%")) {
              var n = line.length
              for (i <- 0 to 9) {
                if (i * 8 < n) {
                  var x = line.substring(i * 8, i * 8 + 8).replaceAll(" +", "")
                  mol_pointer(mol_pointer_cnt) = x.toInt
                  mol_pointer_cnt += 1
                }
              }

            }
          }

          if (mol_pointer_flag == 1 && line.contains("%FLAG BOND_FORCE_CONSTANT")) {
            mol_pointer_flag = 2;

          }

          if (at_per_mol_flag == 0 && line.contains("%FLAG ATOMS_PER_MOLECULE")) {
            at_per_mol_flag = 1
            1
          }
          else if (at_per_mol_flag == 1) {
            if (!line.contains("%")) {
              var n = line.length
              for (i <- 0 to 9) {
                if (i * 8 < n) {
                  var x = line.substring(i * 8, i * 8 + 8).replaceAll(" +", "")
                  if (at_per_mol_cnt != 0)
                    at_per_mol(at_per_mol_cnt) = x.toInt + at_per_mol(at_per_mol_cnt - 1)
                  else
                    at_per_mol(at_per_mol_cnt) = x.toInt
                  at_per_mol_cnt += 1
                }
              }
            }
          }

          if (at_per_mol_flag == 1 && line.contains("%FLAG BOX_DIMENSIONS")) {
            at_per_mol_flag = 2
            box_dim_flag = 1
          }

          if (box_dim_flag == 1) {
            if (line(0) != '%') {
              for (i <- 1 to 3) {
                boxDim(i - 1) = line.substring(i * 16 + 2, i * 16 + 16).replaceAll(" +", "").toDouble
              }
              box_dim_flag = 2
              break
            }
          }

        }
      }

      for (i <- 0 to total_molecules - 2) {
        mol_pointer(i) = mol_pointer(i + 1) - mol_pointer(i)
      }
      mol_pointer(total_molecules - 1) = total_atoms - mol_pointer(total_molecules - 1) + 1

      var final_mols = new Array[String](total_atoms)
      var final_mol_cnt = new Array[Int](total_atoms)
      var cnt = 0
      for (i <- 0 to total_molecules - 1) {
        var n_times = mol_pointer(i)
        for (j <- 0 to n_times - 1) {
          final_mols(cnt) = mols(i)
          final_mol_cnt(cnt) = i + 1
          cnt += 1
        }
      }
      val s_final_mol_cnt = final_mol_cnt.map(_.toString)
      var nmArr = new Array[String](total_atoms)
      for (i <- 0 to total_atoms - 1) {
        nmArr(i) = start_frame + "," + (i + 1) + "," + atoms(i) + "," + final_mols(i) + "," + s_final_mol_cnt(i)
      }
      name_arr = nmArr
      prm_read_flag = true
      atPerMol = at_per_mol
    }

    firstMolCount = at_per_mol(0)
    var crds = read_crd(crd_file, total_atoms)
    var X = Array.ofDim[Float](10,total_atoms)
    var Y = Array.ofDim[Float](10,total_atoms)
    var Z = Array.ofDim[Float](10,total_atoms)

    var cnt = 0

    for(i <- 0 to 9) {
      for(j <- 0 to ((total_atoms-1))) {
        X(i)(j) = crds.getFloat(cnt)
        cnt += 1
        Y(i)(j) = crds.getFloat(cnt)
        cnt += 1
        Z(i)(j) = crds.getFloat(cnt)
        cnt += 1
      }
    }



    var atom = "ATOM"
    var mol: String = null

    import spark.implicits._

    var finalRes = new Array[Dataset[PDB]](10)
    val crdArr = Array(X(0), Y(0), Z(0)).transpose
    val crdRdd = sc.parallelize(crdArr,50).map { case (x) => x.mkString(",") }.zipWithIndex().map{case(a,b) => (b,a)}
    var temp_name_arr = modifyFrame(name_arr,start_frame)
    var name = sc.parallelize(temp_name_arr,50).zipWithIndex().map{case(a,b) => (b,a)}
    var temp = name.join(crdRdd).map{case(a,(b,c)) => (b.toString+","+c.toString)}
    //val temp1 = temp.map(row => row(1).toString +","+row(3).toString)
    val temp3 = temp.map(mapper).toDS()
    finalRes(0) = temp3.filter(!(temp3("resLabel").isin(Seq("WAT"):_*))&& temp3("atom").isin(Seq("N","CA","C"):_*)&& temp3("resCount")<=801)
    finalRes(0).persist()
    //finalRes.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/df")

    for(i<- 1 to 9) {
      val crdArr1 = Array(X(i), Y(i), Z(i)).transpose
      val crdRdd1 = sc.parallelize(crdArr1,50).map { case (x) => x.mkString(",") }.zipWithIndex().map{case(a,b) => (b,a)}
      temp_name_arr = modifyFrame(name_arr,start_frame+i)
      name = sc.parallelize(temp_name_arr,50).zipWithIndex().map{case(a,b) => (b,a)}
      temp = name.join(crdRdd1).map{case(a,(b,c)) => (b.toString+","+c.toString)}
      val temp1 = temp.map(mapper).toDS()
      finalRes(i) = temp1.filter(!(temp1("resLabel").isin(Seq("WAT"):_*))&& temp1("atom").isin(Seq("N","CA","C"):_*) && temp1("resCount")<=801).orderBy($"index")
      finalRes(i).persist()
    }
    return  finalRes
  }

  def genPdb(df:Array[PDB],total_atoms: Int, out_dir:String, spark: SparkSession):Boolean = {

    var cnt = 0
    var ter_index = 0
    val sc = spark.sparkContext
    var frameNo = df(0).frameNo
    var arrPDB = new scala.collection.mutable.ArrayBuffer[String]()
    for (i <- 0 to total_atoms-1) {
      var str = "%-4s %6d %s %3s %5d     %7.3f %7.3f %7.3f".format("ATOM", df(i).index, StringUtils.center(df(i).atom, 4), df(i).resLabel, df(i).resCount, df(i).X, df(i).Y, df(i).Z)
      arrPDB += str
      if(i == at_per_mol(ter_index)-1)
      {
        str = "%-4s %6d %s %3s %5d".format("TER", df(i).index+1, "    ", df(i).resLabel, df(i).resCount)
        //pw.println(str)
        arrPDB += str
        ter_index += 1
      }
    }
    val rdd = sc.parallelize(arrPDB)
    var value =  returnSevenDigitNo(frameNo)
    rdd.coalesce(1).saveAsTextFile(out_dir+value)
    return true
  }

  def returnSevenDigitNo(number: Int): String = {
    var sevenDigitNo : String = null
    val length = String.valueOf(number).length
    if (length == 1) sevenDigitNo = "000000" + number.toString
    if (length == 2) sevenDigitNo = "00000" + number.toString
    if (length == 3) sevenDigitNo = "0000" + number.toString
    if (length == 4) sevenDigitNo = "000" + number.toString
    if (length == 5) sevenDigitNo = "00" + number.toString
    if (length == 6) sevenDigitNo = "0" + number.toString
    if (length >= 7) sevenDigitNo = number.toString
    sevenDigitNo
  }

}

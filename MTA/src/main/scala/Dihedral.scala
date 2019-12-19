package api
import api._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql._
class Dihedral extends java.io.Serializable {
  def VectorSubtract(v1_x:Double,v1_y:Double,v1_z:Double, v2_x:Double,v2_y:Double,v2_z:Double): List[Double]=
  {
    return List(-(v1_x - v2_x), -(v1_y - v2_y), -(v1_z - v2_z));
  }
  def CrossProduct(v1_x:Double,v1_y:Double,v1_z:Double, v2_x:Double,v2_y:Double,v2_z:Double): List[Double]=
  {
    return List(-(v1_y * v2_z - v1_z * v2_y), -(v1_z * v2_x - v1_x * v2_z), -(v1_x * v2_y - v1_y * v2_x));
  }
  def DotProduct(v1_x:Double,v1_y:Double,v1_z:Double, v2_x:Double,v2_y:Double,v2_z:Double): Double=
  {
    return (v1_x * v2_x  +  v1_y * v2_y  +  v1_z * v2_z);
  }
  def VectorNormalization(v1_x:Double,v1_y:Double,v1_z:Double):List[Double]=
  {
    var len : Double = Math.sqrt(Math.pow(v1_x,2)+Math.pow(v1_y,2)+Math.pow(v1_z,2))
    return List(v1_x/len,v1_y/len,v1_z/len)
  }
  def Angle_finder(vA_x:Double, vA_y:Double, vA_z:Double, vB_x:Double, vB_y:Double, vB_z:Double, vC_x:Double, vC_y:Double, vC_z:Double, vD_x:Double, vD_y:Double, vD_z:Double) : Double =
  {
    var b_a:List[Double]=VectorSubtract(vA_x, vA_y, vA_z, vB_x, vB_y, vB_z)
    var b_c:List[Double]=VectorSubtract(vC_x, vC_y, vC_z, vB_x, vB_y, vB_z)
    var c_d:List[Double]=VectorSubtract(vC_x, vC_y, vC_z, vD_x, vD_y, vD_z)
    var b_c_2 : List[Double]=VectorNormalization(b_c(0),b_c(1),b_c(2))
    var b_a_2 : List[Double]=VectorNormalization(b_a(0),b_a(1),b_a(2))
    var c_d_2 : List[Double]=VectorNormalization(c_d(0),c_d(1),c_d(2))
    var n1:List[Double]=CrossProduct(b_a_2(0),b_a_2(1),b_a_2(2),b_c_2(0),b_c_2(1),b_c_2(2))
    var n2:List[Double]=CrossProduct(b_c_2(0),b_c_2(1),b_c_2(2),c_d_2(0),c_d_2(1),c_d_2(2))
    var m :List[Double]=CrossProduct(n1(0),n1(1),n1(2),b_c_2(0),b_c_2(1),b_c_2(2))
    var x:Double = DotProduct(n1(0),n1(1),n1(2),n2(0),n2(1),n2(2))
    var y:Double = DotProduct(m(0),m(1),m(2),n2(0),n2(1),n2(2))
    var angle = 180.0/Math.PI * Math.atan2(y,x)
    return angle
  }

  def dihedralAngle(sampleDF : Dataset[PDB]): String =
  {
    var s=""
    val without_water = sampleDF.rdd.collect

    var ite = 0
    var i = 0
    var j = 0
    var k = 0
    var x1 : Double = 1.0
    var y1 : Double = 1.0
    var z1 : Double = 1.0
    var x2 : Double = 1.0
    var y2 : Double = 1.0
    var z2 : Double = 1.0
    var x3 : Double = 1.0
    var y3 : Double = 1.0
    var z3 : Double = 1.0
    var x4 : Double = 1.0
    var y4 : Double = 1.0
    var z4 : Double = 1.0
    var x5 : Double = 1.0
    var y5 : Double = 1.0
    var z5 : Double = 1.0
    var x6 : Double = 1.0
    var y6 : Double = 1.0
    var z6 : Double = 1.0
    var x7 : Double = 1.0
    var y7 : Double = 1.0
    var z7 : Double = 1.0
    var x8 : Double = 1.0
    var y8 : Double = 1.0
    var z8 : Double = 1.0
    var x9 : Double = 1.0
    var y9 : Double = 1.0
    var z9 : Double = 1.0
    for(line <- without_water)
    {

      println(line)
      if(i == 0)
      {

        i = i + 1;
        x1 = line.X;
        y1 = line.Y;
        z1 = line.Z;
      }
      else if(i == 1)
      {
        x2 = line.X;
        y2 = line.Y;
        z2 = line.Z;
        i = i + 1;
        if(k==0)
        {
          x6 = line.X;
          y6 = line.Y;
          z6 = line.Z;
          k+=1;
        }
        else if(k==3)
        {
          x9 = line.X;
          y9 = line.Y;
          z9 = line.Z;
          k=1;
          var omg : Double = 0.0;
          omg= -1*(Angle_finder(x6,y6,z6,x7,y7,z7,x8,y8,z8,x9,y9,z9))
          s+="O= "+omg.toString+""
          x6 = x9;
          y6 = y9;
          z6 = z9;
        }


      }
      else if ( i == 2)
      {
        x3 = line.X;
        y3 = line.Y;
        z3 = line.Z;
        i = i+1;
        if(k==1)
        {
          x7 = line.X;
          y7 = line.Y;
          z7 = line.Z;
          k+=1;
        }

        if(j==0)
        {
          x5 = x3
          y5 = y3
          z5 = z3
          j=j+1
        }
        else if(j==1)
        {
          var phi : Double = 0.0;
          phi = -1*(Angle_finder(x5,y5,z5,x1,y1,z1,x2,y2,z2,x3,y3,z3))
          s+="Phi= "+phi.toString+" "
          x5 = x3
          y5 = y3
          z5 = z3
        }
      }
      else if ( i == 3)
      {
        x4 = line.X;
        y4 = line.Y;
        z4 = line.Z;
        var psi : Double = 0.0;
        psi = -1*(Angle_finder(x1,y1,z1,x2,y2,z2,x3,y3,z3,x4,y4,z4))
        s+="psi= "+psi.toString+" "
        x1 = x4
        y1 = y4
        z1 = z4
        i = 1;

        if(k==2)
        {
          x8 = line.X;
          y8 = line.Y;
          z8 = line.Z;
          k+=1;
        }
      }
    }

    return s
  }

}
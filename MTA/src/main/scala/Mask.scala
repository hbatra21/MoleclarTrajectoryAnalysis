package api
import api.PDB
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


class Mask {

  def mask(name:String,sampleDF: Dataset[PDB]): Dataset[PDB]= {
    //import spark.implicits._
    val type_mask: Char = name.charAt(0);
    var masking_col : String = "";
    var names = name.slice(1,name.length).split(",")

    if(type_mask == '@')
    {
      masking_col = "atom"
    }

    else if(type_mask == '/')
    {
      masking_col = "resLabel";
    }

    else if(type_mask == '%')
    {
      var s = name.slice(1,name.length)
      println(s)
      var arr = s.split(" ")
      var residues = arr(0).split("-")
      var min = residues(0)
      var max = residues(1)

      var atoms = arr(1).slice(1,arr(1).length).split(",")

      return sampleDF.filter(sampleDF("resCount").>=(min) && sampleDF("resCount").<=(max) && sampleDF("atom").isin(atoms:_*))
    }

    else
    {
      printf("Incorrect Input format")
      return sampleDF
    }
    return sampleDF.filter(sampleDF(masking_col).isin(names:_*))
  }

  def mask_frame(start : Int , end :Int , intervall : Int , sampleDF: Dataset[PDB]): Dataset[PDB]= {
    // var li :List[Int] = List.range(start,end,intervall)
    //return sampleDF.filter(sampleDF("frameNo").isin(li:_*))
    return sampleDF.filter(sampleDF("frameNo") >= start && sampleDF("frameNo") <= end && (sampleDF("frameNo") - start) % intervall === 0)
  }

  def mask_res_no(res_no: Int , df:Dataset[PDB]):Dataset[PDB] = {
    return df.filter(df("resCount") === res_no)
  }

  def mask_index(index: Int, df:Dataset[PDB]):Dataset[PDB] = {
    return df.filter(df("index") === index)
  }

  def stripWater(df : Dataset[PDB]) : Dataset[PDB] = {
    return df.filter(df("resLabel").=!=("WAT"))
  }

}

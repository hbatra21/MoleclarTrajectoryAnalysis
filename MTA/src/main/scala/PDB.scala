package api
case class PDB(frameNo:Int, index:Int, atom:String, resLabel:String, resCount:Int,X:Double, Y:Double,Z:Double)

case class atom_coordinates(x:Double,y:Double,z:Double)
case class CRD (X : Double, Y : Double, Z : Double)
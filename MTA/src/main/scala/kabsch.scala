package api
import java.util
import java.util.HashMap
import org.ejml.data.Matrix
import org.ejml.simple.{SimpleMatrix, SimpleSVD}

class Kabsch(var p: SimpleMatrix, var q: SimpleMatrix) {
  val P: SimpleMatrix = p
  val Q: SimpleMatrix= q

  private def isValidInputMatrix(M: SimpleMatrix) = M.numCols >= 2 && M.numRows >= M.numCols

  private def getCovariance():SimpleMatrix = {
    val CV = P.mult(Q.transpose())
    CV
  }

  def calculate(): Double = {
    val CV = getCovariance
    val svd = new SimpleSVD(CV.getMatrix, true)
    val V = svd.getU.asInstanceOf[SimpleMatrix]
    val W = svd.getV.asInstanceOf[SimpleMatrix]
    val I = SimpleMatrix.identity(V.numCols)
    val d = Math.signum(V.mult(W.transpose()).determinant).toInt
    I.set(V.numCols -1,V.numCols -1,d)
    val R = (W.mult(I)).mult(V.transpose())
    val diff = R.mult(P).minus(Q)
    val temp = diff.elementMult(diff)
    val rmsd = Math.sqrt(temp.elementSum()/P.numCols())
    rmsd
  }
}
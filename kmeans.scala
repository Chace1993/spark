import java.util.Random
import org.apache.log4j.{Level, Logger}
import scala.collection.immutable.Vector
import org.apache.spark.{SparkConf, SparkContext}

object kmeans {

   def clostestpoint(q: Vector[Double], candidates: Array[Vector[Double]]): Vector[Double] = {
     var closest = Double.PositiveInfinity
     var clostpoint = Vector(0.0,0.0)

     for (i <- 0 until candidates.length) {
       val vCurr = candidates(i)
       val tempDist = distance(q,vCurr)
       if (tempDist < closest) {
         closest = tempDist
         clostpoint = vCurr
       }
     }
     clostpoint
   }

   def distance(p:Vector[Double], q:Vector[Double]) : Double = {
      math.sqrt(p.zip(q).map(pair => math.pow((pair._1 - pair._2),2)).reduce(_+_))
   }

   def add_vec(v1: Vector[Double], v2: Vector[Double]): Vector[Double] = {
     Vector(v1(0)+v2(0),v1(1)+v2(1))
   }

   def average(cluster: Iterable[Vector[Double]]): Vector[Double] = {
     var v1 = Vector(0.0,0.0)


     var cluster_iterator = cluster.iterator
     while(cluster_iterator.hasNext){
       var v2 = cluster_iterator.next()
       v1 = add_vec(v1,v2)
     }
     v1.map {mapping => mapping/cluster.size.toDouble}
   }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("K_means")
    val sc = new SparkContext(conf)
    val input = args(0)
    val output = args(1)
    val k = args(2).toInt
    val convergeDist = args(3).toDouble
    var tempDist = 1.0

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


    val data = sc.textFile(input).map(l => Vector.empty ++ l.split('\t').map(_.toDouble))

    var centers = data.takeSample(false, k, new Random().nextLong())

    val center = centers.__resultOfEnsuring

    println("inital candidates:")
    for (i <- 0 until center.length) {
      println(i + ":" + center(i))
    }

    while(tempDist > convergeDist) {

    var closest = data.map(p => (clostestpoint(p, centers), p))

    var mappings = closest.groupByKey()

    var pointStats = mappings.map(
      pair => {
        var center = pair._1
        var newPoints = average(pair._2)
        (center, newPoints)
      }
    )

    tempDist = 0.0
      var newPointArr = pointStats.toArray()

    for (mapping <- newPointArr) {
      var pos = centers.indexOf(mapping._1)
      tempDist += distance(centers(pos), mapping._2)
    }

      for (mapping <- newPointArr) {
        var pos = centers.indexOf(mapping._1)
        centers.update(pos,mapping._2)
      }
  }
    println("Final centers: ")
    centers.foreach(println)
    }
}

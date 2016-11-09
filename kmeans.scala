// def distance(p:Vector[Double], q:Vector[Double]) : Double = {
// }
 
// def clostestpoint(q: Vector[Double], candidates: Array[Vector[Double]]): Vector[Double] = {
// }
 
// def add_vec(v1: Vector[Double], v2: Vector[Double]): Vector[Double] = {
// }
 
// def average(cluster: Iterable[Vector[Double]]): Vector[Double] = {
// }


var lines = sc.textFile("/home/hduser/clustering_dataset.txt");

var data = lines.map(l => Vector.empty ++ l.split('\t').map(_.toDouble))

var k = 3

data.foreach(println)


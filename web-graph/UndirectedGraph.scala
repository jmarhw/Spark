import java.io._
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD

object UndirectedGraph {
  val input = sc.textFile("/../web-Stanford.txt")
  val edges = input.filter(line => !line.contains("#")).map(line => line.split("\\W+"))
    .map(x => if (x(0).toInt < x(1).toInt) (x(0).toInt, x(1).toInt) else (x(1).toInt, x(0).toInt)).distinct // create (u,v) edge list

  def getDegrees(): Array[(Int, Double)] = {
    // for (u,v) get degrees of u and v
    val degree_u = edges.map(x => (x._1, 1.0)).reduceByKey(_ + _)
    val degree_v = edges.map(x => (x._2, 1.0)).reduceByKey(_ + _)

    // get degrees of u and v in a list
    val degrees = (degree_u ++ degree_v).groupBy(_._1) // groupBy returns (key, [(key,value), (key,value)...] ) pairs
      .map(x => (x._1, x._2.map(_._2).sum.toDouble)).sortByKey() // map will return (vertex, degree) pairs
    //      degrees.foreach(println)
    return degrees.collect()
  }

  def getTriangles(degree: Array[(Int, Double)]): RDD[(Int, Double)] = {
    val degreeMap = degree.toMap
    val mapper1 = edges.map(x => {
      if (degreeMap.get(x._1).get > degreeMap.get(x._2).get) (x._2, x._1) else (x._1, x._2)
    }).groupByKey() // it returns (node, (all nodes corresponding to this node with bigger degrees))
    val mapper2 = mapper1.flatMap(x => {
      val higherNodes = (x._2).toArray // all nodes of higher degree
      val len = higherNodes.length
      val triads = // get all possible thirds, but it's controlled by the node of lowest degree
        for (a <- 0 until len) yield {
          for (b <- (a + 1) until len) yield {
            if (higherNodes(b) > higherNodes(a)) {
              ((higherNodes(a), higherNodes(b)), x._1)
            }
            else {
              ((higherNodes(b), higherNodes(a)), x._1)
            }
          }
        }
      triads.flatten
    })

    val triangles = mapper2.join(edges.map(x => ((x._1, x._2), "*"))).map(x => (x._1._1, x._1._2, x._2._1)) // join "checks" which pairs existed in the original list against the triads list
    val reducer = triangles.flatMap(x => Array((x._1, 1.0), (x._2, 1.0), (x._3, 1.0))).reduceByKey(_ + _)

    return reducer
  }

  def clusteringCoefficient(d: Array[(Int, Double)], t: RDD[(Int, Double)]): RDD[(Int, Double)] = {
    val degree = sc.parallelize(d)
    val triangle = t
    val getAll = degree.leftOuterJoin(triangle).map { case (x, (y, z: Option[Double])) => (x, y, if (z.isEmpty) (0.0) else (z.get)) } //creates: (vertex,degree,triangle count)

    val clusterCoeff: RDD[(Int, Double)] = getAll.map(x => {
      val vertex = x._1
      val deg = x._2.toDouble
      val tri = x._3.toDouble

      if (deg <= 1) (vertex, 0.0)
      else (vertex, 2.0 * tri / (deg * (deg - 1.0)))
    })

    clusterCoeff.take(10).foreach(println)
    return clusterCoeff
  }

  def averageClusteringCoefficient(c: RDD[(Int, Double)]): Double = {
    val sumClusterCoeff = c.map(_._2).sum()
    val total = c.count
    return sumClusterCoeff / total
  }
}

object Test {
  def main(): Unit = {
    val t1 = System.nanoTime
    val graph = UndirectedGraph
    val degrees = graph.getDegrees()
    val triangles = graph.getTriangles(degrees)
    val clusterCoeff = graph.clusteringCoefficient(degrees, triangles)
    val averageClusterCoeff = graph.averageClusteringCoefficient(clusterCoeff)

    val output1 = clusterCoeff.leftOuterJoin(sc.parallelize(degrees)).map { case (x, (y, z: Option[Double])) => (x, y, z.get) }.collect()
    val writer1 = new PrintWriter(new File("/../output.txt"))
    for (opt <- output1) {
      writer1.write(opt.toString + "\n")
    }
    writer1.close()
    val writer2 = new PrintWriter(new File("/../average.txt"))
    writer2.write(s"The average clustering coefficient for the graph: ${averageClusterCoeff}")
    writer2.close()

    val duration = (System.nanoTime - t1) / 1e9d
    println(duration)
  }
}



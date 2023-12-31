package david.sc_dbscan

import david.sc_dbscan.objects.NodeBuilder
import david.sc_dbscan.process._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import david.sc_dbscan.objects.{Triple, TripleBuilder, Noeud}

object Main {
  val logger = Logger.getLogger(getClass.getName)

  val usage =
    """
      |Usage: sc_dbscan --eps e --mpts m --cap c file
      |    --eps e  : threshold for clustering
      |    --mpts p : minimum number of points
      |    --cap c  : capacity of a node
      |    --coef cf: coefficient is associated to patterns
      |    --schemafile  schemf   : schema filename
      |    file     : data filename
    """.stripMargin

  type OptionMap = Map[Symbol, Any]

  def parseArguments(map: OptionMap, arguments: List[String]): OptionMap = {
    arguments match {
      case Nil => map
      case "--eps" :: value :: tail =>
        parseArguments(map ++ Map('eps -> value.toDouble), tail)
      case "--mpts" :: value :: tail =>
        parseArguments(map ++ Map('mpts -> value.toInt), tail)
      case "--mergeloop" :: value :: tail =>
        parseArguments(map ++ Map('mergeloop -> value.toInt), tail)
      case "--cap" :: value :: tail =>
        parseArguments(map ++ Map('cap -> value.toInt), tail)
      case "--coef" :: value :: tail =>
        parseArguments(map ++ Map('coef -> value.toBoolean), tail)
      case "--schemafile" :: schemaFile :: tail =>
        map ++ Map('schemafile -> schemaFile)
      case dataFile :: Nil =>
        map ++ Map('datafile -> dataFile)
      case option :: tail =>
        println(usage)
        throw new IllegalArgumentException(s"Unknown argument $option");
    }
  }


  //  val conf = new SparkConf()
  //    .setAppName("SC_DBSCAN")
  ////    .setMaster("local[*]")
  //    .setMaster("spark://s0.adam.uvsq.fr:7077")
  //
  //  val sc = new SparkContext(conf)


  //  val propertyCodification: HashMap[String, Int] = HashMap()
  //
  //  val propertyDecoding: HashMap[Int, String] = HashMap()

  //  val corpsNumber = sc.accumulator(0)

  //    Add a parameter to determine whethier we use coefficients or not
  //  var useCoefficient = false

  //  var temporaryEps: Double = 0

  //  --------------------------------------------------------
  //  Change the properties by codes
  //  --------------------------------------------------------
  //  def codifiate(prop: Set[String]): Set[Int] = {
  //
  //    var newProp: Set[Int] = Set()
  //
  ////    prop.foreach { p =>
  ////      newProp = newProp + propertyCodification.get(p).get
  ////    }
  //
  //    prop.foreach { p =>
  //      newProp = newProp + p.toInt
  //    }
  //
  //    return newProp
  //  }

  //  --------------------------------------------------------
  //  Bring back the properties to the initial value
  //  --------------------------------------------------------
  //  def decoding(prop: Set[Int]): Set[String] = {
  //
  //    var initialProp: Set[String] = Set()
  //
  ////    prop.foreach { p =>
  ////      initialProp = initialProp + propertyDecoding.get(p).get.toString()
  ////    }
  //
  //
  //    prop.foreach { p =>
  //      initialProp = initialProp + p.toString()
  //    }
  //
  //    return initialProp
  //  }

  def main(args: Array[String]): Unit = {

//    val sparkSession = SparkSession.builder
//      .master("local")
//      .appName("SC_DBSCAN")
//      .getOrCreate()
//
//
//    val sc = sparkSession.sparkContext

    println("START_V2.3_MERGING")

    logger.debug(s"Start of main method with arguments ${args.mkString(" ")}")
    val configuration = parseArguments(Map(), args.toList)


    val eps = configuration.getOrElse('eps, 0.7).asInstanceOf[Double]
    val minPts = configuration.getOrElse('mpts, 1).asInstanceOf[Int]
    val nodeCapacity = configuration.getOrElse('cap, 5000).asInstanceOf[Int]
    val coefficient = configuration.getOrElse('coef, true).asInstanceOf[Boolean]
    val dataFile = configuration.getOrElse('datafile, "/home/red/Téléchargements/eval saturation/lubm-uba/small/entitiespatterns3/patterns/patterns").asInstanceOf[String]
    val schemaFile = configuration.getOrElse('schemafile, "/home/red/Téléchargements/eval saturation/lubm-uba/small/entitiespatterns3/patterns/ontology").asInstanceOf[String]

//    Number of loop during the merging step
    val mergeLoop = configuration.getOrElse('mergeloop, 5).asInstanceOf[Int]

    logger.debug(s"Parsed parameters : eps = $eps, minPts = $minPts, capacity = $nodeCapacity, coefficient = $coefficient data file = $dataFile, merging loop = $mergeLoop, schema file = $schemaFile")

    val sparkSession = SparkSession.builder
//      .master("local")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val data = sc.textFile(dataFile)

    val schema = sc.textFile(schemaFile)

    val s1 = Saturation.extract(schema)

    val s2  = Saturation.saturateSchema(s1)

    val s3 = Ordering.getPropertyOrder(data, s2, coefficient)

//    s3.foreach(p => println(p._1+" "+p._2))

//    s2.foreach(p => println(p._1+" -> "+p._2.toList))

//    val x = Ordering.getPropertyStat(data, false)

//    x.foreach(p => println(p._1+" "+p._2.toList))

//    broadcast the schema declarations
    sc.broadcast(s2)
//    broadcast the order of the properties
    sc.broadcast(s3)

    val r2 = Partitioning.getInitalPartition(data, eps, coefficient, s3, s2)

//    r4.foreach(p=> println(p._1+" "+p._2.toList))

    val r3 = Partitioning.getFinalPartitions(r2, nodeCapacity, eps, s3, coefficient)

//    r3.map(f => (f._1, f._2.toSet)).saveAsTextFile("finalPartition")

    val r4 = CoresIdentification.coresIdentification(r3, minPts, eps)

    val r5 = Clustring.generateClusters(r4)

    val r6 = GlobalMerging.clustersMerging(r5, mergeLoop)


//    //    var list: Array[Noeud] = Array()
//    //    r2.values.toLocalIterator.foreach { f =>
//    //      list = list.++:(f)
//    //    }
//    //
//    //    println("Entities : " + list.distinct.size)
//    //    println("Entities Duplication: " + list.size)
//    //
//    //    var list2: Set[ClusterId] = Set()
//    //    r5.values.toLocalIterator.foreach { f =>
//    //      list2 = list2.+(f)
//    //    }
//    //    println("Local Clusters : " + list2.size)

    println("Initial Partition : " + r2.count())
    println("Final Partitions : " + r3.count())
    println("Clusters : " + r6.size)


    //            println("---------------The Initial Partitions--------------")
    //            r2.foreach { f =>
    //              var printer = (f._1).toString() + " -> "
    //              f._2.foreach { p =>
    //                printer += (p.getProperties()).toString() + "  "
    //              }
    //
    //              println(printer)
    //            }
    //
    //                println("---------------The Final Partitions--------------")
    //                r3.foreach { f =>
    //                  var printer = decoding(f._1).toString() + " -> "
    //                  f._2.foreach(p => printer = printer + decoding(p.getProperties()).toString() + "  ")
    //
    //                  println(printer)
    //                }
    //        var z = r3.map { v =>
    //
    //          var printer = ""
    //
    //          v._2.foreach(p => printer = printer + " , [" + p.getId() + "]")
    //
    //          (decoding(v._1), printer)
    //        }
    //
    //        z.saveAsTextFile("partitions")

    //
    //        println("---------------Core identification--------------")
    //        r4.foreach { f =>
    //          var printer = decoding(f._1).toString() + " -> \n\t"
    //          f._2.foreach { p => printer = printer + decoding(p.getProperties()).toString() + " = " + p.isCore() + " | "
    //          }
    //          println(printer)
    //        }
    //
    //        println("---------------Partial Clusters--------------")
    //        r5.groupByKey().foreach { f =>
    //          println(f._1 + " " + f._2)
    //        }

//                    println("---------------Final Clusters--------------")

//                        var i = 1
//                        r6.foreach { f =>
////                          var x = "C" + i + " : " + f._1.toString() + " : " + f._2
//
//                          var x = "C" + i+" : "
//                                                  f._2.foreach { p =>
//                                                    x = x + p+" , "
//                                                  }
//                          println(x)
//                          i = i + 1
//                        }
    //                    var i = 1

    //    println("Number of corps = " + corpsNumber)

    sc.stop()
    sparkSession.stop()

    println("END")
    logger.debug("End of main method")
  }

}

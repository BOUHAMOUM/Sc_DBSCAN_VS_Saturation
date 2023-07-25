package david.sc_dbscan.process


import david.sc_dbscan.objects.Triple
import org.apache.spark.rdd.RDD
import org.dmg.pmml.Coefficient


object Ordering {

  val subProperty = "<subProperty>"
  val inverseOf = "<inverseOf>"
  val symetricProperty = "<symetricProperty>"

  //  ----------------------------------------------------------
  //  Generate the first partitions according to the propertySet
  //  ----------------------------------------------------------
  def getPropertyStat(files: RDD[String], coefficient: Boolean): Map[Int, Map[Int, Int]] = {

    val properties = files.flatMap {
      line =>

        var propertyList = line.split(" ")

        var propetySize = propertyList.length - 1

        //        If the last column is a coefficient, it won't be considered
        if (coefficient) {
          propetySize = propetySize - 1
        }

        var combinaisons: Map[Int, Int] = Map()

        for (i <- 1 to propetySize; if propertyList(i) != "") {
          combinaisons = combinaisons + (propertyList(i).toInt -> 1)
        }


        for (i <- 1 to propetySize; if propertyList(i) != "") yield {

          val property = propertyList(i).toInt

          if( property > 0 )
//            If the properties are out-going, we only need their initial selectivity
            ( property, combinaisons )
          else ( property, Map(( property -> 1 )))
        }
    }.reduceByKey{
      (x, y) =>

        var retour: Map[Int, Int] = y

        x.foreach{
          elem =>
            if( retour.contains(elem._1) )
            {
              retour = retour + (elem._1 -> (elem._2 + retour.get(elem._1).get))
            }
            else
            {
              retour = retour + (elem._1 -> elem._2)
            }
        }

        retour
    }

    return properties.collect().toMap
  }



  def getPropertyOrder(files: RDD[String], schema: Map[Int, Set[Triple]], coefficient: Boolean ): Map[Int, Int] ={

    val propertyStat: Map[Int, Map[Int, Int]] = getPropertyStat(files, coefficient)

    var propertyOrder: Map[Int, Int] = Map()

    propertyStat.foreach{ p =>

      var concernedProperty = p._1

      var selectivity = p._2.get(p._1).get

      val statE = schema.get(p._1)

      if( statE != None && !statE.get.isEmpty )
      {
        val stat = statE.get

        stat.foreach { schemaProperty =>

//          The selectivity whish should be changed is the selectivity of the object
          if( schemaProperty.getProperty() == subProperty )
          {
            var objectSelectivity = selectivity

            if( propertyStat.get(schemaProperty.getValue().toInt) != None )
            {
              if( p._2.get(schemaProperty.getValue().toInt) != None )
              {
                objectSelectivity = objectSelectivity  - p._2.get(schemaProperty.getValue().toInt).get
              }
              else
              {
                objectSelectivity = objectSelectivity
              }
            }

            if( propertyOrder.contains(schemaProperty.getValue().toInt) )
              {
                objectSelectivity = objectSelectivity + propertyOrder.get(schemaProperty.getValue().toInt ).get
                propertyOrder = propertyOrder + (schemaProperty.getValue().toInt -> objectSelectivity)
              }
            else
              {
                propertyOrder = propertyOrder + (schemaProperty.getValue().toInt -> objectSelectivity)
              }

          }
          else if( schemaProperty.getProperty() == inverseOf )
          {
            if( propertyStat.get(schemaProperty.getSubject().toInt * -1) != None )
            {
              concernedProperty = schemaProperty.getValue().toInt

              //                  The selectivity of the property inverseOf p._1
              val elem2 = propertyStat.get(schemaProperty.getSubject().toInt * -1).get.get(schemaProperty.getSubject().toInt * -1).get

              if( p._2.get(schemaProperty.getValue().toInt * -1) != None )
              {
                selectivity = (selectivity + elem2) - p._2.get(schemaProperty.getSubject().toInt * -1).get
              }
              else
              {
                println("ELEM "+ elem2)

                selectivity = selectivity + elem2
              }
            }
          }
          else if( schemaProperty.getProperty() == symetricProperty )
          {
            if( p._2.get(schemaProperty.getSubject().toInt * -1) != None )
            {
              selectivity = (selectivity * 2) - p._2.get(schemaProperty.getSubject().toInt * -1).get
            }
            else
            {
              selectivity = selectivity * 2
            }
          }
        }
      }

      propertyOrder = propertyOrder + (concernedProperty -> selectivity)
    }


    return propertyOrder
  }

}
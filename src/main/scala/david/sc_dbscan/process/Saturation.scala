package david.sc_dbscan.process

import david.sc_dbscan.objects.{Triple, TripleBuilder, Noeud}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashMap

object Saturation {


  val typeE = "<type>"
  val subProperty = "<subProperty>"
  val functionalproperty = "<Functionalproperty>"
  val sameAs = "<sameAs>"
  val inverseFunctionalproperty = "<inverseFunctionalproperty>"
  val equivalentProperty = "<equivalentProperty>"
  val owlProperty = "<owlProperty>"
  val inverseOf = "<inverseOf>"
  val sameRole = "<sameRole>"
  val symetricProperty = "<symetricProperty>"


  def extract(data: RDD[String]): RDD[(Int, Array[Triple])] = {

    val schemaTriples = data.flatMap {
      line =>
        val triple = line.split(" ")

        if( triple(1) == subProperty )
        {
          for (i <- 0 to 1) yield {
            (triple((i+1)*i).toInt, Array(TripleBuilder.createTriple(line)))
          }
        }
        else if( triple(1) == inverseOf )
        {
          for (i <- 0 to 1) yield {
            (triple((i+1)*i).toInt, Array(TripleBuilder.createTriple(line)))
          }
        }
//          Execute rules 6 et 7
//          Generate subProperty declarations from equivalentProperty triples
        else if( triple(1) == equivalentProperty )
        {
          val t1 = new Triple(triple(0), subProperty, triple(2))
          val t2 = new Triple(triple(2), subProperty, triple(0))

          for (i <- 0 to 1) yield {
            (triple((i+1)*i).toInt, Array(t1, t2))
          }
        }
//          Execute the rule 10
//          sameAs declarations are commutative
        else if( triple(1) == sameAs )
        {
          val t1 = new Triple(triple(0), sameAs, triple(2))
//          val t2 = new Triple(triple(2), sameAs, triple(0))

          for (i <- 0 to 1) yield {
            (triple((i+1)*i).toInt, Array(t1))
          }
        }
        else if(triple(2) == functionalproperty || triple(2) == inverseFunctionalproperty || triple(2) == owlProperty)
        {
          for (i <- 1 to 1) yield {
            (triple(0).toInt, Array(TripleBuilder.createTriple(line)))
          }
        }
        else if(triple(2) == symetricProperty)
        {
          for (i <- 1 to 1) yield {
            (triple(0).toInt, Array(new Triple(triple(0), symetricProperty, "")))
          }
        }
        else
          {
            for (i <- 1 to 1) yield {
              println()
              (triple(1).toInt, Array(TripleBuilder.createTriple(line)))
            }
          }
    }

    return schemaTriples.reduceByKey(_ ++ _)
  }


  def saturateSchema( dataSet: RDD[(Int, Array[Triple])] ): Map[Int, Set[Triple]] = {

    val s1 = saturateFunctionalproperty(dataSet)

    val s2 = saturateInverseFunctionalproperty(dataSet)

    val s3 = saturateOwlProperty(dataSet.union(s1).union(s2))

    val s4 = saturateSubProperty(dataSet.union(s3))

    val s5 = saturateSupPropToEquivalent(s4)

    val s6 = saturateInverseOf(dataSet)

    val totalData = s4.union(s5).union(s6)

    val schemaDeclarations: RDD[(Int, Set[Triple])] = totalData.flatMap{ part =>
      val t = part._2
      for (i <- 0 to t.length -1) yield {
          if( usefulForEntitiesSaturation(t(i).getProperty()))
            (t(i).getSubject().toInt, Set(t(i)))
          else
            (0, Set(new Triple("","","")))
        }
    }.reduceByKey(_ ++ _)


    return schemaDeclarations.collect().toMap
  }


  def usefulForEntitiesSaturation(property: String): Boolean = {

    if( property == subProperty || property == inverseOf || property == sameRole || property == symetricProperty )
      return true
    else
      return false
  }

//----------------------------------------
//  Saturate the subProperty declarations
//  --------------------------------------
  def saturateSubProperty( dataSet: RDD[(Int, Array[Triple])] ): RDD[(Int, Array[Triple])] = {

    var newTriples = dataSet.flatMap{
      property =>
        val r = saturateRule1(property._2)

        for (i <- 0 to (r.length*3) - 1) yield {
          if( i < r.length)
            {
              (r(i).getSubject().toInt, Array(r(i)))
            }
          else if( i < r.length*2)
            {
              (r(i/2).getValue().toInt, Array(r(i/2)))
            }
          else
            {
              //  Remettre les triplets dans les partitions de création afin de ne pas les reproduire
              (property._1, Array(r(i/3)))
            }
          }
    }.reduceByKey(_ ++ _)


    if( newTriples.count() > 0 )
      saturateSubProperty(dataSet.union(newTriples).reduceByKey(_ ++ _))
    else
      return dataSet.union(newTriples.distinct()).reduceByKey(_ ++ _)
  }


  def saturateRule1( triples: Array[Triple] ): Array[Triple] = {

    var tripetsResult: Array[Triple] = Array()

    for ( i <- 0 to triples.length -1 ){
      val triple1 = triples(i)

      for ( j <- i+1 to triples.length - 1){
        val triple2 = triples(j)
              if( (triple1.getProperty() == subProperty && triple2.getProperty() == subProperty) )
                {
                  if ( triple1.getValue() == triple2.getSubject() && triple1.getSubject() != triple2.getValue() )
                    {
                      val newT = new Triple(triple1.getSubject(), subProperty, triple2.getValue())

                      if( !triples.contains(newT) )
                      {
                        tripetsResult = tripetsResult :+ newT
                      }
                    }
                  else if ( triple2.getValue() == triple1.getSubject() && triple2.getSubject() != triple1.getValue())
                    {
                      val newT = new Triple(triple2.getSubject(), subProperty, triple1.getValue())

                      if( !triples.contains(newT) )
                      {
                        tripetsResult = tripetsResult :+ newT
                      }
                    }
                }
        }

    }

    return tripetsResult.distinct
  }


  //----------------------------------------
  //  Saturate the Functionalproperty declarations
  //  --------------------------------------
  def saturateFunctionalproperty( dataSet: RDD[(Int, Array[Triple])] ): RDD[(Int, Array[Triple])] = {

    var newTriples = dataSet.flatMap{
      property =>
        val r = saturateRule11(property._2)

        for (i <- 0 to r.length - 1) yield {
            (r(i).getSubject().toInt, Array(r(i)))
        }
    }.reduceByKey(_ ++ _)

    return newTriples
  }


  def saturateRule11( triples: Array[Triple] ): Array[Triple] = {

    var tripetsResult: Array[Triple] = Array()

    for (i <- 0 to triples.length - 1){
        val triple1 = triples(i)

        if( triple1.getValue() == functionalproperty && triple1.getProperty() == typeE)
          {
            //        List of entities having the property triple1.subject()
            //        Hash according to the value to have a fast access
            var entitiesOfP: HashMap[String, Array[Triple]] = HashMap()

            for (j <- i+1 to triples.length -1){
              val triple2 = triples(j)
              if( triple2.getProperty() == triple1.getSubject() )
                  {
                    if( entitiesOfP.contains(triple2.getValue()) )
                    {
                      var listE: Array[Triple] = entitiesOfP.get(triple2.getValue()).get
                      listE = listE :+ triple2

                      entitiesOfP.put(triple2.getValue(), listE)
                    }
                    else
                    {
                      entitiesOfP.put(triple2.getValue(), Array(triple2))
                    }
                  }
            }

            if( entitiesOfP.size > 0 )
              {
                entitiesOfP.foreach {
                  t =>
                    t._2.foreach {
                      t2 =>
                        if( entitiesOfP.contains(t2.getSubject()) )
                        {
                          var listE: Array[Triple] = entitiesOfP.get(t2.getSubject()).get

                          listE.foreach{
                            t3 =>
                              val newT1 = new Triple(t2.getValue(), sameAs, t3.getSubject())
                              val newT2 = new Triple(t3.getSubject(), sameAs, t2.getValue())

                              tripetsResult = tripetsResult :+ newT1
                              tripetsResult = tripetsResult :+ newT2
                          }
                        }
                    }
                }
              }
          }
        }


    return tripetsResult.distinct
  }


  //----------------------------------------
  //  Saturate the InverseFunctionalproperty declarations
  //  --------------------------------------
  def saturateInverseFunctionalproperty( dataSet: RDD[(Int, Array[Triple])] ): RDD[(Int, Array[Triple])] = {

    var newTriples = dataSet.flatMap{
      property =>
        val r = saturateRule12(property._2)

        for (i <- 0 to r.length - 1) yield {
          (r(i).getSubject().toInt, Array(r(i)))
        }
    }.reduceByKey(_ ++ _)

    return newTriples
  }


  def saturateRule12( triples: Array[Triple] ): Array[Triple] = {

    var tripetsResult: Array[Triple] = Array()

    for(i <- 0 to triples.length - 1){
      val triple1 = triples(i)

        if( triple1.getValue() == inverseFunctionalproperty && triple1.getProperty() == typeE)
        {
          //        List of entities having the property triple1.subject()
          //        Hash according to the value to have a fast access
          var entitiesOfP: HashMap[String, Array[Triple]] = HashMap()

          for(j <- i+1 to triples.length - 1){
            val triple2 = triples(j)
              if( triple2.getProperty() == triple1.getSubject() )
              {
                if( entitiesOfP.contains(triple2.getValue()) )
                {
                  var listE: Array[Triple] = entitiesOfP.get(triple2.getValue()).get
                  listE = listE :+ triple2

                  entitiesOfP.put(triple2.getValue(), listE)
                }
                else
                {
                  entitiesOfP.put(triple2.getValue(), Array(triple2))
                }
              }
          }

          if( entitiesOfP.size > 0 )
          {
            entitiesOfP.foreach {
              t =>
                t._2.foreach {
                  t2 =>
                    if( entitiesOfP.contains(t2.getValue()) )
                    {
                      var listE: Array[Triple] = entitiesOfP.get(t2.getValue()).get

                      listE.foreach{
                        t3 =>
                          if( t2.getSubject() != t3.getSubject() )
                          {
                            val newT1 = new Triple(t2.getSubject(), sameAs, t3.getSubject())
                            val newT2 = new Triple(t3.getSubject(), sameAs, t2.getSubject())

                            tripetsResult = tripetsResult :+ newT1 :+ newT2
                          }
                      }
                    }
                }
            }
          }
        }
    }


    return tripetsResult.distinct
  }


  //----------------------------------------
  //  transform the supProperty declarations to equivalentProperty
  //  --------------------------------------
  def saturateSupPropToEquivalent( dataSet: RDD[(Int, Array[Triple])] ): RDD[(Int, Array[Triple])] = {

    var newTriples = dataSet.flatMap{
      property =>
        val r = saturateRule8(property._2)

        for (i <- 0 to r.length - 1) yield {
          (r(i).getSubject().toInt, Array(r(i)))
        }
    }.reduceByKey(_ ++ _)

    return newTriples
  }

  def saturateRule8( triples: Array[Triple] ): Array[Triple] = {

    var tripetsResult: Array[Triple] = Array()

    for (i <- 0 to triples.length - 1)
      {
        val triple1 = triples(i)

        for (j <- i+1 to triples.length - 1)
          {
            val triple2 = triples(j)

            if( (triple1 != triple2) && (triple1.getProperty() == subProperty && triple2.getProperty() == subProperty)
              && (triple1.getValue() == triple2.getSubject()) && (triple2.getValue() == triple1.getSubject()) )
            {
              val newT = new Triple(triple1.getSubject(), equivalentProperty, triple1.getValue())

              if( !triples.contains(newT) )
              {
                tripetsResult = tripetsResult :+ newT
              }
            }
          }
      }

    return tripetsResult.distinct
  }


  //----------------------------------------
  //  transform the owlProperty declarations to supProperty (RULE 9)
  //  --------------------------------------
  def saturateOwlProperty( dataSet: RDD[(Int, Array[Triple])] ): RDD[(Int, Array[Triple])] = {

    var newTriples = dataSet.flatMap{
      property =>
        val r = saturateRule9(property._2)

        for (i <- 0 to r.length - 1) yield {
          (r(i).getSubject().toInt, Array(r(i)))
        }
    }.reduceByKey(_ ++ _)

    return newTriples
  }


  def saturateRule9( triples: Array[Triple] ): Array[Triple] = {

    var tripetsResult: Array[Triple] = Array()

    for (i <- 0 to triples.length - 1){
      val triple1 = triples(i)

      if( triple1.getValue() == owlProperty && triple1.getProperty() == typeE)
      {
        for (j <- 0 to triples.length -1){
          val triple2 = triples(j)

          if( triple2.getProperty() == sameAs && ( triple2.getSubject() == triple1.getSubject() || triple2.getValue() == triple1.getSubject() ))
          {
            val newT1 = new Triple(triple2.getSubject(), subProperty, triple2.getValue())
            val newT2 = new Triple(triple2.getValue(), subProperty, triple2.getSubject())

            tripetsResult = tripetsResult :+ newT1
            tripetsResult = tripetsResult :+ newT2
          }
        }
      }
    }


    return tripetsResult.distinct
  }


  //----------------------------------------
  //  Saturate the inverseOf declarations
  //  --------------------------------------
  def saturateInverseOf( dataSet: RDD[(Int, Array[Triple])] ): RDD[(Int, Array[Triple])] = {

    var newTriples = dataSet.flatMap{
      property =>
        val r = saturateRule13(property._2)

        for (i <- 0 to (r.length*2) - 1) yield {
          if( i < r.length)
          {
            (r(i).getSubject().toInt, Array(r(i)))
          }
          else
          {
            (r(i/2).getValue().toInt, Array(r(i/2)))
          }
        }
    }.reduceByKey(_ ++ _)

    return newTriples
  }


  def saturateRule13( triples: Array[Triple] ): Array[Triple] = {

    var tripetsResult: Array[Triple] = Array()

    for ( i <- 0 to triples.length -1 ){
      val triple1 = triples(i)

      for ( j <- i+1 to triples.length - 1){
        val triple2 = triples(j)
        if( (triple1.getProperty() == inverseOf && triple2.getProperty() == inverseOf) )
        {
          if ( triple1.getValue() == triple2.getSubject() )
          {
            val newT1 = new Triple(triple1.getSubject(), sameRole, triple2.getValue())
            val newT2 = new Triple(triple2.getValue(), sameRole, triple1.getSubject())

            if( !triples.contains(newT1) )
            {
              tripetsResult = tripetsResult :+ newT1 :+ newT2
            }
          }
          else if ( triple2.getValue() == triple1.getSubject() )
          {
            val newT1 = new Triple(triple2.getSubject(), sameRole, triple1.getValue())
            val newT2 = new Triple(triple1.getValue(), sameRole, triple2.getSubject())

            if( !triples.contains(newT1) )
            {
              tripetsResult = tripetsResult :+ newT1 :+ newT2
            }
          }
        }
      }

    }

    return tripetsResult.distinct
  }


  //----------------------------------------
  //  Saturation of the entities based on schema declarations
  //  --------------------------------------
  def saturateEntities(entity: Noeud, schema: Map[Int, Set[Triple]]): Noeud = {

    var newProperties:Set[Int] = Set()

    entity.getProperties().foreach{
      property =>

        if( property > 0 && schema.get(property) != None )
          {
            val propertyRelations = schema.get(property).get

            propertyRelations.foreach{ schemaDeclaration =>

              if( schemaDeclaration.getProperty() == subProperty || schemaDeclaration.getProperty() == sameRole )
                {
                  newProperties = newProperties + schemaDeclaration.getValue().toInt
                }
            }
          }
        else if (property < 0 && schema.get(property * -1) != None)
          {
            val propertyRelations = schema.get(property * -1).get

            propertyRelations.foreach { schemaDeclaration =>

              if ( schemaDeclaration.getProperty() == inverseOf )
              {
                newProperties = newProperties + schemaDeclaration.getValue().toInt
              }
              else if ( schemaDeclaration.getProperty() == symetricProperty )
                {
                  newProperties = newProperties + schemaDeclaration.getSubject().toInt
                }
            }
          }


//        We add the in-going properties to the description of the entity
        if( property > 0 )
          {
            newProperties = newProperties + property
          }


    }

//    println(entity.getId()+" "+newProperties)

    return new Noeud(entity.getId(), newProperties, entity.getCoefficient())
  }

}

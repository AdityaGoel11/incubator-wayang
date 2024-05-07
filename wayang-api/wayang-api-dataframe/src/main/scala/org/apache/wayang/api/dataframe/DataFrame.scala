package org.apache.wayang.api.dataframe

import org.apache.commons.lang3.Validate
import org.apache.wayang.api.{PlanBuilder, basicDataUnitType, dataSetType}
import org.apache.wayang.basic.data.Record
import org.apache.wayang.basic.function.ProjectionDescriptor
import org.apache.wayang.basic.operators._
import org.apache.wayang.basic.types.RecordType
import org.apache.wayang.core.function.{FlatMapDescriptor, FunctionDescriptor, PredicateDescriptor, ReduceDescriptor, TransformationDescriptor}
import org.apache.wayang.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction}
import org.apache.wayang.core.optimizer.costs.{LoadProfileEstimator, LoadProfileEstimators}
import org.apache.wayang.core.plan.wayangplan._
import org.apache.wayang.core.types.{BasicDataUnitType, DataSetType, DataUnitType}
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.reflect.{ClassTag, classTag}
import org.apache.wayang.basic.operators.GlobalReduceOperator
import org.apache.wayang.basic.operators.MapOperator


class DataFrame[Out: ClassTag](val operator: ElementaryOperator, schema: List[String], outputIndex: Int = 0, group: List[String] = null)(implicit val planBuilder: PlanBuilder) {

  Validate.isTrue(operator.getNumOutputs > outputIndex)

  private[dataframe] def connectTo(operator: Operator, inputIndex: Int): Unit =
    this.operator.connectTo(outputIndex, operator, inputIndex)

  def project[Record: ClassTag](fieldNames: Seq[String]): DataFrame[Record] = {
    val recordType = new RecordType(this.schema: _*)
    val projectionOperator = MapOperator.createProjection(recordType,fieldNames: _*)
    this.connectTo(projectionOperator, 0)
    new DataFrame(projectionOperator, fieldNames.toList,0)
  }

  def aggregate(aggregateExpression: String): DataFrame[Record] = {

    val mapFunction = new addAggCols(aggregateExpression)
    val mapOperator = new MapOperator(new addAggCols(aggregateExpression),
      classOf[Record],
      classOf[Record])

    this.connectTo(mapOperator, 0)


    val globalReduceOperator = new GlobalReduceOperator(
      new ReduceDescriptor(
        new AggregateFunction(aggregateExpression),
        DataUnitType.createGrouped(classOf[Record]),
        DataUnitType.createBasic(classOf[Record])
      )
    )
    mapOperator.connectTo(globalReduceOperator, 0)

    val mapOperator2 = new MapOperator(new getResult(aggregateExpression),
      classOf[Record],
      classOf[Record])

    globalReduceOperator.connectTo(mapOperator2, 0)
    new DataFrame(mapOperator2, schema, 0)
  }

  def filter(expression: String): DataFrame[Record] = {
    val predicate: Record => Boolean = row => {
      val conditions = expression.split(";")
      conditions.forall { condition =>
        try {
          val columns = schema.zipWithIndex.toMap
          val comparison = condition.trim.split(" ")
          val columnName = comparison(0)
          val operator = comparison(1)
          val value = comparison(2)
          val columnIndex = columns(columnName)
          val recordValue = row.getField(columnIndex)
          // Evaluate the comparison expression
          val comparisonResult = operator match {
            case ">" => compare(recordValue, value) > 0
            case "<" => compare(recordValue, value) < 0
            case ">=" => compare(recordValue, value) >= 0
            case "<=" => compare(recordValue, value) <= 0
            case "==" => recordValue == value
            case "!=" => recordValue != value
            case _ => throw new IllegalArgumentException(s"Unsupported operator: $operator")
          }
          comparisonResult
        } catch {
          case ex: Throwable =>
            println(s"Caught error in filter method: ${ex.getMessage}") // Print the caught error
            false // Catch any errors and remove rows with invalid expressions
        }
      }
    }

    val predicateFunction = toSerializablePredicate(predicate)
    val predicateDescriptor = new PredicateDescriptor[Record](predicateFunction, classOf[Record])
    val filterOperator = new FilterOperator(predicateDescriptor)
    this.connectTo(filterOperator, 0)
    new DataFrame(filterOperator, schema, 0)
  }

  // Helper method for comparison
  private def compare(recordValue: Any, value: String): Int = {
    (recordValue, value) match {
      case (r: Comparable[_], v) => r.asInstanceOf[Comparable[String]].compareTo(v)
      case _ => throw new IllegalArgumentException("Cannot compare non-comparable values.")
    }
  }


  def distinct[Record: ClassTag](): DataFrame[Record] = {
    val recordType = new RecordType(this.schema:_*)
    val distinctOperator = new DistinctOperator(recordType.getTypeClass)
    this.connectTo(distinctOperator, 0)
    new DataFrame(distinctOperator,this.schema)
  }

  def withSchema(columnNames: Seq[String]): DataFrame[Record] = {
    // Create RecordType with the provided schema
    val udf: String => Record = (inputString: String) => {
      // Split the input string using comma as the delimiter
      val fields = inputString.split(",")
      // Create a Record using the split fields
      new Record(fields: _*)
    }

    // Create a Map operator with the provided UDF

    val mapOperator = new MapOperator(new TransformationDescriptor(
      toSerializableFunction(udf), basicDataUnitType[String],new RecordType(columnNames: _*), null
    ))

    // Connect the current DataFrame to the flatMap operator
    this.connectTo(mapOperator, 0)

    // Return a new DataFrame with the schema
    new DataFrame(mapOperator, columnNames.toList, 0)
  }

  def groupBy[Record: ClassTag](fieldNames: Seq[String]): DataFrame[Record] = {
    new DataFrame(this.operator,this.schema,0,fieldNames.toList)
  }

  def sort(fieldName: String): DataFrame[Record] = {
    val recordType = new RecordType(this.schema:_*);
    val idx = this.schema.indexOf(fieldName)
    val keyUdf: Record => Object = (record: Record) => {
      // Split the input string using comma as the delimiter
      record.getField(idx)
    }
    val sortOperator = new SortOperator(new TransformationDescriptor(
      toSerializableFunction(keyUdf), recordType, basicDataUnitType[Object]))
    this.connectTo(sortOperator, 0)
    new DataFrame(sortOperator, this.schema, 0)
  }

  def union(that:DataFrame[Record]) : DataFrame[Record] = {
    require(this.planBuilder eq that.planBuilder, s"$this and $that must use the same plan builders.")
    val unionAllOperator = new UnionAllOperator(dataSetType[Record])
    this.connectTo(unionAllOperator, 0)
    that.connectTo(unionAllOperator, 1)
    new DataFrame(unionAllOperator, this.schema, 0)
  }

  def intersect(that: DataFrame[Record]): DataFrame[Record] = {
    require(this.planBuilder eq that.planBuilder, s"$this and $that must use the same plan builders.")
    val intersectOperator = new IntersectOperator(dataSetType[Record])
    this.connectTo(intersectOperator, 0)
    that.connectTo(intersectOperator, 1)
    new DataFrame(intersectOperator, this.schema, 0)
  }

  def collect() = {
    // Set up the sink.
    val recordType = new RecordType(this.schema:_*);
    val collector = new java.util.LinkedList[recordType.type ]()
    val sink = LocalCallbackSink.createStdoutSink(recordType.getTypeClass);
    sink.setName("collect()")
    this.connectTo(sink, 0)

    // Do the execution.
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()

    // Return the collected values.
        collector.asScala
  }


  class getResult(aggregateExpression: String) extends FunctionDescriptor.SerializableFunction[Record, Record] {
    override def apply(record: Record): Record = {
      val l = record.size()

      val aggregations = scala.collection.mutable.ListBuffer[String]()
      val columns = scala.collection.mutable.ListBuffer[String]()

      aggregateExpression.split("\\W+").grouped(2).foreach {
        case Array(agg, col) =>
          aggregations += agg.toUpperCase()
          columns += col.toLowerCase()
        case _ => throw new IllegalArgumentException("Invalid aggregation expression")
      }

      // Convert mutable lists to immutable lists
      val aggList = aggregations.toList
      val colList = columns.toList

      val outputRecordSize = aggList.size
      val resValues = new Array[Object](outputRecordSize)

      var i = 0
      var j = 0

      i = l - aggList.size - 1
      for (agg <- aggList) {
        if (agg == "AVG") {
          resValues(j) = record.getDouble(i) / record.getDouble(l - 1) : java.lang.Double
        } else {
          resValues(j) = record.getField(i)
        }
        j += 1
        i += 1
      }

      new Record(resValues: _*)
    }
  }


  class addAggCols(aggregateExpression: String) extends FunctionDescriptor.SerializableFunction[Record, Record] {
    override def apply(record: Record): Record = {
      val aggregations = scala.collection.mutable.ListBuffer[String]()
      val columns = scala.collection.mutable.ListBuffer[String]()

      aggregateExpression.split("\\W+").grouped(2).foreach {
        case Array(agg, col) =>
          aggregations += agg.toUpperCase()
          columns += col.toLowerCase()
        case _ => throw new IllegalArgumentException("Invalid aggregation expression")
      }

      // Convert mutable lists to immutable lists
      val aggList = aggregations.toList
      val colList = columns.toList

      val l = record.size()
      val newRecordSize = l + aggList.size + 1
      val resValues = new Array[Object](l + colList.size + 1)

      var i = 0
      while (i < l) {
        resValues(i) = record.getField(i)
        i += 1
      }
      for ((agg, col) <- aggList.zip(colList)) {
        if (agg == "COUNT") {
          resValues(i) = 1 : java.lang.Integer
        } else {
          val columnIndex = schema.indexOf(col)
          if (columnIndex == -1) {
            throw new IllegalArgumentException(s"Column '$col' not found in schema")
          }
          resValues(i) = record.getField(columnIndex)
        }
        i += 1
      }
      resValues(newRecordSize - 1) = 1 : java.lang.Integer
      new Record(resValues: _*)
    }
  }

  class AggregateFunction(aggregateExpression: String) extends FunctionDescriptor.SerializableBinaryOperator[Record] {
    override def apply(record1: Record, record2: Record): Record = {
      val aggregations = scala.collection.mutable.ListBuffer[String]()
      val columns = scala.collection.mutable.ListBuffer[String]()

      aggregateExpression.split("\\W+").grouped(2).foreach {
        case Array(agg, col) =>
          aggregations += agg.toUpperCase()
          columns += col.toLowerCase()
        case _ => throw new IllegalArgumentException("Invalid aggregation expression")
      }

      // Convert mutable lists to immutable lists
      val aggList = aggregations.toList
      val colList = columns.toList

      val l = record1.size()
      val resValues = new Array[Object](l)
      var i = 0
      var countDone = false

      while (i < l - aggList.size - 1) {
        resValues(i) = record1.getField(i)
        i += 1
      }
      for ((agg, col) <- aggList.zip(colList)) {
        val val1 = record1.getField(i).toString.toDouble
        val val2 = record2.getField(i).toString.toDouble

        agg match {
          case "SUM" =>
            resValues(i) = val1 + val2 : java.lang.Double
          case "MIN" =>
            resValues(i) = Math.min(val1, val2) : java.lang.Double
          case "MAX" =>
            resValues(i) = Math.max(val1, val2) : java.lang.Double
          case "COUNT" =>
            resValues(i) = val1 + val2 : java.lang.Double
          case "AVG" =>
            resValues(i) = val1 + val2 : java.lang.Double
            if (!countDone) {
              resValues(l - 1) = record1.getInt(l - 1) + record2.getInt(l - 1) : java.lang.Integer
              countDone = true
            }
          case _ =>
            throw new IllegalArgumentException(s"Unsupported aggregation: $agg")
        }
        i += 1
      }

      new Record(resValues: _*)
    }
  }

}



// Method to compute the average value of a column
//  def avg(fieldName: String): DataFrame[Record] = {
//    // Define a function to calculate the sum and count of values
//    val sumFunction: SerializableBinaryOperator[Double] = new SerializableBinaryOperator[Double] {
//      override def apply(t1: Double, t2: Double): Double = t1 + t2
//    }
//    val countFunction: SerializableBinaryOperator[Long] = new SerializableBinaryOperator[Long] {
//      override def apply(t1: Long, t2: Long): Long = t1 + t2
//    }
//
//    // Create a ReduceDescriptor for finding the sum
//    val sumDescriptor: ReduceDescriptor[Double] = new ReduceDescriptor[Double](
//      sumFunction,
//      org.apache.wayang.core.types.DataUnitType.createGroupedUnchecked(classOf[Double]),
//      new BasicDataUnitType[Double](classOf[Double])
//    )
//
//    // Create a ReduceDescriptor for counting the values
//    val countDescriptor: ReduceDescriptor[Long] = new ReduceDescriptor[Long](
//      countFunction,
//      org.apache.wayang.core.types.DataUnitType.createGroupedUnchecked(classOf[Long]),
//      new BasicDataUnitType[Long](classOf[Long])
//    )
//
//    // Create a GlobalReduceOperator for finding the sum
//    val sumOperator: GlobalReduceOperator[Double] = new GlobalReduceOperator[Double](sumDescriptor)
//
//    // Create a GlobalReduceOperator for counting the values
//    val countOperator: GlobalReduceOperator[Long] = new GlobalReduceOperator[Long](countDescriptor)
//
//    // Connect the current DataFrame to the sum and count operators
//    this.connectTo(sumOperator, 0)
//    this.connectTo(countOperator, 0)
//
//    // Return a new DataFrame with the result
//    // Calculate the average using sum/count
//    val avgResult: Double = sumOperator.collect().head.asInstanceOf[Double] / countOperator.collect().head.asInstanceOf[Double]
//    new DataFrame[Record](avgResult, List(fieldName), 0)
//  }



// Method to compute the sum of a column
//  def sum(fieldName: String): DataFrame[Record] = {
//    // Define a function to extract the field value from the records
//    val extractValueFunction: SerializableBinaryOperator[Double] = new SerializableBinaryOperator[Double] {
//      override def apply(t1: Double, t2: Double): Double = t1 + t2
//    }
//
//    // Create a ReduceDescriptor for summation
//    val sumDescriptor: ReduceDescriptor[Record] = new ReduceDescriptor[Record](
//      extractValueFunction,
//      org.apache.wayang.core.types.DataUnitType.createGrouped(classOf[Record]),
//      org.apache.wayang.core.types.DataUnitType.createBasicUnchecked(classOf[Record])
//    )
//
//
//    // Create a GlobalReduceOperator for summation
//    val sumOperator: GlobalReduceOperator[Double] = new GlobalReduceOperator[Double](sumDescriptor)
//
//    // Connect the current DataFrame to the sum operator
//    this.connectTo(sumOperator, 0)
//
//    // Return a new DataFrame with the result
//    new DataFrame[Record](sumOperator, List(fieldName), 0)
//  }
//
//  // Method to compute the minimum value of a column
//  def min(fieldName: String): DataFrame[Record] = {
//    // Define a function to extract the field value from the records
//    val extractValueFunction: SerializableBinaryOperator[Double] = new SerializableBinaryOperator[Double] {
//      override def apply(t1: Double, t2: Double): Double = Math.min(t1, t2)
//    }
//
//    // Create a ReduceDescriptor for finding the minimum
//    val minDescriptor: ReduceDescriptor[Double] = new ReduceDescriptor[Double](
//      extractValueFunction,
//      DataUnitType.createGrouped(classOf[Record]),
//      DataUnitType.createBasicUnchecked(classOf[Record])
//    )
//
//    // Create a GlobalReduceOperator for finding the minimum
//    val minOperator: GlobalReduceOperator[Double] = new GlobalReduceOperator[Double](minDescriptor)
//
//    // Connect the current DataFrame to the min operator
//    this.connectTo(minOperator, 0)
//
//    // Return a new DataFrame with the result
//    new DataFrame[Record](minOperator, List(fieldName), 0)
//  }
//
//  // Method to compute the maximum value of a column
//  def max(fieldName: String): DataFrame[Record] = {
//    // Define a function to extract the field value from the records
//    val extractValueFunction: SerializableBinaryOperator[Double] = new SerializableBinaryOperator[Double] {
//      override def apply(t1: Double, t2: Double): Double = Math.max(t1, t2)
//    }
//
//    // Create a ReduceDescriptor for finding the maximum
//    val maxDescriptor: ReduceDescriptor[Double] = new ReduceDescriptor[Double](
//      extractValueFunction,
//      DataUnitType.createGrouped(classOf[Record]),
//      DataUnitType.createBasicUnchecked(classOf[Record])
//    )
//
//    // Create a GlobalReduceOperator for finding the maximum
//    val maxOperator: GlobalReduceOperator[Double] = new GlobalReduceOperator[Double](maxDescriptor)
//
//    // Connect the current DataFrame to the max operator
//    this.connectTo(maxOperator, 0)
//
//    // Return a new DataFrame with the result
//    new DataFrame[Record](maxOperator, List(fieldName), 0)
//  }
//
//
//
//  // Method to compute the count of non-null values in a column
//  def count(fieldName: String): DataFrame[Record] = {
//    // Define a function to count the non-null values
//    val countFunction: SerializableBinaryOperator[Long] = new SerializableBinaryOperator[Long] {
//      override def apply(t1: Long, t2: Long): Long = t1 + t2
//    }
//
//    // Create a ReduceDescriptor for counting the values
//    val countDescriptor: ReduceDescriptor[Long] = new ReduceDescriptor[Long](
//      countFunction,
//      DataUnitType.createGrouped(classOf[Record]),
//      DataUnitType.createBasicUnchecked(classOf[Record])
//    )
//
//    // Create a GlobalReduceOperator for counting the values
//    val countOperator: GlobalReduceOperator[Long] = new GlobalReduceOperator[Long](countDescriptor)
//
//    // Connect the current DataFrame to the count operator
//    this.connectTo(countOperator, 0)
//
//    // Return a new DataFrame with the result
//    new DataFrame[Record](countOperator, List(fieldName), 0)
//  }
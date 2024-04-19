package org.apache.wayang.api.dataframe

import org.apache.commons.lang3.Validate
import org.apache.wayang.api.{PlanBuilder, basicDataUnitType, dataSetType}
import org.apache.wayang.basic.data.Record
import org.apache.wayang.basic.function.ProjectionDescriptor
import org.apache.wayang.basic.operators._
import org.apache.wayang.basic.types.RecordType
import org.apache.wayang.core.function.{FlatMapDescriptor, TransformationDescriptor}
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction
import org.apache.wayang.core.optimizer.costs.{LoadProfileEstimator, LoadProfileEstimators}
import org.apache.wayang.core.plan.wayangplan._
import org.apache.wayang.core.types.{BasicDataUnitType, DataSetType}

import scala.collection.JavaConverters._
import java.util
import scala.reflect.ClassTag

class DataFrame[Out: ClassTag](val operator: ElementaryOperator, schema: List[String], outputIndex: Int = 0)(implicit val planBuilder: PlanBuilder) {

  Validate.isTrue(operator.getNumOutputs > outputIndex)


  private[dataframe] def connectTo(operator: Operator, inputIndex: Int): Unit =
    this.operator.connectTo(outputIndex, operator, inputIndex)


  def project[Record: ClassTag](fieldNames: Seq[String]): DataFrame[Record] = {
    val recordType = new RecordType(this.schema: _*)
    val projectionOperator = MapOperator.createProjection(recordType,fieldNames: _*)
    this.connectTo(projectionOperator, 0)
    new DataFrame(projectionOperator, fieldNames.toList,0)
  }

  def withSchema(columnNames: Seq[String]): DataFrame[Record] = {
    // Create RecordType with the provided schema
    val udf: String => Record = (inputString: String) => {
      // Split the input string using comma as the delimiter
      val fields = inputString.split(" ")
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

  def collect() = {
    // Set up the sink.
    val recordType = new RecordType(this.schema:_*);
    val collector = new java.util.LinkedList[recordType.type ]()
    val sink = LocalCallbackSink.createStdoutSink(recordType.getTypeClass.getClass);
    sink.setName("collect()")
    this.connectTo(sink, 0)

    // Do the execution.
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()

    // Return the collected values.
//    collector.asScala
  }
}

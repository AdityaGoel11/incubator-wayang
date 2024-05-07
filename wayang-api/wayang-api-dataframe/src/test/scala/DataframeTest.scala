/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.function.ExecutionContext
import org.apache.wayang.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.apache.wayang.core.function.ExecutionContext
import org.apache.wayang.core.plan.wayangplan.{Operator, UnaryToUnaryOperator}
import org.apache.wayang.core.types.DataSetType
import org.apache.wayang.core.util.ReflectionUtils


import org.apache.wayang.api.PlanBuilder
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.java.Java


object DataframeTest {
  def main(args: Array[String]): Unit = {
    val configuration = new Configuration()

    val wayangContext = new WayangContext(configuration).withPlugin(Java.basicPlugin())
    val planBuilder = new PlanBuilder(wayangContext, "test")

    val homeDirectory: String = System.getProperty("user.home")

    val start = System.currentTimeMillis()

    val c = planBuilder
      .readCsvFile(s"file://$homeDirectory/data.txt")
      .withSchema(List("name","age","gender","city","income"))
      .project(List("name", "age", "income"))
      .aggregate("sum(age), max(income)")
      .collect()

    val end = System.currentTimeMillis()

    // Print time taken
    println("Time taken: " + (end - start) + " milliseconds")

  }
}

//object DataframeTest {
//  def main(args: Array[String]): Unit = {
//    val configuration = new Configuration()
//
//    val wayangContext = new WayangContext(configuration).withPlugin(Java.basicPlugin())
//    val planBuilder = new PlanBuilder(wayangContext, "test")
//
//    val homeDirectory: String = System.getProperty("user.home")
//    val df = planBuilder
//      .readCsvFile(s"file://$homeDirectory/data.txt")
//      .withSchema(List("Name","Age","Gender","City","Income"))
//      .filter("Age > 30;Gender == Male")
//      .collect()

    // Perform aggregation
//    val aggregatedDF = df.aggregate("avg(Age)", "sum(Income)")
//      .collect()

    // Collect the results
//    val results = aggregatedDF.collect()
    //    planBuilder.buildAndExecute()
//  }
//}




//import org.apache.wayang.api.PlanBuilder
//import org.apache.wayang.core.api.{Configuration, WayangContext}
//import org.apache.wayang.java.Java
//
//object DataframeTest {
//  def main(args: Array[String]): Unit = {
//    val configuration = new Configuration()
//
//    val wayangContext = new WayangContext(configuration).withPlugin(Java.basicPlugin())
//    val planBuilder = new PlanBuilder(wayangContext, "test")
//
//    val homeDirectory: String = System.getProperty("user.home")
//
//    // Read the CSV file
//    val dataframe = planBuilder
//      .readCsvFile(s"file://$homeDirectory/data.txt")
//      .withSchema(List("Name","Age","Gender","City","Education","Income"))
//
//    // Apply filter operation
////    val filteredDataframe = dataframe.filter("Age > 30")
//
//    // Project to select required columns
//    val projectedDataframe = dataframe.project(List("Name","Age", "Gender")).collect()
//
//    // Collect the results
////    val result = projectedDataframe.collect()
////
////    // Print the result
////    result.foreach(println)
//  }
//}

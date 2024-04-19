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

import org.apache.wayang.api.PlanBuilder
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.java.Java


object DataframeTest {
  def main(args: Array[String]): Unit = {
    val configuration = new Configuration()

    val wayangContext = new WayangContext(configuration).withPlugin(Java.basicPlugin())
    val planBuilder = new PlanBuilder(wayangContext, "test")

    val homeDirectory: String = System.getProperty("user.home")
    val c = planBuilder
      .readCsvFile(s"file://$homeDirectory/demo.txt")
      .withSchema(List("name","age","size"))
      .project(List("name"))
      .collect()
//    c.foreach(println)
//    planBuilder.buildAndExecute()
  }
}
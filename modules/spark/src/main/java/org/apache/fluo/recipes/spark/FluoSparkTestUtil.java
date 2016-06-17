/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.recipes.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Utility code for Fluo/Spark testing
 */
public class FluoSparkTestUtil {

  /**
   * Creates Java Spark Context for unit/integration testing
   * 
   * @param testName Name of test being run
   * @return JavaSparkContext
   */
  public static JavaSparkContext newSparkContext(String testName) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName(testName);
    sparkConf.set("spark.app.id", testName);
    sparkConf.set("spark.ui.port", "4444");
    return new JavaSparkContext(sparkConf);
  }

}

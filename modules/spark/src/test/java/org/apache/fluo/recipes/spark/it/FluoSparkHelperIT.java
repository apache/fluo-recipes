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

package org.apache.fluo.recipes.spark.it;

import java.util.List;

import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.recipes.spark.FluoSparkHelper;
import org.apache.fluo.recipes.spark.FluoSparkTestUtil;
import org.apache.fluo.recipes.test.AccumuloExportITBase;
import org.apache.fluo.recipes.test.FluoITHelper;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class FluoSparkHelperIT extends AccumuloExportITBase {

  static JavaSparkContext ctx;

  public FluoSparkHelperIT() {
    super(false);
  }

  @BeforeClass
  public static void setupIT() {
    ctx = FluoSparkTestUtil.newSparkContext("fluo-spark-helper");
  }

  @AfterClass
  public static void teardownIT() {
    ctx.stop();
  }

  private List<RowColumnValue> getData() {
    return FluoITHelper.parse("arow|acf|acq|aval", "brow|bcf|bcq|bval", "crow|ccf|ccq|cval");
  }

  @Test
  public void testAccumuloBulkImport() throws Exception {
    FluoSparkHelper fsh =
        new FluoSparkHelper(getFluoConfiguration(), ctx.hadoopConfiguration(), new Path("/tmp/"));
    List<RowColumnValue> expected = getData();
    final String accumuloTable = "table1";
    getAccumuloConnector().tableOperations().create(accumuloTable);
    fsh.bulkImportRcvToAccumulo(FluoSparkHelper.toPairRDD(ctx.parallelize(expected)),
        accumuloTable, new FluoSparkHelper.BulkImportOptions());
    Assert.assertTrue(FluoITHelper.verifyAccumuloTable(getAccumuloConnector(), accumuloTable,
        expected));
  }

  @Test
  public void testFluoBulkImport() throws Exception {
    FluoSparkHelper fsh =
        new FluoSparkHelper(getFluoConfiguration(), ctx.hadoopConfiguration(), new Path("/tmp/"));
    List<RowColumnValue> expected = getData();
    fsh.bulkImportRcvToFluo(FluoSparkHelper.toPairRDD(ctx.parallelize(expected)),
        new FluoSparkHelper.BulkImportOptions());

    try (MiniFluo miniFluo = FluoFactory.newMiniFluo(getFluoConfiguration())) {
      Assert.assertNotNull(miniFluo);
      Assert.assertTrue(FluoITHelper.verifyFluoTable(getFluoConfiguration(), expected));

      List<RowColumnValue> actualRead = FluoSparkHelper.toRcvRDD(fsh.readFromFluo(ctx)).collect();
      Assert.assertTrue(FluoITHelper.verifyRowColumnValues(expected, actualRead));
    }
  }
}

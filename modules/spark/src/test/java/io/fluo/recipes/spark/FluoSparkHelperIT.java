/*
 * Copyright 2016 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.recipes.spark;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumnValue;
import io.fluo.recipes.test.FluoITHelper;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FluoSparkHelperIT {

  static FluoITHelper fluoIT;
  static JavaSparkContext ctx;

  @BeforeClass
  public static void setupIT() {
    fluoIT = new FluoITHelper(Paths.get("target/miniAccumulo"));
    ctx = FluoSparkTestUtil.newSparkContext("fluo-spark-helper");
  }

  @AfterClass
  public static void teardownIT() {
    ctx.stop();
    fluoIT.close();
  }

  @Before
  public void setupTest() {
    fluoIT.setupFluo();
  }

  @After
  public void teardownTest() {
    fluoIT.teardownFluo();
  }

  private List<RowColumnValue> getData() {
    List<RowColumnValue> rcvList = new ArrayList<>();
    rcvList.add(new RowColumnValue("arow", new Column("acf", "acq"), "aval"));
    rcvList.add(new RowColumnValue("brow", new Column("bcf", "bcq"), "bval"));
    rcvList.add(new RowColumnValue("crow", new Column("ccf", "ccq"), "cval"));
    return rcvList;
  }

  @Test
  public void testAccumuloBulkImport() throws Exception {
    FluoSparkHelper fsh =
        new FluoSparkHelper(fluoIT.getFluoConfig(), ctx.hadoopConfiguration(), new Path("/tmp/"));
    List<RowColumnValue> expected = getData();
    final String accumuloTable = "table1";
    fluoIT.getAccumuloConnector().tableOperations().create(accumuloTable);
    fsh.bulkImportRcvToAccumulo(FluoSparkHelper.toPairRDD(ctx.parallelize(expected)),
        accumuloTable, new FluoSparkHelper.BulkImportOptions());
    Assert.assertTrue(fluoIT.verifyAccumuloTable(accumuloTable, expected));
  }

  @Test
  public void testFluoBulkImport() throws Exception {
    FluoSparkHelper fsh =
        new FluoSparkHelper(fluoIT.getFluoConfig(), ctx.hadoopConfiguration(), new Path("/tmp/"));
    List<RowColumnValue> expected = getData();
    fsh.bulkImportRcvToFluo(FluoSparkHelper.toPairRDD(ctx.parallelize(expected)),
        new FluoSparkHelper.BulkImportOptions());
    Assert.assertTrue(fluoIT.verifyFluoTable(expected));

    List<RowColumnValue> actualRead = FluoSparkHelper.toRcvRDD(fsh.readFromFluo(ctx)).collect();
    Assert.assertTrue(fluoIT.verifyRowColumnValues(expected, actualRead));
  }
}

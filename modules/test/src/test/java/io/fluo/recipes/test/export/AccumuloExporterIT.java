/*
 * Copyright 2014 Fluo authors (see AUTHORS)
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

package io.fluo.recipes.test.export;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Transaction;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.mini.MiniFluo;
import io.fluo.recipes.accumulo.export.AccumuloExporter;
import io.fluo.recipes.accumulo.export.TableInfo;
import io.fluo.recipes.export.ExportQueue;
import io.fluo.recipes.test.AccumuloExportITBase;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class AccumuloExporterIT extends AccumuloExportITBase {

  private String et;
  public static final String QUEUE_ID = "aeqt";

  @Override
  public void preFluoInitHook() throws Exception {

    FluoConfiguration fluoConfig = getFluoConfiguration();
    ExportQueue.configure(fluoConfig,
        new ExportQueue.Options(QUEUE_ID, AccumuloExporter.class.getName(), String.class.getName(),
            TestExport.class.getName(), 5));

    // create and configure export table
    et = "export" + tableCounter.getAndIncrement();
    getAccumuloConnector().tableOperations().create(et);
    MiniAccumuloCluster miniAccumulo = getMiniAccumuloCluster();
    AccumuloExporter.setExportTableInfo(fluoConfig.getAppConfiguration(), QUEUE_ID, new TableInfo(
        miniAccumulo.getInstanceName(), miniAccumulo.getZooKeepers(), ACCUMULO_USER,
        ACCUMULO_PASSWORD, et));
  }

  @Test
  public void testAccumuloExport() throws Exception {

    ExportQueue<String, TestExport> teq =
        ExportQueue.getInstance(QUEUE_ID, getFluoConfiguration().getAppConfiguration());

    Assert.assertEquals(6, getFluoSplits().size());

    MiniFluo miniFluo = getMiniFluo();

    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {

      Map<String, String> expected = new HashMap<>();

      try (Transaction tx = fc.newTransaction()) {
        export(teq, tx, expected, "0001", "abc");
        export(teq, tx, expected, "0002", "def");
        export(teq, tx, expected, "0003", "ghi");
        tx.commit();
      }

      miniFluo.waitForObservers();
      Assert.assertEquals(expected, getExports());

      try (Transaction tx = fc.newTransaction()) {
        export(teq, tx, expected, "0001", "xyz");
        tx.commit();
      }

      miniFluo.waitForObservers();
      Assert.assertEquals(expected, getExports());

      try (Transaction tx = fc.newTransaction()) {
        export(teq, tx, expected, "0001", "zzz");
        tx.commit();
      }

      try (Transaction tx = fc.newTransaction()) {
        export(teq, tx, expected, "0001", "mmm");
        tx.commit();
      }

      miniFluo.waitForObservers();
      Assert.assertEquals(expected, getExports());

      Random rand = new Random(42);
      for (int i = 0; i < 1000; i++) {
        String k = String.format("%04d", rand.nextInt(100));
        String v = String.format("%04d", rand.nextInt(10000));

        try (Transaction tx = fc.newTransaction()) {
          export(teq, tx, expected, k, v);
          tx.commit();
        }
      }

      miniFluo.waitForObservers();
      Assert.assertEquals(expected, getExports());
    }
  }

  private void export(ExportQueue<String, TestExport> teq, Transaction tx,
      Map<String, String> expected, String k, String v) {
    teq.add(tx, k, new TestExport(v));
    expected.put(k, v);
  }

  private Collection<Text> getFluoSplits() throws Exception {
    return getAccumuloConnector().tableOperations().listSplits(
        getFluoConfiguration().getAccumuloTable());
  }

  private Map<String, String> getExports() throws Exception {
    Scanner scanner = getAccumuloConnector().createScanner(et, Authorizations.EMPTY);
    Map<String, String> ret = new HashMap<>();

    for (Entry<Key, Value> entry : scanner) {
      String k = entry.getKey().getRowData().toString();
      Assert.assertFalse(ret.containsKey(k));
      ret.put(k, entry.getValue().toString());
    }

    return ret;
  }
}

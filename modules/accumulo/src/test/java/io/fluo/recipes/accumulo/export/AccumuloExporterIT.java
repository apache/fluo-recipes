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

package io.fluo.recipes.accumulo.export;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Transaction;
import io.fluo.recipes.export.ExportQueue;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class AccumuloExporterIT extends AccumuloITBase {

  private String et;
  public static final String QUEUE_ID = "aeqt";

  @Override
  public void setupExporter() throws Exception {

    ExportQueue.configure(props, new ExportQueue.Options(QUEUE_ID, TestExporter.class,
        String.class, String.class, 5));

    // create and configure export table
    et = "export" + tableCounter.getAndIncrement();
    cluster.getConnector("root", "secret").tableOperations().create(et);
    AccumuloExporter.setExportTableInfo(props.getAppConfiguration(), QUEUE_ID, new TableInfo(
        cluster.getInstanceName(), cluster.getZooKeepers(), "root", "secret", et));

  }

  @Test
  public void testAccumuloExport() throws Exception {

    ExportQueue<String, String> teq =
        ExportQueue.getInstance(QUEUE_ID, props.getAppConfiguration());

    Assert.assertEquals(2, getFluoSplits().size());

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

  private void export(ExportQueue<String, String> teq, Transaction tx,
      Map<String, String> expected, String k, String v) {
    teq.add(tx, k, v);
    expected.put(k, v);
  }

  private Collection<Text> getFluoSplits() throws Exception {
    return cluster.getConnector("root", "secret").tableOperations()
        .listSplits(props.getAccumuloTable());
  }

  private Map<String, String> getExports() throws Exception {
    Scanner scanner =
        cluster.getConnector("root", "secret").createScanner(et, Authorizations.EMPTY);
    Map<String, String> ret = new HashMap<>();

    for (Entry<Key, Value> entry : scanner) {
      String k = entry.getKey().getRowData().toString();
      Assert.assertFalse(ret.containsKey(k));
      ret.put(k, entry.getValue().toString());
    }

    return ret;
  }
}

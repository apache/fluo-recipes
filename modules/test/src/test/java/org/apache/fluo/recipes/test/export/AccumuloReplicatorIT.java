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

package org.apache.fluo.recipes.test.export;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.recipes.accumulo.export.AccumuloExporter;
import org.apache.fluo.recipes.accumulo.export.AccumuloReplicator;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.apache.fluo.recipes.core.transaction.TxLog;
import org.apache.fluo.recipes.test.AccumuloExportITBase;
import org.apache.fluo.recipes.core.transaction.RecordingTransaction;
import org.apache.fluo.recipes.core.types.StringEncoder;
import org.apache.fluo.recipes.core.types.TypeLayer;
import org.apache.fluo.recipes.core.types.TypedTransaction;
import org.junit.Assert;
import org.junit.Test;

public class AccumuloReplicatorIT extends AccumuloExportITBase {

  private String exportTable;
  public static final String QUEUE_ID = "repq";
  private TypeLayer tl = new TypeLayer(new StringEncoder());

  @Override
  public void preFluoInitHook() throws Exception {

    // create and configure export table
    exportTable = "export" + tableCounter.getAndIncrement();
    getAccumuloConnector().tableOperations().create(exportTable);

    MiniAccumuloCluster miniAccumulo = getMiniAccumuloCluster();

    ExportQueue.configure(getFluoConfiguration(), new ExportQueue.Options(QUEUE_ID,
        AccumuloReplicator.class.getName(), String.class.getName(), TxLog.class.getName(), 5)
        .setExporterConfiguration(new AccumuloExporter.Configuration(
            miniAccumulo.getInstanceName(), miniAccumulo.getZooKeepers(), ACCUMULO_USER,
            ACCUMULO_PASSWORD, exportTable)));
  }

  @Test
  public void testAccumuloReplicator() throws Exception {

    ExportQueue<String, TxLog> eq =
        ExportQueue.getInstance(QUEUE_ID, getFluoConfiguration().getAppConfiguration());

    MiniFluo miniFluo = getMiniFluo();
    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {

      Map<String, String> expected = new HashMap<>();

      try (Transaction tx = fc.newTransaction()) {
        RecordingTransaction rtx = RecordingTransaction.wrap(tx, AccumuloReplicator.getFilter());
        TypedTransaction ttx = tl.wrap(rtx);
        write(ttx, expected, "k1", "v1");
        write(ttx, expected, "k2", "v2");
        write(ttx, expected, "k3", "v3");
        eq.add(tx, "q1", rtx.getTxLog());
        tx.commit();
      }

      miniFluo.waitForObservers();
      Assert.assertEquals(expected, getExports());

      try (Transaction tx = fc.newTransaction()) {
        RecordingTransaction rtx = RecordingTransaction.wrap(tx, AccumuloReplicator.getFilter());
        TypedTransaction ttx = tl.wrap(rtx);
        write(ttx, expected, "k1", "v4");
        delete(ttx, expected, "k3");
        write(ttx, expected, "k2", "v5");
        write(ttx, expected, "k4", "v6");
        eq.add(tx, "q1", rtx.getTxLog());
        tx.commit();
      }

      miniFluo.waitForObservers();
      Assert.assertEquals(expected, getExports());

      try (Transaction tx = fc.newTransaction()) {
        RecordingTransaction rtx = RecordingTransaction.wrap(tx, AccumuloReplicator.getFilter());
        TypedTransaction ttx = tl.wrap(rtx);
        write(ttx, expected, "k2", "v7");
        write(ttx, expected, "k3", "v8");
        delete(ttx, expected, "k1");
        delete(ttx, expected, "k4");
        eq.add(tx, "q1", rtx.getTxLog());
        tx.commit();
      }

      miniFluo.waitForObservers();
      Assert.assertEquals(expected, getExports());
    }
  }

  private void write(TypedTransaction ttx, Map<String, String> expected, String key, String value) {
    ttx.mutate().row(key).fam("fam").qual("qual").set(value);
    expected.put(key, value);
  }

  private void delete(TypedTransaction ttx, Map<String, String> expected, String key) {
    ttx.mutate().row(key).fam("fam").qual("qual").delete();
    expected.remove(key);
  }

  private Map<String, String> getExports() throws Exception {
    Scanner scanner = getAccumuloConnector().createScanner(exportTable, Authorizations.EMPTY);
    Map<String, String> ret = new HashMap<>();

    for (Entry<Key, Value> entry : scanner) {
      String k = entry.getKey().getRowData().toString();
      Assert.assertFalse(ret.containsKey(k));
      ret.put(k, entry.getValue().toString());
    }
    return ret;
  }
}

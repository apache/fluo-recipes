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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.recipes.accumulo.export.function.AccumuloExporter;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.apache.fluo.recipes.test.AccumuloExportITBase;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class AccumuloExporterIT extends AccumuloExportITBase {

  private String exportTable;
  public static final String QUEUE_ID = "aeqt";

  public static class AccumuloExporterObserverProvider implements ObserverProvider {

    @Override
    public void provide(Registry obsRegistry, Context ctx) {
      SimpleConfiguration appCfg = ctx.getAppConfiguration();

      ExportQueue<String, String> teq = ExportQueue.getInstance(QUEUE_ID, appCfg);

      teq.registerObserver(obsRegistry, new AccumuloExporter<>(QUEUE_ID, appCfg, (export,
          mutConsumer) -> {
        Mutation m = new Mutation(export.getKey());
        m.put("cf", "cq", export.getSequence(), export.getValue());
        mutConsumer.accept(m);
      }));
    }

  }

  @Override
  public void preFluoInitHook() throws Exception {

    // create and configure export table
    exportTable = "export" + tableCounter.getAndIncrement();
    getAccumuloConnector().tableOperations().create(exportTable);

    MiniAccumuloCluster miniAccumulo = getMiniAccumuloCluster();

    getFluoConfiguration().setObserverProvider(AccumuloExporterObserverProvider.class);

    ExportQueue.configure(QUEUE_ID).keyType(String.class).valueType(String.class).buckets(5)
        .bucketsPerTablet(1).save(getFluoConfiguration());

    AccumuloExporter.configure(QUEUE_ID)
        .instance(miniAccumulo.getInstanceName(), miniAccumulo.getZooKeepers())
        .credentials(ACCUMULO_USER, ACCUMULO_PASSWORD).table(exportTable)
        .save(getFluoConfiguration());

  }

  @Test
  public void testAccumuloExport() throws Exception {

    ExportQueue<String, String> teq =
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

  private void export(ExportQueue<String, String> teq, Transaction tx,
      Map<String, String> expected, String k, String v) {
    teq.add(tx, k, v);
    expected.put(k, v);
  }

  private Collection<Text> getFluoSplits() throws Exception {
    return getAccumuloConnector().tableOperations().listSplits(
        getFluoConfiguration().getAccumuloTable());
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

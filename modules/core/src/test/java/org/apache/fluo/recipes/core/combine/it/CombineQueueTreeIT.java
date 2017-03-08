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

package org.apache.fluo.recipes.core.combine.it;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.recipes.core.combine.ChangeObserver.Change;
import org.apache.fluo.recipes.core.combine.CombineQueue;
import org.apache.fluo.recipes.core.combine.SummingCombiner;
import org.apache.fluo.recipes.core.map.it.TestSerializer;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CombineQueueTreeIT {

  private static final String CQ_XYT_ID = "xyt";
  private static final String CQ_XY_ID = "xy";
  private static final String CQ_XT_ID = "xt";
  private static final String CQ_YT_ID = "yt";
  private static final String CQ_X_ID = "x";
  private static final String CQ_T_ID = "t";
  private static final String CQ_Y_ID = "y";

  public static class CqitObserverProvider implements ObserverProvider {

    @Override
    public void provide(Registry or, Context ctx) {
      // Link combine queues into a tree of combine queues.

      SimpleConfiguration appCfg = ctx.getAppConfiguration();
      CombineQueue<String, Long> xytCq = CombineQueue.getInstance(CQ_XYT_ID, appCfg);
      CombineQueue<String, Long> xyCq = CombineQueue.getInstance(CQ_XY_ID, appCfg);
      CombineQueue<String, Long> ytCq = CombineQueue.getInstance(CQ_YT_ID, appCfg);
      CombineQueue<String, Long> xtCq = CombineQueue.getInstance(CQ_XT_ID, appCfg);
      CombineQueue<String, Long> xCq = CombineQueue.getInstance(CQ_X_ID, appCfg);
      CombineQueue<String, Long> tCq = CombineQueue.getInstance(CQ_T_ID, appCfg);
      CombineQueue<String, Long> yCq = CombineQueue.getInstance(CQ_Y_ID, appCfg);

      xytCq.registerObserver(or, new SummingCombiner<>(), (tx, changes) -> {
        Map<String, Long> xtUpdates = new HashMap<>();
        Map<String, Long> ytUpdates = new HashMap<>();
        Map<String, Long> xyUpdates = new HashMap<>();
        for (Change<String, Long> change : changes) {
          String[] fields = change.getKey().split(":");
          long delta = change.getNewValue().orElse(0L) - change.getOldValue().orElse(0L);
          xtUpdates.merge(fields[0] + ":" + fields[2], delta, Long::sum);
          ytUpdates.merge(fields[1] + ":" + fields[2], delta, Long::sum);
          xyUpdates.merge(fields[0] + ":" + fields[1], delta, Long::sum);
        }

        xtCq.addAll(tx, xtUpdates);
        ytCq.addAll(tx, ytUpdates);
        xyCq.addAll(tx, xyUpdates);

        invert("xyt", tx, changes);
      });

      xyCq.registerObserver(or, new SummingCombiner<>(), (tx, changes) -> {
        Map<String, Long> xUpdates = new HashMap<>();
        Map<String, Long> yUpdates = new HashMap<>();
        for (Change<String, Long> change : changes) {
          String[] fields = change.getKey().split(":");
          long delta = change.getNewValue().orElse(0L) - change.getOldValue().orElse(0L);
          xUpdates.merge(fields[0], delta, Long::sum);
          yUpdates.merge(fields[1], delta, Long::sum);
        }

        xCq.addAll(tx, xUpdates);
        yCq.addAll(tx, yUpdates);

        invert("xy", tx, changes);
      });

      xtCq.registerObserver(or, new SummingCombiner<>(), (tx, changes) -> {
        Map<String, Long> tUpdates = new HashMap<>();
        for (Change<String, Long> change : changes) {
          String[] fields = change.getKey().split(":");
          long delta = change.getNewValue().orElse(0L) - change.getOldValue().orElse(0L);
          tUpdates.merge(fields[1], delta, Long::sum);
        }

        tCq.addAll(tx, tUpdates);

        invert("xt", tx, changes);
      });

      ytCq.registerObserver(or, new SummingCombiner<>(), (tx, changes) -> {
        invert("yt", tx, changes);
      });

      xCq.registerObserver(or, new SummingCombiner<>(), (tx, changes) -> {
        invert("x", tx, changes);
      });

      tCq.registerObserver(or, new SummingCombiner<>(), (tx, changes) -> {
        invert("t", tx, changes);
      });

      yCq.registerObserver(or, new SummingCombiner<>(), (tx, changes) -> {
        invert("y", tx, changes);
      });
    }

    private static void invert(String prefix, TransactionBase tx,
        Iterable<Change<String, Long>> changes) {
      for (Change<String, Long> change : changes) {
        change.getOldValue().ifPresent(
            val -> tx.delete("inv:" + prefix + ":" + val, new Column("inv", change.getKey())));
        change.getNewValue().ifPresent(
            val -> tx.set("inv:" + prefix + ":" + val, new Column("inv", change.getKey()), ""));
      }
    }

  }

  private MiniFluo miniFluo;

  @Before
  public void setUpFluo() throws Exception {
    FileUtils.deleteQuietly(new File("target/mini"));

    FluoConfiguration props = new FluoConfiguration();
    props.setApplicationName("eqt");
    props.setWorkerThreads(20);
    props.setMiniDataDir("target/mini");

    CombineQueue.configure(CQ_XYT_ID).keyType(String.class).valueType(Long.class).buckets(7)
        .save(props);
    CombineQueue.configure(CQ_XT_ID).keyType(String.class).valueType(Long.class).buckets(7)
        .save(props);
    CombineQueue.configure(CQ_XY_ID).keyType(String.class).valueType(Long.class).buckets(7)
        .save(props);
    CombineQueue.configure(CQ_YT_ID).keyType(String.class).valueType(Long.class).buckets(7)
        .save(props);
    CombineQueue.configure(CQ_X_ID).keyType(String.class).valueType(Long.class).buckets(7)
        .save(props);
    CombineQueue.configure(CQ_T_ID).keyType(String.class).valueType(Long.class).buckets(7)
        .save(props);
    CombineQueue.configure(CQ_Y_ID).keyType(String.class).valueType(Long.class).buckets(7)
        .save(props);

    props.setObserverProvider(CqitObserverProvider.class);

    SimpleSerializer.setSerializer(props, TestSerializer.class);

    miniFluo = FluoFactory.newMiniFluo(props);
  }

  private static Map<String, Long> merge(Map<String, Long> m1, Map<String, Long> m2) {
    Map<String, Long> ret = new HashMap<>(m1);
    m2.forEach((k, v) -> ret.merge(k, v, Long::sum));
    return ret;
  }

  private static Map<String, Long> rollup(Map<String, Long> m, String rollupFields) {
    boolean useX = rollupFields.contains("x");
    boolean useY = rollupFields.contains("y");
    boolean useTime = rollupFields.contains("t");

    Map<String, Long> ret = new HashMap<>();
    m.forEach((k, v) -> {
      String[] fields = k.split(":");
      String nk =
          (useX ? fields[0] : "") + (useY ? ((useX ? ":" : "") + fields[1]) : "")
              + (useTime ? ((useX || useY ? ":" : "") + fields[2]) : "");

      ret.merge(nk, v, Long::sum);
    });
    return ret;
  }

  private static Map<String, Long> readRollup(Snapshot snap, String rollupFields) {
    Map<String, Long> ret = new HashMap<>();

    String prefix = "inv:" + rollupFields + ":";
    for (RowColumnValue rcv : snap.scanner().over(Span.prefix("inv:" + rollupFields + ":")).build()) {
      String row = rcv.getsRow();
      long count = Long.valueOf(row.substring(prefix.length(), row.length()));
      Assert.assertNull(ret.put(rcv.getColumn().getsQualifier(), count));
    }

    return ret;
  }

  @Test
  public void testCqTree() {
    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {

      CombineQueue<String, Long> xytCq =
          CombineQueue.getInstance(CQ_XYT_ID, fc.getAppConfiguration());

      Map<String, Long> updates1 = new HashMap<>();
      updates1.put("5:4:23", 1L);
      updates1.put("5:5:23", 1L);
      updates1.put("7:5:23", 1L);
      updates1.put("9:2:23", 1L);
      updates1.put("5:4:29", 1L);
      updates1.put("6:6:29", 1L);
      updates1.put("7:5:29", 1L);
      updates1.put("9:3:29", 1L);
      updates1.put("3:3:31", 1L);

      try (Transaction tx = fc.newTransaction()) {
        xytCq.addAll(tx, updates1);
        tx.commit();
      }

      miniFluo.waitForObservers();

      try (Snapshot snap = fc.newSnapshot()) {
        for (String fieldsNames : new String[] {"x", "y", "t", "xy", "xt", "yt", "xyt"}) {
          Map<String, Long> expected = rollup(updates1, fieldsNames);
          Map<String, Long> actual = readRollup(snap, fieldsNames);

          Assert.assertEquals(expected, actual);
        }
      }

      Map<String, Long> updates2 = new HashMap<>();
      updates2.put("7:5:23", 1L);
      updates2.put("6:6:7", 1L);
      updates2.put("4:3:31", 1L);
      updates2.put("9:1:23", 1L);
      updates2.put("1:2:3", 1L);

      try (Transaction tx = fc.newTransaction()) {
        xytCq.addAll(tx, updates2);
        tx.commit();
      }

      miniFluo.waitForObservers();

      try (Snapshot snap = fc.newSnapshot()) {
        for (String fieldsNames : new String[] {"x", "y", "t", "xy", "xt", "yt", "xyt"}) {
          Map<String, Long> expected = rollup(merge(updates1, updates2), fieldsNames);
          Map<String, Long> actual = readRollup(snap, fieldsNames);

          Assert.assertEquals(expected, actual);
        }
      }
    }
  }

  @After
  public void tearDownFluo() throws Exception {
    if (miniFluo != null) {
      miniFluo.close();
    }
  }
}

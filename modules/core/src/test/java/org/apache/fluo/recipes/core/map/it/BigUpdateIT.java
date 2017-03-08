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

package org.apache.fluo.recipes.core.map.it;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.recipes.core.map.CollisionFreeMap;
import org.apache.fluo.recipes.core.map.Combiner;
import org.apache.fluo.recipes.core.map.Update;
import org.apache.fluo.recipes.core.map.UpdateObserver;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;
import org.apache.fluo.recipes.core.types.StringEncoder;
import org.apache.fluo.recipes.core.types.TypeLayer;
import org.apache.fluo.recipes.core.types.TypedSnapshot;
import org.apache.fluo.recipes.core.types.TypedTransactionBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This test configures a small buffer size and verifies that multiple passes are made to process
 * updates.
 */
@Deprecated
// TODO migrate to CombineQueue test when removing CFM
public class BigUpdateIT {
  private static final TypeLayer tl = new TypeLayer(new StringEncoder());

  private MiniFluo miniFluo;

  private CollisionFreeMap<String, Long> wcMap;

  static final String MAP_ID = "bu";

  public static class LongCombiner implements Combiner<String, Long> {

    @Override
    public Optional<Long> combine(String key, Iterator<Long> updates) {
      long[] count = new long[] {0};
      updates.forEachRemaining(l -> count[0] += l);
      return Optional.of(count[0]);
    }
  }

  static final Column DSCOL = new Column("debug", "sum");

  private static AtomicInteger globalUpdates = new AtomicInteger(0);

  public static class MyObserver extends UpdateObserver<String, Long> {

    @Override
    public void updatingValues(TransactionBase tx, Iterator<Update<String, Long>> updates) {
      TypedTransactionBase ttx = tl.wrap(tx);

      Map<String, Long> expectedOld = new HashMap<>();


      while (updates.hasNext()) {
        Update<String, Long> update = updates.next();

        if (update.getOldValue().isPresent()) {
          expectedOld.put("side:" + update.getKey(), update.getOldValue().get());
        }

        ttx.mutate().row("side:" + update.getKey()).col(DSCOL).set(update.getNewValue().get());
      }

      // get last values set to verify same as passed in old value
      Map<String, Long> actualOld =
          Maps.transformValues(
              ttx.get().rowsString(expectedOld.keySet()).columns(ImmutableSet.of(DSCOL))
                  .toStringMap(), m -> m.get(DSCOL).toLong());

      MapDifference<String, Long> diff = Maps.difference(expectedOld, actualOld);

      Assert.assertTrue(diff.toString(), diff.areEqual());

      globalUpdates.incrementAndGet();
    }
  }

  @Before
  public void setUpFluo() throws Exception {
    FileUtils.deleteQuietly(new File("target/mini"));

    FluoConfiguration props = new FluoConfiguration();
    props.setApplicationName("eqt");
    props.setWorkerThreads(20);
    props.setMiniDataDir("target/mini");

    SimpleSerializer.setSerializer(props, TestSerializer.class);

    CollisionFreeMap.configure(props, new CollisionFreeMap.Options(MAP_ID, LongCombiner.class,
        MyObserver.class, String.class, Long.class, 2).setBufferSize(1 << 10));

    miniFluo = FluoFactory.newMiniFluo(props);

    wcMap = CollisionFreeMap.getInstance(MAP_ID, props.getAppConfiguration());

    globalUpdates.set(0);
  }

  @After
  public void tearDownFluo() throws Exception {
    if (miniFluo != null) {
      miniFluo.close();
    }
  }

  @Test
  public void testBigUpdates() {
    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {
      updateMany(fc);

      miniFluo.waitForObservers();

      int numUpdates = 0;

      try (TypedSnapshot snap = tl.wrap(fc.newSnapshot())) {
        checkUpdates(snap, 1, 1000);
        numUpdates = globalUpdates.get();
        // there are two buckets, expect update processing at least twice per bucket
        Assert.assertTrue(numUpdates >= 4);
      }

      updateMany(fc);
      updateMany(fc);

      miniFluo.waitForObservers();

      try (TypedSnapshot snap = tl.wrap(fc.newSnapshot())) {
        checkUpdates(snap, 3, 1000);
        numUpdates = globalUpdates.get() - numUpdates;
        Assert.assertTrue(numUpdates >= 4);
      }

      for (int i = 0; i < 10; i++) {
        updateMany(fc);
      }

      miniFluo.waitForObservers();

      try (TypedSnapshot snap = tl.wrap(fc.newSnapshot())) {
        checkUpdates(snap, 13, 1000);
        numUpdates = globalUpdates.get() - numUpdates;
        Assert.assertTrue(numUpdates >= 4);
      }
    }
  }

  private void checkUpdates(TypedSnapshot snap, long expectedVal, long expectedRows) {
    RowScanner rows = snap.scanner().over(Span.prefix("side:")).byRow().build();

    int row = 0;

    for (ColumnScanner columns : rows) {
      Assert.assertEquals(String.format("side:%06d", row++), columns.getsRow());

      for (ColumnValue columnValue : columns) {
        Assert.assertEquals(new Column("debug", "sum"), columnValue.getColumn());
        Assert
            .assertEquals("row : " + columns.getsRow(), "" + expectedVal, columnValue.getsValue());
      }
    }

    Assert.assertEquals(expectedRows, row);
  }

  private void updateMany(FluoClient fc) {
    try (Transaction tx = fc.newTransaction()) {
      Map<String, Long> updates = new HashMap<>();
      for (int i = 0; i < 1000; i++) {
        updates.put(String.format("%06d", i), 1L);
      }

      wcMap.update(tx, updates);
      tx.commit();
    }
  }
}

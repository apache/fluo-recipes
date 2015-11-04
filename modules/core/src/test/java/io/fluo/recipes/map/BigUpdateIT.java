/*
 * Copyright 2015 Fluo authors (see AUTHORS)
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

package io.fluo.recipes.map;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Optional;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Transaction;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.mini.MiniFluo;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.api.types.TypedSnapshot;
import io.fluo.api.types.TypedTransactionBase;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This test configures a small buffer size and verifies that multiple passes are made to process
 * updates.
 */
public class BigUpdateIT {
  private static final TypeLayer tl = new TypeLayer(new StringEncoder());

  private MiniFluo miniFluo;

  private CollisionFreeMap<String, Long, Long> wcMap;

  static final String MAP_ID = "bu";

  public static class LongCombiner implements Combiner<String, Long, Long> {

    @Override
    public Optional<Long> combine(String key, Optional<Long> currentValue, Iterator<Long> updates) {
      long[] count = new long[] {currentValue.or(0L)};
      updates.forEachRemaining(l -> count[0] += l);
      return Optional.of(count[0]);
    }
  }

  public static class MyObserver extends UpdateObserver<String, Long> {
    @Override
    public void updatingValues(TransactionBase tx, Iterator<Update<String, Long>> updates) {
      TypedTransactionBase ttx = tl.wrap(tx);

      while (updates.hasNext()) {
        Update<String, Long> update = updates.next();
        ttx.mutate().row("side:" + update.getKey()).fam("debug").qual("sum")
            .set(update.getNewValue().get());
      }

      ttx.mutate().row("sideE01").fam("debug").qual("updates").increment(1);
    }
  }

  @Before
  public void setUpFluo() throws Exception {
    FileUtils.deleteQuietly(new File("target/mini"));

    FluoConfiguration props = new FluoConfiguration();
    props.setApplicationName("eqt");
    props.setWorkerThreads(20);
    props.setMiniDataDir("target/mini");

    CollisionFreeMap.configure(props, new CollisionFreeMap.Options(MAP_ID, LongCombiner.class,
        MyObserver.class, String.class, Long.class, Long.class, 2).setBufferSize(1 << 10));

    miniFluo = FluoFactory.newMiniFluo(props);

    wcMap = CollisionFreeMap.getInstance(MAP_ID, props.getAppConfiguration());
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
        numUpdates = snap.get().row("sideE01").fam("debug").qual("updates").toInteger(0);
        // there are two buckets, expect update processing at least twice per bucket
        Assert.assertTrue(numUpdates >= 4);
      }

      updateMany(fc);
      updateMany(fc);

      miniFluo.waitForObservers();

      try (TypedSnapshot snap = tl.wrap(fc.newSnapshot())) {
        checkUpdates(snap, 3, 1000);
        numUpdates =
            snap.get().row("sideE01").fam("debug").qual("updates").toInteger(0) - numUpdates;
        Assert.assertTrue(numUpdates >= 4);
      }

      for (int i = 0; i < 10; i++) {
        updateMany(fc);
      }

      miniFluo.waitForObservers();

      try (TypedSnapshot snap = tl.wrap(fc.newSnapshot())) {
        checkUpdates(snap, 13, 1000);
        numUpdates =
            snap.get().row("sideE01").fam("debug").qual("updates").toInteger(0) - numUpdates;
        Assert.assertTrue(numUpdates >= 4);
      }
    }
  }

  private void checkUpdates(TypedSnapshot snap, long expectedVal, long expectedRows) {
    RowIterator iter = snap.get(new ScannerConfiguration().setSpan(Span.prefix("side:")));

    int row = 0;

    while (iter.hasNext()) {
      Entry<Bytes, ColumnIterator> entry = iter.next();

      Assert.assertEquals(String.format("side:%06d", row++), entry.getKey().toString());

      ColumnIterator colITer = entry.getValue();
      while (colITer.hasNext()) {
        Entry<Column, Bytes> entry2 = colITer.next();
        Assert.assertEquals(new Column("debug", "sum"), entry2.getKey());
        Assert.assertEquals("" + expectedVal, entry2.getValue().toString());
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

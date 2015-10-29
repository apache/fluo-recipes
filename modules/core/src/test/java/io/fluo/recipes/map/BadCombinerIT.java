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

package io.fluo.recipes.map;

import java.io.File;
import java.util.Iterator;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Transaction;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.config.FluoConfiguration;
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
 * Wrote this test for a bug in the collision free map, where the map was not resilient to the
 * combiner mutating the current value object.
 */
public class BadCombinerIT {

  private static final TypeLayer tl = new TypeLayer(new StringEncoder());

  private MiniFluo miniFluo;

  private CollisionFreeMap<String, MutableLong, MutableLong> wcMap;

  static final String MAP_ID = "bom";

  public static class MutableLong {
    public long l;

    public MutableLong() {}

    public MutableLong(long l) {
      this.l = l;
    }
  }

  public static class BadCombiner implements Combiner<String, MutableLong, MutableLong> {

    @Override
    public Optional<MutableLong> combine(String key, Optional<MutableLong> currentValue,
        Iterator<MutableLong> updates) {
      MutableLong cv = currentValue.or(new MutableLong(0));

      while (updates.hasNext()) {
        MutableLong u = updates.next();

        // this modifies the object passed in... want to make sure that CollisionFreeMap is
        // resilient to this
        cv.l += u.l;
      }

      return Optional.of(cv);
    }
  }

  public static class MyObserver extends UpdateObserver<String, MutableLong> {
    @Override
    public void updatingValues(TransactionBase tx, Iterator<Update<String, MutableLong>> updates) {

      TypedTransactionBase ttx = tl.wrap(tx);

      while (updates.hasNext()) {
        Update<String, MutableLong> update = updates.next();
        // if the modfication made by the bad combiner is passed here it should cause a delta of
        // zero, which would cause the test to break.
        long delta =
            update.getNewValue().or(new MutableLong(0)).l
                - update.getOldValue().or(new MutableLong(0)).l;
        ttx.mutate().row(update.getKey()).fam("f").qual("q").set(delta);
      }
    }
  }

  @Before
  public void setUpFluo() throws Exception {
    FileUtils.deleteQuietly(new File("target/mini"));

    FluoConfiguration props = new FluoConfiguration();
    props.setApplicationName("eqt");
    props.setWorkerThreads(20);
    props.setMiniDataDir("target/mini");

    CollisionFreeMap.configure(props, new CollisionFreeMap.Options(MAP_ID, BadCombiner.class,
        MyObserver.class, String.class, MutableLong.class, MutableLong.class, 17));

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
  public void testGet() {
    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {
      try (Transaction tx = fc.newTransaction()) {
        wcMap.update(tx, ImmutableMap.of("cat", new MutableLong(4)));
        tx.commit();
      }

      miniFluo.waitForObservers();

      try (TypedSnapshot snap = tl.wrap(fc.newSnapshot())) {
        Assert.assertEquals("4", snap.get().row("cat").fam("f").qual("q").toString());
      }

      try (Transaction tx = fc.newTransaction()) {
        wcMap.update(tx, ImmutableMap.of("cat", new MutableLong(7)));
        tx.commit();
      }

      miniFluo.waitForObservers();

      try (TypedSnapshot snap = tl.wrap(fc.newSnapshot())) {
        Assert.assertEquals("7", snap.get().row("cat").fam("f").qual("q").toString());
      }
    }
  }
}

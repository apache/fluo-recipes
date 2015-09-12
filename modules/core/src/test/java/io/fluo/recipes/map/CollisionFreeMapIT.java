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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import com.google.common.collect.ImmutableMap;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.client.Snapshot;
import io.fluo.api.client.Transaction;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.mini.MiniFluo;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CollisionFreeMapIT {

  private MiniFluo miniFluo;

  private CollisionFreeMap<String, Long, Long> wcMap;

  static final String MAP_ID = "wcm";

  @Before
  public void setUpFluo() throws Exception {
    FileUtils.deleteQuietly(new File("target/mini"));

    FluoConfiguration props = new FluoConfiguration();
    props.setApplicationName("eqt");
    props.setWorkerThreads(20);
    props.setMiniDataDir("target/mini");

    props.addObserver(new ObserverConfiguration(DocumentObserver.class.getName()));

    CollisionFreeMap.configure(props, new CollisionFreeMap.Options(MAP_ID, WordCountCombiner.class,
        WordCountObserver.class, String.class, Long.class, Long.class, 17)
        .setSerializer(TestSerializer.class));

    miniFluo = FluoFactory.newMiniFluo(props);

    wcMap = CollisionFreeMap.getInstance(MAP_ID, props.getAppConfiguration());
  }

  @After
  public void tearDownFluo() throws Exception {
    if (miniFluo != null) {
      miniFluo.close();
    }
  }

  private Map<String, Long> getComputedWordCounts(FluoClient fc) {
    Map<String, Long> counts = new HashMap<>();

    try (Snapshot snap = fc.newSnapshot()) {
      RowIterator scanner = snap.get(new ScannerConfiguration().setSpan(Span.prefix("iwc:")));
      while (scanner.hasNext()) {
        Entry<Bytes, ColumnIterator> row = scanner.next();

        String[] tokens = row.getKey().toString().split(":");
        String word = tokens[2];
        Long count = Long.valueOf(tokens[1]);

        Assert.assertFalse("Word seen twice in index " + word, counts.containsKey(word));

        if (count != 0) {
          counts.put(word, count);
        }
      }
    }

    return counts;
  }

  private Map<String, Long> computeWordCounts(FluoClient fc) {
    Map<String, Long> counts = new HashMap<>();

    try (Snapshot snap = fc.newSnapshot()) {
      RowIterator scanner =
          snap.get(new ScannerConfiguration().setSpan(Span.prefix("d:")).fetchColumn(
              Bytes.of("content"), Bytes.of("current")));
      while (scanner.hasNext()) {
        Entry<Bytes, ColumnIterator> row = scanner.next();

        ColumnIterator colIter = row.getValue();

        while (colIter.hasNext()) {
          Entry<Column, Bytes> entry = colIter.next();

          String[] words = entry.getValue().toString().split("\\s+");
          for (String word : words) {
            if (word.isEmpty()) {
              continue;
            }

            counts.merge(word, 1L, Long::sum);
          }
        }
      }
    }

    return counts;
  }

  @Test
  public void testGet() {
    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {
      try (Transaction tx = fc.newTransaction()) {
        wcMap.update(tx, ImmutableMap.of("cat", 2L, "dog", 5L));
        tx.commit();
      }

      try (Transaction tx = fc.newTransaction()) {
        wcMap.update(tx, ImmutableMap.of("cat", 1L, "dog", 1L));
        tx.commit();
      }

      try (Transaction tx = fc.newTransaction()) {
        wcMap.update(tx, ImmutableMap.of("cat", 1L, "dog", 1L, "fish", 2L));
        tx.commit();
      }

      // try reading possibly before observer combines... will either see outstanding updates or a
      // current value
      try (Snapshot snap = fc.newSnapshot()) {
        Assert.assertEquals((Long) 4L, wcMap.get(snap, "cat"));
        Assert.assertEquals((Long) 7L, wcMap.get(snap, "dog"));
        Assert.assertEquals((Long) 2L, wcMap.get(snap, "fish"));
      }

      miniFluo.waitForObservers();

      // in this case there should be no updates, only a current value
      try (Snapshot snap = fc.newSnapshot()) {
        Assert.assertEquals((Long) 4L, wcMap.get(snap, "cat"));
        Assert.assertEquals((Long) 7L, wcMap.get(snap, "dog"));
        Assert.assertEquals((Long) 2L, wcMap.get(snap, "fish"));
      }

      Map<String, Long> expectedCounts = new HashMap<>();
      expectedCounts.put("cat", 4L);
      expectedCounts.put("dog", 7L);
      expectedCounts.put("fish", 2L);

      Assert.assertEquals(expectedCounts, getComputedWordCounts(fc));

      try (Transaction tx = fc.newTransaction()) {
        wcMap.update(tx, ImmutableMap.of("cat", 1L, "dog", 1L));
        tx.commit();
      }

      // there may be outstanding update and a current value for the key in this case
      try (Snapshot snap = fc.newSnapshot()) {
        Assert.assertEquals((Long) 5L, wcMap.get(snap, "cat"));
        Assert.assertEquals((Long) 8L, wcMap.get(snap, "dog"));
        Assert.assertEquals((Long) 2L, wcMap.get(snap, "fish"));
      }

      miniFluo.waitForObservers();

      try (Snapshot snap = fc.newSnapshot()) {
        Assert.assertEquals((Long) 5L, wcMap.get(snap, "cat"));
        Assert.assertEquals((Long) 8L, wcMap.get(snap, "dog"));
        Assert.assertEquals((Long) 2L, wcMap.get(snap, "fish"));
      }

      expectedCounts.put("cat", 5L);
      expectedCounts.put("dog", 8L);

      Assert.assertEquals(expectedCounts, getComputedWordCounts(fc));
    }
  }

  @Test
  public void testBasic() {
    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {
      try (LoaderExecutor loader = fc.newLoaderExecutor()) {
        loader.execute(new DocumentLoader("0001", "dog cat"));
        loader.execute(new DocumentLoader("0002", "cat hamster"));
        loader.execute(new DocumentLoader("0003", "milk bread cat food"));
        loader.execute(new DocumentLoader("0004", "zoo big cat"));
      }

      miniFluo.waitForObservers();

      try (Snapshot snap = fc.newSnapshot()) {
        Assert.assertEquals((Long) 4L, wcMap.get(snap, "cat"));
        Assert.assertEquals((Long) 1L, wcMap.get(snap, "milk"));
      }

      Map<String, Long> expectedCounts = new HashMap<>();
      expectedCounts.put("dog", 1L);
      expectedCounts.put("cat", 4L);
      expectedCounts.put("hamster", 1L);
      expectedCounts.put("milk", 1L);
      expectedCounts.put("bread", 1L);
      expectedCounts.put("food", 1L);
      expectedCounts.put("zoo", 1L);
      expectedCounts.put("big", 1L);

      Assert.assertEquals(expectedCounts, getComputedWordCounts(fc));

      try (LoaderExecutor loader = fc.newLoaderExecutor()) {
        loader.execute(new DocumentLoader("0001", "dog feline"));
      }

      miniFluo.waitForObservers();

      expectedCounts.put("cat", 3L);
      expectedCounts.put("feline", 1L);

      Assert.assertEquals(expectedCounts, getComputedWordCounts(fc));

      try (LoaderExecutor loader = fc.newLoaderExecutor()) {
        // swap contents of two documents... should not change doc counts
        loader.execute(new DocumentLoader("0003", "zoo big cat"));
        loader.execute(new DocumentLoader("0004", "milk bread cat food"));
      }

      miniFluo.waitForObservers();
      Assert.assertEquals(expectedCounts, getComputedWordCounts(fc));

      try (LoaderExecutor loader = fc.newLoaderExecutor()) {
        loader.execute(new DocumentLoader("0003", "zoo big cat"));
        loader.execute(new DocumentLoader("0004", "zoo big cat"));
      }

      miniFluo.waitForObservers();

      expectedCounts.put("zoo", 2L);
      expectedCounts.put("big", 2L);
      expectedCounts.remove("milk");
      expectedCounts.remove("bread");
      expectedCounts.remove("food");

      Assert.assertEquals(expectedCounts, getComputedWordCounts(fc));

      try (LoaderExecutor loader = fc.newLoaderExecutor()) {
        loader.execute(new DocumentLoader("0002", "cat cat hamster hamster"));
      }

      miniFluo.waitForObservers();

      expectedCounts.put("cat", 4L);
      expectedCounts.put("hamster", 2L);

      Assert.assertEquals(expectedCounts, getComputedWordCounts(fc));

      try (LoaderExecutor loader = fc.newLoaderExecutor()) {
        loader.execute(new DocumentLoader("0002", "dog hamster"));
      }

      miniFluo.waitForObservers();

      expectedCounts.put("cat", 2L);
      expectedCounts.put("hamster", 1L);
      expectedCounts.put("dog", 2L);

      Assert.assertEquals(expectedCounts, getComputedWordCounts(fc));
    }
  }

  private static String randDocId(Random rand) {
    return String.format("%04d", rand.nextInt(5000));
  }

  private static String randomDocument(Random rand) {
    StringBuilder sb = new StringBuilder();

    String sep = "";
    for (int i = 2; i < rand.nextInt(18); i++) {
      sb.append(sep);
      sep = " ";
      sb.append(String.format("%05d", rand.nextInt(50000)));
    }

    return sb.toString();
  }

  public void diff(Map<String, Long> m1, Map<String, Long> m2) {
    for (String word : m1.keySet()) {
      Long v1 = m1.get(word);
      Long v2 = m2.get(word);

      if (v2 == null || !v1.equals(v2)) {
        System.out.println(word + " " + v1 + " != " + v2);
      }
    }

    for (String word : m2.keySet()) {
      Long v1 = m1.get(word);
      Long v2 = m2.get(word);

      if (v1 == null) {
        System.out.println(word + " " + v1 + " != " + v2);
      }
    }
  }

  @Test
  public void testStress() {
    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {
      Random rand = new Random();

      try (LoaderExecutor loader = fc.newLoaderExecutor()) {
        for (int i = 0; i < 1000; i++) {
          loader.execute(new DocumentLoader(randDocId(rand), randomDocument(rand)));
        }
      }

      miniFluo.waitForObservers();
      assertWordCountsEqual(fc);

      try (LoaderExecutor loader = fc.newLoaderExecutor()) {
        for (int i = 0; i < 100; i++) {
          loader.execute(new DocumentLoader(randDocId(rand), randomDocument(rand)));
        }
      }

      miniFluo.waitForObservers();
      assertWordCountsEqual(fc);
    }
  }

  private void assertWordCountsEqual(FluoClient fc) {
    Map<String, Long> expected = computeWordCounts(fc);
    Map<String, Long> actual = getComputedWordCounts(fc);
    if (!expected.equals(actual)) {
      diff(expected, actual);
      Assert.fail();
    }
  }
}

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

package io.fluo.recipes.export;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Iterators;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.client.Snapshot;
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

public class ExportQueueIT {

  private static Map<String, Map<String, RefInfo>> globalExports = new HashMap<>();

  private static Set<String> getExportedReferees(String node) {
    Set<String> ret = new HashSet<>();

    Map<String, RefInfo> referees = globalExports.get(node);

    if (referees == null) {
      return ret;
    }

    referees.forEach((k, v) -> {
      if (!v.deleted)
        ret.add(k);
    });

    return ret;
  }

  private static Map<String, Set<String>> getExportedReferees() {
    Map<String, Set<String>> ret = new HashMap<>();

    for (String k : globalExports.keySet()) {
      Set<String> referees = getExportedReferees(k);
      if (referees.size() > 0) {
        ret.put(k, referees);
      }
    }

    return ret;
  }

  public static class RefExporter extends Exporter<String, RefUpdates> {

    public static final String QUEUE_ID = "req";

    private void updateExports(String key, long seq, String addedRef, boolean deleted) {
      Map<String, RefInfo> referees = globalExports.computeIfAbsent(addedRef, k -> new HashMap<>());
      referees.compute(key, (k, v) -> (v == null || v.seq < seq) ? new RefInfo(seq, deleted) : v);
    }

    @Override
    protected void processExports(Iterator<SequencedExport<String, RefUpdates>> exportIterator) {
      ArrayList<SequencedExport<String, RefUpdates>> exportList = new ArrayList<>();
      Iterators.addAll(exportList, exportIterator);

      synchronized (ExportQueueIT.globalExports) {
        for (SequencedExport<String, RefUpdates> se : exportList) {
          for (String addedRef : se.getValue().getAddedRefs()) {
            updateExports(se.getKey(), se.getSequence(), addedRef, false);
          }

          for (String deletedRef : se.getValue().getDeletedRefs()) {
            updateExports(se.getKey(), se.getSequence(), deletedRef, true);
          }
        }
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

    ObserverConfiguration doc = new ObserverConfiguration(DocumentObserver.class.getName());
    props.addObserver(doc);

    ExportQueue.Options exportQueueOpts =
        new ExportQueue.Options(RefExporter.QUEUE_ID, RefExporter.class, String.class,
            RefUpdates.class, 13);
    ExportQueue.configure(props, exportQueueOpts);

    miniFluo = FluoFactory.newMiniFluo(props);
  }

  @After
  public void tearDownFluo() throws Exception {
    if (miniFluo != null) {
      miniFluo.close();
    }
  }

  private static Set<String> ns(String... sa) {
    return new HashSet<>(Arrays.asList(sa));
  }

  @Test
  public void testExport() {
    globalExports.clear();

    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {
      try (LoaderExecutor loader = fc.newLoaderExecutor()) {
        loader.execute(new DocumentLoader("0999", "0005", "0002"));
        loader.execute(new DocumentLoader("0002", "0999", "0042"));
        loader.execute(new DocumentLoader("0005", "0999", "0042"));
        loader.execute(new DocumentLoader("0042", "0999"));
      }

      miniFluo.waitForObservers();

      Assert.assertEquals(ns("0002", "0005", "0042"), getExportedReferees("0999"));
      Assert.assertEquals(ns("0999"), getExportedReferees("0002"));
      Assert.assertEquals(ns("0999"), getExportedReferees("0005"));
      Assert.assertEquals(ns("0002", "0005"), getExportedReferees("0042"));

      try (LoaderExecutor loader = fc.newLoaderExecutor()) {
        loader.execute(new DocumentLoader("0999", "0005", "0042"));
      }

      try (LoaderExecutor loader = fc.newLoaderExecutor()) {
        loader.execute(new DocumentLoader("0999", "0005"));
      }

      miniFluo.waitForObservers();

      Assert.assertEquals(ns("0002", "0005", "0042"), getExportedReferees("0999"));
      Assert.assertEquals(ns(), getExportedReferees("0002"));
      Assert.assertEquals(ns("0999"), getExportedReferees("0005"));
      Assert.assertEquals(ns("0002", "0005"), getExportedReferees("0042"));

      try (LoaderExecutor loader = fc.newLoaderExecutor()) {
        loader.execute(new DocumentLoader("0042", "0999", "0002", "0005"));
        loader.execute(new DocumentLoader("0005", "0002"));
      }

      try (LoaderExecutor loader = fc.newLoaderExecutor()) {
        loader.execute(new DocumentLoader("0005", "0003"));
      }

      miniFluo.waitForObservers();

      Assert.assertEquals(ns("0002", "0042"), getExportedReferees("0999"));
      Assert.assertEquals(ns("0042"), getExportedReferees("0002"));
      Assert.assertEquals(ns("0005"), getExportedReferees("0003"));
      Assert.assertEquals(ns("0999", "0042"), getExportedReferees("0005"));
      Assert.assertEquals(ns("0002"), getExportedReferees("0042"));

    }
  }

  public void assertEquals(Map<String, Set<String>> expected, Map<String, Set<String>> actual,
      FluoClient fc) {
    if (!expected.equals(actual)) {
      System.out.println("*** diff ***");
      diff(expected, actual);
      System.out.println("*** fluo dump ***");
      dump(fc);
      System.out.println("*** map dump ***");

      Assert.fail();
    }
  }

  @Test
  public void exportStressTest() {
    FluoConfiguration config = new FluoConfiguration(miniFluo.getClientConfiguration());
    config.setLoaderQueueSize(100);
    config.setLoaderThreads(20);

    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {

      loadRandom(fc, 1000, 500);

      miniFluo.waitForObservers();

      diff(getFluoReferees(fc), getExportedReferees());

      assertEquals(getFluoReferees(fc), getExportedReferees(), fc);

      loadRandom(fc, 1000, 500);

      miniFluo.waitForObservers();

      assertEquals(getFluoReferees(fc), getExportedReferees(), fc);

      loadRandom(fc, 1000, 10000);

      miniFluo.waitForObservers();

      assertEquals(getFluoReferees(fc), getExportedReferees(), fc);

      loadRandom(fc, 1000, 10000);

      miniFluo.waitForObservers();

      assertEquals(getFluoReferees(fc), getExportedReferees(), fc);
    }
  }

  private void loadRandom(FluoClient fc, int num, int maxDocId) {
    try (LoaderExecutor loader = fc.newLoaderExecutor()) {
      Random rand = new Random();

      for (int i = 0; i < num; i++) {
        String docid = String.format("%05d", rand.nextInt(maxDocId));
        String[] refs = new String[rand.nextInt(20) + 1];
        for (int j = 0; j < refs.length; j++) {
          refs[j] = String.format("%05d", rand.nextInt(maxDocId));
        }

        loader.execute(new DocumentLoader(docid, refs));
      }
    }
  }

  private void diff(Map<String, Set<String>> fr, Map<String, Set<String>> er) {
    HashSet<String> allKeys = new HashSet<>(fr.keySet());
    allKeys.addAll(er.keySet());

    for (String k : allKeys) {
      Set<String> s1 = fr.getOrDefault(k, Collections.emptySet());
      Set<String> s2 = er.getOrDefault(k, Collections.emptySet());

      HashSet<String> sub1 = new HashSet<>(s1);
      sub1.removeAll(s2);

      HashSet<String> sub2 = new HashSet<>(s2);
      sub2.removeAll(s1);

      if (sub1.size() > 0 || sub2.size() > 0) {
        System.out.println(k + " " + sub1 + " " + sub2);
      }

    }
  }

  private Map<String, Set<String>> getFluoReferees(FluoClient fc) {
    Map<String, Set<String>> fluoReferees = new HashMap<>();

    try (Snapshot snap = fc.newSnapshot()) {
      ScannerConfiguration scannerConfig = new ScannerConfiguration();
      scannerConfig.fetchColumn(Bytes.of("content"), Bytes.of("current"));
      scannerConfig.setSpan(Span.prefix("d:"));
      RowIterator scanner = snap.get(scannerConfig);
      while (scanner.hasNext()) {
        Entry<Bytes, ColumnIterator> row = scanner.next();
        ColumnIterator colIter = row.getValue();

        String docid = row.getKey().toString().substring(2);

        while (colIter.hasNext()) {
          Entry<Column, Bytes> entry = colIter.next();

          String[] refs = entry.getValue().toString().split(" ");

          for (String ref : refs) {
            if (ref.isEmpty())
              continue;

            fluoReferees.computeIfAbsent(ref, k -> new HashSet<>()).add(docid);
          }
        }
      }
    }
    return fluoReferees;
  }

  public static void dump(FluoClient fc) {
    try (Snapshot snap = fc.newSnapshot()) {
      RowIterator scanner = snap.get(new ScannerConfiguration());
      while (scanner.hasNext()) {
        Entry<Bytes, ColumnIterator> row = scanner.next();
        ColumnIterator colIter = row.getValue();

        while (colIter.hasNext()) {
          Entry<Column, Bytes> entry = colIter.next();

          System.out.println("row:[" + row.getKey() + "]  col:[" + entry.getKey() + "]  val:["
              + entry.getValue() + "]");
        }
      }
    }
  }
}

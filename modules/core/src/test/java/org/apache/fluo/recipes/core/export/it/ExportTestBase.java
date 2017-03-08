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

package org.apache.fluo.recipes.core.export.it;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Iterators;
import org.apache.commons.io.FileUtils;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.apache.fluo.recipes.core.export.SequencedExport;
import org.apache.fluo.recipes.core.export.function.Exporter;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import static org.apache.fluo.api.observer.Observer.NotificationType.STRONG;

public class ExportTestBase {

  private static Map<String, Map<String, RefInfo>> globalExports = new HashMap<>();
  private static int exportCalls = 0;

  protected static Set<String> getExportedReferees(String node) {
    synchronized (globalExports) {
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
  }

  protected static Map<String, Set<String>> getExportedReferees() {
    synchronized (globalExports) {

      Map<String, Set<String>> ret = new HashMap<>();

      for (String k : globalExports.keySet()) {
        Set<String> referees = getExportedReferees(k);
        if (referees.size() > 0) {
          ret.put(k, referees);
        }
      }

      return ret;
    }
  }

  protected static int getNumExportCalls() {
    synchronized (globalExports) {
      return exportCalls;
    }
  }

  public static class RefExporter implements Exporter<String, RefUpdates> {

    public static final String QUEUE_ID = "req";

    private void updateExports(String key, long seq, String addedRef, boolean deleted) {
      Map<String, RefInfo> referees = globalExports.computeIfAbsent(addedRef, k -> new HashMap<>());
      referees.compute(key, (k, v) -> (v == null || v.seq < seq) ? new RefInfo(seq, deleted) : v);
    }

    @Override
    public void export(Iterator<SequencedExport<String, RefUpdates>> exportIterator) {
      ArrayList<SequencedExport<String, RefUpdates>> exportList = new ArrayList<>();
      Iterators.addAll(exportList, exportIterator);

      synchronized (globalExports) {
        exportCalls++;

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

  protected MiniFluo miniFluo;

  protected int getNumBuckets() {
    return 13;
  }

  protected Integer getBufferSize() {
    return null;
  }

  public static class ExportTestObserverProvider implements ObserverProvider {

    @Override
    public void provide(Registry or, Context ctx) {
      ExportQueue<String, RefUpdates> refExportQueue =
          ExportQueue.getInstance(RefExporter.QUEUE_ID, ctx.getAppConfiguration());

      or.register(new Column("content", "new"), STRONG, new DocumentObserver(refExportQueue));
      refExportQueue.registerObserver(or, new RefExporter());
    }
  }

  @Before
  public void setUpFluo() throws Exception {
    FileUtils.deleteQuietly(new File("target/mini"));

    FluoConfiguration props = new FluoConfiguration();
    props.setApplicationName("eqt");
    props.setWorkerThreads(20);
    props.setMiniDataDir("target/mini");
    props.setObserverProvider(ExportTestObserverProvider.class);

    SimpleSerializer.setSerializer(props, GsonSerializer.class);

    if (getBufferSize() == null) {
      ExportQueue.configure(RefExporter.QUEUE_ID).keyType(String.class).valueType(RefUpdates.class)
          .buckets(getNumBuckets()).save(props);
    } else {
      ExportQueue.configure(RefExporter.QUEUE_ID).keyType(String.class).valueType(RefUpdates.class)
          .buckets(getNumBuckets()).bufferSize(getBufferSize()).save(props);
    }

    miniFluo = FluoFactory.newMiniFluo(props);

    globalExports.clear();
    exportCalls = 0;
  }

  @After
  public void tearDownFluo() throws Exception {
    if (miniFluo != null) {
      miniFluo.close();
    }
  }

  protected static Set<String> ns(String... sa) {
    return new HashSet<>(Arrays.asList(sa));
  }

  protected static String nk(int i) {
    return String.format("%06d", i);
  }

  protected static Set<String> ns(int... ia) {
    HashSet<String> ret = new HashSet<>();
    for (int i : ia) {
      ret.add(nk(i));
    }
    return ret;
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

  protected void loadRandom(FluoClient fc, int num, int maxDocId) {
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

  protected void diff(Map<String, Set<String>> fr, Map<String, Set<String>> er) {
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

  protected Map<String, Set<String>> getFluoReferees(FluoClient fc) {
    Map<String, Set<String>> fluoReferees = new HashMap<>();

    try (Snapshot snap = fc.newSnapshot()) {

      Column currCol = new Column("content", "current");
      RowScanner rowScanner = snap.scanner().over(Span.prefix("d:")).fetch(currCol).byRow().build();

      for (ColumnScanner columnScanner : rowScanner) {
        String docid = columnScanner.getsRow().substring(2);

        for (ColumnValue columnValue : columnScanner) {
          String[] refs = columnValue.getsValue().split(" ");

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
      CellScanner scanner = snap.scanner().build();
      scanner.forEach(rcv -> System.out.println("row:[" + rcv.getRow() + "]  col:["
          + rcv.getColumn() + "]  val:[" + rcv.getValue() + "]"));
    }
  }
}

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.junit.Assert;
import org.junit.Test;

public class ExportBufferIT extends ExportTestBase {

  @Override
  protected int getNumBuckets() {
    return 2;
  }

  @Override
  protected Integer getBufferSize() {
    return 1024;
  }

  @Test
  public void testSmallExportBuffer() {
    // try setting the export buffer size small. Make sure everything is exported.

    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {
      ExportQueue<String, RefUpdates> refExportQueue =
          ExportQueue.getInstance(RefExporter.QUEUE_ID, fc.getAppConfiguration());
      try (Transaction tx = fc.newTransaction()) {
        for (int i = 0; i < 1000; i++) {
          refExportQueue.add(tx, nk(i), new RefUpdates(ns(i + 10, i + 20), ns(new int[0])));
        }

        tx.commit();
      }
    }

    miniFluo.waitForObservers();

    Map<String, Set<String>> erefs = getExportedReferees();
    Map<String, Set<String>> expected = new HashMap<>();

    for (int i = 0; i < 1000; i++) {
      expected.computeIfAbsent(nk(i + 10), s -> new HashSet<>()).add(nk(i));
      expected.computeIfAbsent(nk(i + 20), s -> new HashSet<>()).add(nk(i));
    }

    assertEquals(expected, erefs);
    int prevNumExportCalls = getNumExportCalls();
    Assert.assertTrue(prevNumExportCalls > 10); // with small buffer there should be lots of exports
                                                // calls

    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {
      ExportQueue<String, RefUpdates> refExportQueue =
          ExportQueue.getInstance(RefExporter.QUEUE_ID, fc.getAppConfiguration());
      try (Transaction tx = fc.newTransaction()) {
        for (int i = 0; i < 1000; i++) {
          refExportQueue.add(tx, nk(i), new RefUpdates(ns(i + 12), ns(i + 10)));
        }

        tx.commit();
      }
    }

    miniFluo.waitForObservers();

    erefs = getExportedReferees();
    expected = new HashMap<>();

    for (int i = 0; i < 1000; i++) {
      expected.computeIfAbsent(nk(i + 12), s -> new HashSet<>()).add(nk(i));
      expected.computeIfAbsent(nk(i + 20), s -> new HashSet<>()).add(nk(i));
    }

    assertEquals(expected, erefs);
    prevNumExportCalls = getNumExportCalls() - prevNumExportCalls;
    Assert.assertTrue(prevNumExportCalls > 10);
  }

  public void assertEquals(Map<String, Set<String>> expected, Map<String, Set<String>> actual) {
    if (!expected.equals(actual)) {
      System.out.println("*** diff ***");
      diff(expected, actual);
      Assert.fail();
    }
  }
}

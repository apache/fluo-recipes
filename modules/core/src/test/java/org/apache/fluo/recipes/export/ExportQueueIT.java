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

package org.apache.fluo.recipes.export;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.config.FluoConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class ExportQueueIT extends ExportTestBase {

  @Test
  public void testExport() {
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
      Assert.assertEquals(ns(new String[0]), getExportedReferees("0002"));
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
}

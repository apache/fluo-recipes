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

package io.fluo.recipes.accumulo.export;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.fluo.api.client.FluoAdmin.InitOpts;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Transaction;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.mini.MiniFluo;
import io.fluo.recipes.accumulo.export.AccumuloExporter;
import io.fluo.recipes.accumulo.export.TableInfo;
import io.fluo.recipes.export.ExportQueue;
import io.fluo.recipes.export.ExportQueueOptions;

public class AccumuloExporterIT {
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  private static FluoConfiguration props;
  private static MiniFluo miniFluo;
  private static final PasswordToken password = new PasswordToken("secret");
  private static AtomicInteger tableCounter = new AtomicInteger(1);
  private String et;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg =
        new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), new String(password.getPassword()));
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }

  @Before
  public void setUpFluo() throws Exception {
    props = new FluoConfiguration();
    props.setMiniStartAccumulo(false);
    props.setApplicationName("aeq");
    props.setAccumuloInstance(cluster.getInstanceName());
    props.setAccumuloUser("root");
    props.setAccumuloPassword("secret");
    props.setInstanceZookeepers(cluster.getZooKeepers() + "/fluo");
    props.setAccumuloZookeepers(cluster.getZooKeepers());
    props.setAccumuloTable("data" + tableCounter.getAndIncrement());
    props.setWorkerThreads(5);
    props.setObservers(Arrays.asList(new ObserverConfiguration(TestExporter.class.getName())));

    new TestExporter().setConfiguration(props.getAppConfiguration(), new ExportQueueOptions(5, 5));

    // create and configure export table
    et = "export" + tableCounter.getAndIncrement();
    cluster.getConnector("root", "secret").tableOperations().create(et);
    AccumuloExporter.setExportTableInfo(props.getAppConfiguration(), TestExporter.QUEUE_ID,
        new TableInfo(cluster.getInstanceName(), cluster.getZooKeepers(), "root", "secret", et));

    FluoFactory.newAdmin(props).initialize(
        new InitOpts().setClearTable(true).setClearZookeeper(true));

    miniFluo = FluoFactory.newMiniFluo(props);
  }

  @Test
  public void testAccumuloExport() throws Exception {

    ExportQueue<String, String> teq =
        new TestExporter().getExportQueue(props.getAppConfiguration());

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

  private Map<String, String> getExports() throws Exception {
    Scanner scanner =
        cluster.getConnector("root", "secret").createScanner(et, Authorizations.EMPTY);
    Map<String, String> ret = new HashMap<>();

    for (Entry<Key, Value> entry : scanner) {
      String k = entry.getKey().getRowData().toString();
      Assert.assertFalse(ret.containsKey(k));
      ret.put(k, entry.getValue().toString());
    }

    return ret;
  }

  @After
  public void tearDownFluo() throws Exception {
    if (miniFluo != null)
      miniFluo.close();
  }
}

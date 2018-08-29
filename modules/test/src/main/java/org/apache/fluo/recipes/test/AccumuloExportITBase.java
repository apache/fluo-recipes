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

package org.apache.fluo.recipes.test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.recipes.accumulo.ops.TableOperations;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * This class is intended to be extended by classes testing exporting from Fluo to Accumulo. Using
 * MiniFluo by itself is easy. However, using MiniAccumulo and MiniFluo together involves writing a
 * lot of boilerplate code. That's why this class exists: its a place to put that boilerplate code.
 *
 * <p>
 * Below is some example code showing how to use this class to write a test.
 *
 * <pre>
 * <code>
 *    class MyExportIT extends AccumuloExportITBase {
 * 
 *         private String exportTable;
 * 
 *         public MyExportIT(){
 *           //indicate that MiniFluo should be started before each test
 *           super(true);
 *         }
 * 
 *         {@literal @}Override
 *         //this is method is called by super class before initializing Fluo
 *         public void preFluoInitHook() throws Exception {
 * 
 *           //create table to export to
 *           Connector conn = getAccumuloConnector();
 *           exportTable = "export" + tableCounter.getAndIncrement();
 *           conn.tableOperations().create(exportTable);
 * 
 *           //This config will be used to initialize Fluo
 *           FluoConfiguration fluoConfig = getFluoConfiguration();
 * 
 *           MiniAccumuloCluster miniAccumulo = getMiniAccumuloCluster();
 *           String instance = miniAccumulo.getInstanceName();
 *           String zookeepers = miniAccumulo.getZooKeepers();
 *           String user = ACCUMULO_USER;
 *           String password = ACCUMULO_PASSWORD;
 * 
 *           //Configure observers on fluoConfig to export using info above
 *        }
 * 
 *        {@literal @}Test
 *        public void exportTest1(){
 *            try(FluoClient client = FluoFactory.newClient(getFluoConfiguration())) {
 *              //write some data that will cause an observer to export data
 *            }
 * 
 *            getMiniFluo().waitForObservers();
 * 
 *            //verify data was exported
 *        }
 *    }
 * </code>
 * </pre>
 *
 * @since 1.0.0
 */
public class AccumuloExportITBase {

  public static final String ACCUMULO_USER = "root";
  public static final String ACCUMULO_PASSWORD = "secret";

  private static File baseDir;
  private static MiniAccumuloCluster cluster;
  private FluoConfiguration fluoConfig;
  private MiniFluo miniFluo;
  protected static AtomicInteger tableCounter = new AtomicInteger(1);
  private boolean startMiniFluo;


  protected AccumuloExportITBase() {
    this(true);
  }

  /**
   * @param startMiniFluo passing true will cause MiniFluo to be started before each test. Passing
   *        false will cause Fluo to be initialized, but not started before each test.
   */
  protected AccumuloExportITBase(boolean startMiniFluo) {
    this.startMiniFluo = startMiniFluo;
  }

  @BeforeClass
  public static void setupMiniAccumulo() throws Exception {
    try {

      // try to put in target dir
      File targetDir = new File("target");
      if (targetDir.exists() && targetDir.isDirectory()) {
        baseDir = new File(targetDir, "accumuloExportIT-" + UUID.randomUUID());
      } else {
        baseDir = new File(FileUtils.getTempDirectory(), "accumuloExportIT-" + UUID.randomUUID());
      }

      FileUtils.deleteDirectory(baseDir);
      MiniAccumuloConfig cfg = new MiniAccumuloConfig(baseDir, ACCUMULO_PASSWORD);
      cluster = new MiniAccumuloCluster(cfg);
      cluster.start();
    } catch (IOException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  @AfterClass
  public static void tearDownMiniAccumulo() throws Exception {
    cluster.stop();
    FileUtils.deleteDirectory(baseDir);
  }

  @Before
  public void setupMiniFluo() throws Exception {
    resetFluoConfig();
    preFluoInitHook();
    FluoFactory.newAdmin(fluoConfig).initialize(
        new FluoAdmin.InitializationOptions().setClearTable(true).setClearZookeeper(true));
    postFluoInitHook();
    if (startMiniFluo) {
      miniFluo = FluoFactory.newMiniFluo(fluoConfig);
    } else {
      miniFluo = null;
    }
  }

  @After
  public void tearDownMiniFluo() throws Exception {
    if (miniFluo != null) {
      miniFluo.close();
      miniFluo = null;
    }
  }

  /**
   * This method is intended to be overridden. The method is called before each test before Fluo is
   * initialized.
   */
  protected void preFluoInitHook() throws Exception {

  }

  /**
   * This method is intended to be overridden. The method is called before each test after Fluo is
   * initialized before MiniFluo is started.
   */
  protected void postFluoInitHook() throws Exception {
    TableOperations.optimizeTable(fluoConfig);
  }

  /**
   * Retrieves MiniAccumuloCluster
   */
  protected MiniAccumuloCluster getMiniAccumuloCluster() {
    return cluster;
  }

  /**
   * Retrieves MiniFluo
   */
  protected synchronized MiniFluo getMiniFluo() {
    return miniFluo;
  }

  /**
   * Returns an Accumulo Connector to MiniAccumuloCluster
   */
  protected Connector getAccumuloConnector() {
    try {
      return cluster.getConnector(ACCUMULO_USER, ACCUMULO_PASSWORD);
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Retrieves Fluo Configuration
   */
  protected synchronized FluoConfiguration getFluoConfiguration() {
    return fluoConfig;
  }

  /**
   * A utility method that will set the configuration needed by Fluo from a given MiniCluster
   */
  public static void configureFromMAC(FluoConfiguration fluoConfig, MiniAccumuloCluster cluster) {
    fluoConfig.setMiniStartAccumulo(false);
    fluoConfig.setAccumuloInstance(cluster.getInstanceName());
    fluoConfig.setAccumuloUser("root");
    fluoConfig.setAccumuloPassword(cluster.getConfig().getRootPassword());
    fluoConfig.setInstanceZookeepers(cluster.getZooKeepers() + "/fluo");
    fluoConfig.setAccumuloZookeepers(cluster.getZooKeepers());
  }

  private void resetFluoConfig() {
    fluoConfig = new FluoConfiguration();
    configureFromMAC(fluoConfig, cluster);
    fluoConfig.setApplicationName("fluo-it");
    fluoConfig.setAccumuloTable("fluo" + tableCounter.getAndIncrement());
  }
}

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

import java.util.concurrent.atomic.AtomicInteger;

import io.fluo.api.client.FluoAdmin.InitOpts;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.mini.MiniFluo;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;

public abstract class AccumuloITBase {
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  static FluoConfiguration props;
  static MiniFluo miniFluo;
  static final PasswordToken password = new PasswordToken("secret");
  static AtomicInteger tableCounter = new AtomicInteger(1);

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

    setupExporter();

    FluoFactory.newAdmin(props).initialize(
        new InitOpts().setClearTable(true).setClearZookeeper(true));

    miniFluo = FluoFactory.newMiniFluo(props);
  }

  public abstract void setupExporter() throws Exception;

  @After
  public void tearDownFluo() throws Exception {
    if (miniFluo != null)
      miniFluo.close();
  }
}

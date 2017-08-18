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

package org.apache.fluo.recipes.accumulo.ops;

import java.util.List;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.recipes.core.common.RowRange;
import org.apache.fluo.recipes.core.common.TableOptimizations;
import org.apache.fluo.recipes.core.common.TransientRegistry;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for operating on the Fluo table used by recipes.
 *
 * @since 1.0.0
 */
public class TableOperations {

  private static final String RGB_CLASS =
      "org.apache.accumulo.server.master.balancer.RegexGroupBalancer";
  private static final String RGB_PATTERN_PROP = "table.custom.balancer.group.regex.pattern";
  private static final String RGB_DEFAULT_PROP = "table.custom.balancer.group.regex.default";
  private static final String TABLE_BALANCER_PROP = "table.balancer";

  private static final Logger logger = LoggerFactory.getLogger(TableOperations.class);

  private static Connector getConnector(FluoConfiguration fluoConfig) throws Exception {

    ZooKeeperInstance zki =
        new ZooKeeperInstance(new ClientConfiguration().withInstance(
            fluoConfig.getAccumuloInstance()).withZkHosts(fluoConfig.getAccumuloZookeepers()));

    Connector conn =
        zki.getConnector(fluoConfig.getAccumuloUser(),
            new PasswordToken(fluoConfig.getAccumuloPassword()));
    return conn;
  }

  /**
   * Make the requested table optimizations.
   * 
   * @param fluoConfig should contain information need to connect to Accumulo and name of Fluo table
   * @param tableOptim Will perform these optimizations on Fluo table in Accumulo.
   */
  public static void optimizeTable(FluoConfiguration fluoConfig, TableOptimizations tableOptim)
      throws Exception {
    Connector conn = getConnector(fluoConfig);

    TreeSet<Text> splits = new TreeSet<>();

    for (Bytes split : tableOptim.getSplits()) {
      splits.add(new Text(split.toArray()));
    }

    String table = fluoConfig.getAccumuloTable();
    conn.tableOperations().addSplits(table, splits);

    if (tableOptim.getTabletGroupingRegex() != null
        && !tableOptim.getTabletGroupingRegex().isEmpty()) {
      // was going to call :
      // conn.instanceOperations().testClassLoad(RGB_CLASS, TABLET_BALANCER_CLASS)
      // but that failed. See ACCUMULO-4068

      try {
        // setting this prop first intentionally because it should fail in 1.6
        conn.tableOperations().setProperty(table, RGB_PATTERN_PROP,
            tableOptim.getTabletGroupingRegex());
        conn.tableOperations().setProperty(table, RGB_DEFAULT_PROP, "none");
        conn.tableOperations().setProperty(table, TABLE_BALANCER_PROP, RGB_CLASS);
      } catch (AccumuloException e) {
        logger
            .warn("Unable to setup regex balancer (this is expected to fail in Accumulo 1.6.X) : "
                + e.getMessage());
        logger.debug("Unable to setup regex balancer (this is expected to fail in Accumulo 1.6.X)",
            e);
      }
    }
  }

  /**
   * This method will perform all registered table optimizations. It will call
   * {@link TableOptimizations#getConfiguredOptimizations(FluoConfiguration)} to obtain
   * optimizations to perform.
   */
  public static void optimizeTable(FluoConfiguration fluoConfig) throws Exception {
    TableOptimizations tableOptim = TableOptimizations.getConfiguredOptimizations(fluoConfig);
    optimizeTable(fluoConfig, tableOptim);
  }

  /**
   * Compact all transient regions that were registered using {@link TransientRegistry}
   */
  public static void compactTransient(FluoConfiguration fluoConfig) throws Exception {
    Connector conn = getConnector(fluoConfig);

    try (FluoClient client = FluoFactory.newClient(fluoConfig)) {
      SimpleConfiguration appConfig = client.getAppConfiguration();

      TransientRegistry transientRegistry = new TransientRegistry(appConfig);
      List<RowRange> ranges = transientRegistry.getTransientRanges();

      for (RowRange r : ranges) {
        long t1 = System.currentTimeMillis();
        conn.tableOperations().compact(fluoConfig.getAccumuloTable(),
            new Text(r.getStart().toArray()), new Text(r.getEnd().toArray()), true, true);
        long t2 = System.currentTimeMillis();
        logger.info("Compacted {} in {}ms", r, (t2 - t1));
      }
    }
  }

  public static void compactTransient(FluoConfiguration fluoConfig, RowRange tRange)
      throws Exception {
    Connector conn = getConnector(fluoConfig);
    conn.tableOperations().compact(fluoConfig.getAccumuloTable(),
        new Text(tRange.getStart().toArray()), new Text(tRange.getEnd().toArray()), true, true);
  }
}

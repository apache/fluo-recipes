/*
 * Copyright 2015 Fluo authors (see AUTHORS)
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

package io.fluo.recipes.accumulo.ops;

import java.util.List;
import java.util.TreeSet;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.recipes.common.Pirtos;
import io.fluo.recipes.common.RowRange;
import io.fluo.recipes.common.TransientRegistry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for operating on the Fluo table used by recipes.
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
   * This method will perform all post initialization recommended actions.
   */
  public static void optimizeTable(FluoConfiguration fluoConfig, Pirtos pirtos) throws Exception {

    Connector conn = getConnector(fluoConfig);

    TreeSet<Text> splits = new TreeSet<>();

    for (Bytes split : pirtos.getSplits()) {
      splits.add(new Text(split.toArray()));
    }

    String table = fluoConfig.getAccumuloTable();
    conn.tableOperations().addSplits(table, splits);

    if (pirtos.getTabletGroupingRegex() != null && !pirtos.getTabletGroupingRegex().isEmpty()) {
      // was going to call :
      // conn.instanceOperations().testClassLoad(RGB_CLASS, TABLET_BALANCER_CLASS)
      // but that failed. See ACCUMULO-4068

      try {
        // setting this prop first intentionally because it should fail in 1.6
        conn.tableOperations()
            .setProperty(table, RGB_PATTERN_PROP, pirtos.getTabletGroupingRegex());
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
   * Compact all transient regions that were registered using {@link TransientRegistry}
   */
  public static void compactTransient(FluoConfiguration fluoConfig) throws Exception {
    Connector conn = getConnector(fluoConfig);

    try (FluoClient client = FluoFactory.newClient(fluoConfig)) {
      Configuration appConfig = client.getAppConfiguration();

      TransientRegistry transientRegistry = new TransientRegistry(appConfig);
      List<RowRange> ranges = transientRegistry.getTransientRanges();

      for (RowRange r : ranges) {
        long t1 = System.currentTimeMillis();
        conn.tableOperations().compact(fluoConfig.getAccumuloTable(),
            new Text(r.getStart().toArray()), new Text(r.getEnd().toArray()), true, true);
        long t2 = System.currentTimeMillis();
        logger.info("Compacted {} {} in {}ms", r.getStart(), r.getEnd(), (t2 - t1));
      }
    }
  }
}

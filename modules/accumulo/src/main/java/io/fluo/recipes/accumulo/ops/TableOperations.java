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

import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.recipes.common.Pirtos;
import io.fluo.recipes.common.RowRange;
import io.fluo.recipes.common.TransientRegistry;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;

/**
 * Utility methods for operating on the Fluo table used by recipes.
 */

public class TableOperations {

  private static Connector getConnector(FluoConfiguration fluoConfig) throws Exception {
    ZooKeeperInstance zki =
        new ZooKeeperInstance(fluoConfig.getAccumuloInstance(), fluoConfig.getAccumuloZookeepers());
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
      System.out.println("split : " + split);
      splits.add(new Text(split.toArray()));
    }

    conn.tableOperations().addSplits(fluoConfig.getAccumuloTable(), splits);
  }

  /**
   * Compact all transient regions that were registered using {@link TransientRegistry}
   */
  public static void compactTransient(FluoConfiguration fluoConfig) throws Exception {
    Connector conn = getConnector(fluoConfig);

    Configuration appConfig = FluoFactory.newClient(fluoConfig).getAppConfiguration();

    TransientRegistry transientRegistry = new TransientRegistry(appConfig);
    List<RowRange> ranges = transientRegistry.getTransientRanges();

    for (RowRange r : ranges) {
      conn.tableOperations().compact(fluoConfig.getAccumuloTable(),
          new Text(r.getStart().toArray()), new Text(r.getEnd().toArray()), true, true);
    }
  }
}

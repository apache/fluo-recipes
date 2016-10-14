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

package org.apache.fluo.recipes.core.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;

/**
 * Post initialization recommended table optimizations.
 *
 * @since 1.0.0
 */
public class TableOptimizations {
  private List<Bytes> splits = new ArrayList<>();
  private String tabletGroupingRegex = "";

  public void setSplits(List<Bytes> splits) {
    this.splits.clear();
    this.splits.addAll(splits);
  }

  /**
   * @return A recommended set of splits points to add to a Fluo table after initialization.
   */
  public List<Bytes> getSplits() {
    return Collections.unmodifiableList(splits);
  }

  public void setTabletGroupingRegex(String tgr) {
    Objects.requireNonNull(tgr);
    this.tabletGroupingRegex = tgr;
  }

  public String getTabletGroupingRegex() {
    return "(" + tabletGroupingRegex + ").*";
  }

  public void merge(TableOptimizations other) {
    splits.addAll(other.splits);
    if (tabletGroupingRegex.length() > 0 && other.tabletGroupingRegex.length() > 0) {
      tabletGroupingRegex += "|" + other.tabletGroupingRegex;
    } else {
      tabletGroupingRegex += other.tabletGroupingRegex;
    }
  }

  public static interface TableOptimizationsFactory {
    TableOptimizations getTableOptimizations(String key, SimpleConfiguration appConfig);
  }

  private static final String PREFIX = "recipes.optimizations.";

  /**
   * This method provides a standard way to register a table optimization for the Fluo table before
   * initialization. After Fluo is initialized, the optimizations can be retrieved by calling
   * {@link #getConfiguredOptimizations(FluoConfiguration)}.
   * 
   * @param appConfig config, likely obtained from calling
   *        {@link FluoConfiguration#getAppConfiguration()}
   * @param key A unique identifier for the optimization
   * @param clazz The optimization factory type.
   */
  public static void registerOptimization(SimpleConfiguration appConfig, String key,
      Class<? extends TableOptimizationsFactory> clazz) {
    appConfig.setProperty(PREFIX + key, clazz.getName());
  }

  /**
   * A utility method to get all registered table optimizations. Many recipes will automatically
   * register table optimizations when configured.
   */
  public static TableOptimizations getConfiguredOptimizations(FluoConfiguration fluoConfig) {
    try (FluoClient client = FluoFactory.newClient(fluoConfig)) {
      SimpleConfiguration appConfig = client.getAppConfiguration();
      TableOptimizations tableOptim = new TableOptimizations();

      SimpleConfiguration subset = appConfig.subset(PREFIX.substring(0, PREFIX.length() - 1));
      Iterator<String> keys = subset.getKeys();
      while (keys.hasNext()) {
        String key = keys.next();
        String clazz = subset.getString(key);
        try {
          TableOptimizationsFactory factory =
              Class.forName(clazz).asSubclass(TableOptimizationsFactory.class).newInstance();
          tableOptim.merge(factory.getTableOptimizations(key, appConfig));
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }

      return tableOptim;
    }
  }
}

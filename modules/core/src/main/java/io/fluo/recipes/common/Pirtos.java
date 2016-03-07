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

package io.fluo.recipes.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.recipes.export.ExportQueue;
import io.fluo.recipes.map.CollisionFreeMap;
import org.apache.commons.configuration.Configuration;

/**
 * Post initialization recommended table optimizations.
 */

public class Pirtos {
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

  public void merge(Pirtos other) {
    splits.addAll(other.splits);
    if (tabletGroupingRegex.length() > 0 && other.tabletGroupingRegex.length() > 0) {
      tabletGroupingRegex += "|" + other.tabletGroupingRegex;
    } else {
      tabletGroupingRegex += other.tabletGroupingRegex;
    }
  }

  /**
   * A utility method to get table optimizations for all configured recipes.
   */
  public static Pirtos getConfiguredOptimizations(FluoConfiguration fluoConfig) {
    try (FluoClient client = FluoFactory.newClient(fluoConfig)) {
      Configuration appConfig = client.getAppConfiguration();
      Pirtos pirtos = new Pirtos();

      pirtos.merge(ExportQueue.getTableOptimizations(appConfig));
      pirtos.merge(CollisionFreeMap.getTableOptimizations(appConfig));

      return pirtos;
    }
  }
}

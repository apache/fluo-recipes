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
import java.util.Iterator;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;

/**
 * This class offers a standard way to register transient ranges. The project level documentation
 * provides a comprehensive overview.
 *
 * @since 1.0.0
 */
public class TransientRegistry {

  private SimpleConfiguration appConfig;
  private static final String PREFIX = "recipes.transientRange.";

  /**
   * @param appConfig Fluo application config. Can be obtained from
   *        {@link FluoConfiguration#getAppConfiguration()} before initializing fluo when adding
   *        Transient ranges. After Fluo is initialized, app config can be obtained from
   *        {@link FluoClient#getAppConfiguration()} or
   *        {@link org.apache.fluo.api.observer.Observer.Context#getAppConfiguration()}
   */
  public TransientRegistry(SimpleConfiguration appConfig) {
    this.appConfig = appConfig;
  }

  /**
   * This method is expected to be called before Fluo is initialized to register transient ranges.
   *
   */
  public void addTransientRange(String id, RowRange range) {
    String start = DatatypeConverter.printHexBinary(range.getStart().toArray());
    String end = DatatypeConverter.printHexBinary(range.getEnd().toArray());

    appConfig.setProperty(PREFIX + id, start + ":" + end);
  }

  /**
   * This method is expected to be called after Fluo is initialized to get the ranges that were
   * registered before initialization.
   */
  public List<RowRange> getTransientRanges() {
    List<RowRange> ranges = new ArrayList<>();
    Iterator<String> keys = appConfig.getKeys(PREFIX.substring(0, PREFIX.length() - 1));
    while (keys.hasNext()) {
      String key = keys.next();
      String val = appConfig.getString(key);
      String[] sa = val.split(":");
      RowRange rowRange =
          new RowRange(Bytes.of(DatatypeConverter.parseHexBinary(sa[0])),
              Bytes.of(DatatypeConverter.parseHexBinary(sa[1])));
      ranges.add(rowRange);
    }
    return ranges;
  }
}

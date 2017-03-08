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

package org.apache.fluo.recipes.core.combine;

import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.recipes.core.combine.CombineQueue.FluentArg1;
import org.apache.fluo.recipes.core.combine.CombineQueue.FluentArg2;
import org.apache.fluo.recipes.core.combine.CombineQueue.FluentArg3;
import org.apache.fluo.recipes.core.combine.CombineQueue.FluentOptions;
import org.apache.fluo.recipes.core.common.RowRange;
import org.apache.fluo.recipes.core.common.TableOptimizations;
import org.apache.fluo.recipes.core.common.TransientRegistry;

// this class intentionally package private
class CqConfigurator implements FluentArg1, FluentArg2, FluentArg3, FluentOptions {

  static final String UPDATE_RANGE_END = ":u:~";

  static final String DATA_RANGE_END = ":d:~";

  int numBuckets;
  Integer bucketsPerTablet = null;

  Long bufferSize;

  String keyType;
  String valueType;
  String cqId;

  static final int DEFAULT_BUCKETS_PER_TABLET = 10;

  static final long DEFAULT_BUFFER_SIZE = 1 << 22;

  static final String PREFIX = "recipes.cfm.";

  CqConfigurator(String id) {
    Objects.requireNonNull(id);
    Preconditions.checkArgument(!id.contains(":"), "Combine queue id cannot contain ':'");
    this.cqId = id;
  }

  @Override
  public FluentOptions buckets(int numBuckets) {
    Preconditions.checkArgument(numBuckets > 0);
    this.numBuckets = numBuckets;
    return this;
  }

  @Override
  public FluentArg3 valueType(String valType) {
    this.valueType = Objects.requireNonNull(valType);
    return this;
  }


  @Override
  public FluentArg3 valueType(Class<?> valType) {
    this.valueType = valType.getName();
    return this;
  }

  @Override
  public FluentArg2 keyType(String keyType) {
    this.keyType = Objects.requireNonNull(keyType);
    return this;
  }

  @Override
  public FluentArg2 keyType(Class<?> keyType) {
    this.keyType = keyType.getName();
    return this;
  }

  @Override
  public FluentOptions bufferSize(long bufferSize) {
    Preconditions.checkArgument(bufferSize > 0, "Buffer size must be positive");
    this.bufferSize = bufferSize;
    return this;
  }

  @Override
  public FluentOptions bucketsPerTablet(int bucketsPerTablet) {
    Preconditions.checkArgument(bucketsPerTablet > 0, "bucketsPerTablet is <= 0 : "
        + bucketsPerTablet);
    this.bucketsPerTablet = bucketsPerTablet;
    return this;
  }

  @Override
  public void save(FluoConfiguration fluoConfig) {
    SimpleConfiguration appConfig = fluoConfig.getAppConfiguration();
    appConfig.setProperty(PREFIX + cqId + ".buckets", numBuckets + "");
    appConfig.setProperty(PREFIX + cqId + ".key", keyType);
    appConfig.setProperty(PREFIX + cqId + ".val", valueType);
    if (bufferSize != null) {
      appConfig.setProperty(PREFIX + cqId + ".bufferSize", bufferSize);
    }
    if (bucketsPerTablet != null) {
      appConfig.setProperty(PREFIX + cqId + ".bucketsPerTablet", bucketsPerTablet);
    }

    Bytes dataRangeEnd = Bytes.of(cqId + DATA_RANGE_END);
    Bytes updateRangeEnd = Bytes.of(cqId + UPDATE_RANGE_END);

    new TransientRegistry(fluoConfig.getAppConfiguration()).addTransientRange("cfm." + cqId,
        new RowRange(dataRangeEnd, updateRangeEnd));

    TableOptimizations.registerOptimization(appConfig, cqId, CombineQueue.Optimizer.class);
  }

  static long getBufferSize(String cqId, SimpleConfiguration appConfig) {
    return appConfig.getLong(PREFIX + cqId + ".bufferSize", DEFAULT_BUFFER_SIZE);
  }

  static String getValueType(String cqId, SimpleConfiguration appConfig) {
    return appConfig.getString(PREFIX + cqId + ".val");
  }

  static String getKeyType(String cqId, SimpleConfiguration appConfig) {
    return appConfig.getString(PREFIX + cqId + ".key");
  }

  static int getBucketsPerTablet(String cqId, SimpleConfiguration appConfig) {
    return appConfig.getInt(PREFIX + cqId + ".bucketsPerTablet", DEFAULT_BUCKETS_PER_TABLET);
  }

  static int getNumBucket(String cqId, SimpleConfiguration appConfig) {
    return appConfig.getInt(PREFIX + cqId + ".buckets");
  }
}

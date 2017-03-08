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

package org.apache.fluo.recipes.core.export;

import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.recipes.core.common.RowRange;
import org.apache.fluo.recipes.core.common.TableOptimizations;
import org.apache.fluo.recipes.core.common.TransientRegistry;
import org.apache.fluo.recipes.core.export.ExportQueue.FluentArg1;
import org.apache.fluo.recipes.core.export.ExportQueue.FluentArg2;
import org.apache.fluo.recipes.core.export.ExportQueue.FluentArg3;
import org.apache.fluo.recipes.core.export.ExportQueue.FluentOptions;
import org.apache.fluo.recipes.core.export.ExportQueue.Optimizer;

// This class intentionally package private.
class FluentConfigurator implements FluentArg1, FluentArg2, FluentArg3, FluentOptions {

  static final long DEFAULT_BUFFER_SIZE = 1 << 20;
  static final int DEFAULT_BUCKETS_PER_TABLET = 10;
  static final String PREFIX = "recipes.exportQueue.";

  String queueId;
  Long bufferSize = null;
  int buckets;
  String valueType;
  Integer bucketsPerTablet = null;
  String keyType;
  String exporterType;

  FluentConfigurator(String queueId) {
    this.queueId = queueId;
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

  void save(SimpleConfiguration appConfig) {
    appConfig.setProperty(PREFIX + queueId + ".buckets", buckets + "");
    appConfig.setProperty(PREFIX + queueId + ".key", keyType);
    appConfig.setProperty(PREFIX + queueId + ".val", valueType);

    if (exporterType != null) {
      appConfig.setProperty(PREFIX + queueId + ".exporter", exporterType);
    }

    if (bufferSize != null) {
      appConfig.setProperty(PREFIX + queueId + ".bufferSize", bufferSize);
    }

    if (bucketsPerTablet != null) {
      appConfig.setProperty(PREFIX + queueId + ".bucketsPerTablet", bucketsPerTablet);
    }

    Bytes exportRangeStart = Bytes.of(queueId + ExportQueue.RANGE_BEGIN);
    Bytes exportRangeStop = Bytes.of(queueId + ExportQueue.RANGE_END);

    new TransientRegistry(appConfig).addTransientRange("exportQueue." + queueId, new RowRange(
        exportRangeStart, exportRangeStop));

    TableOptimizations.registerOptimization(appConfig, queueId, Optimizer.class);
  }

  @Override
  public void save(FluoConfiguration fluoConfig) {
    save(fluoConfig.getAppConfiguration());
  }

  static FluentConfigurator load(String queueId, SimpleConfiguration appConfig) {
    FluentConfigurator fc = new FluentConfigurator(queueId);
    fc.buckets = appConfig.getInt(PREFIX + queueId + ".buckets");
    fc.keyType = appConfig.getString(PREFIX + queueId + ".key");
    fc.valueType = appConfig.getString(PREFIX + queueId + ".val");
    fc.bufferSize = appConfig.getLong(PREFIX + queueId + ".bufferSize", DEFAULT_BUFFER_SIZE);
    fc.bucketsPerTablet =
        appConfig.getInt(PREFIX + queueId + ".bucketsPerTablet", DEFAULT_BUCKETS_PER_TABLET);
    fc.exporterType = appConfig.getString(PREFIX + queueId + ".exporter", null);
    return fc;
  }

  long getBufferSize() {
    if (bufferSize == null) {
      return DEFAULT_BUFFER_SIZE;
    }

    return bufferSize;
  }

  int getBucketsPerTablet() {
    if (bucketsPerTablet == null) {
      return DEFAULT_BUCKETS_PER_TABLET;
    }

    return bucketsPerTablet;
  }

  @Override
  public FluentOptions buckets(int numBuckets) {
    Preconditions.checkArgument(numBuckets > 0);
    this.buckets = numBuckets;
    return this;
  }

  @Override
  public FluentArg3 valueType(String valueType) {
    this.valueType = Objects.requireNonNull(valueType);
    return this;
  }

  @Override
  public FluentArg3 valueType(Class<?> valueType) {
    this.valueType = valueType.getName();
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
}

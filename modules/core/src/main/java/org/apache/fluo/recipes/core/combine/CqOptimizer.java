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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.fluo.recipes.core.common.TableOptimizations;

// This class intentionally package private.
class CqOptimizer {

  public static TableOptimizations getTableOptimizations(String cqId, SimpleConfiguration appConfig) {
    int numBuckets = CqConfigurator.getNumBucket(cqId, appConfig);
    int bpt = CqConfigurator.getBucketsPerTablet(cqId, appConfig);

    BytesBuilder rowBuilder = Bytes.builder();
    rowBuilder.append(cqId);

    List<Bytes> dataSplits = new ArrayList<>();
    for (int i = bpt; i < numBuckets; i += bpt) {
      String bucketId = CombineQueueImpl.genBucketId(i, numBuckets);
      rowBuilder.setLength(cqId.length());
      dataSplits.add(rowBuilder.append(":d:").append(bucketId).toBytes());
    }
    Collections.sort(dataSplits);

    List<Bytes> updateSplits = new ArrayList<>();
    for (int i = bpt; i < numBuckets; i += bpt) {
      String bucketId = CombineQueueImpl.genBucketId(i, numBuckets);
      rowBuilder.setLength(cqId.length());
      updateSplits.add(rowBuilder.append(":u:").append(bucketId).toBytes());
    }
    Collections.sort(updateSplits);

    Bytes dataRangeEnd = Bytes.of(cqId + CqConfigurator.DATA_RANGE_END);
    Bytes updateRangeEnd = Bytes.of(cqId + CqConfigurator.UPDATE_RANGE_END);

    List<Bytes> splits = new ArrayList<>();
    splits.add(dataRangeEnd);
    splits.add(updateRangeEnd);
    splits.addAll(dataSplits);
    splits.addAll(updateSplits);

    TableOptimizations tableOptim = new TableOptimizations();
    tableOptim.setSplits(splits);

    tableOptim.setTabletGroupingRegex(Pattern.quote(cqId + ":") + "[du]:");

    return tableOptim;
  }
}

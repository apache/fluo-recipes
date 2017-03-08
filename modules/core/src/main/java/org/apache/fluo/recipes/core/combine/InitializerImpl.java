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

import com.google.common.hash.Hashing;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.recipes.core.combine.CombineQueue.Initializer;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;

// intentionally package private
class InitializerImpl<K, V> implements Initializer<K, V> {
  private static final long serialVersionUID = 1L;

  private Bytes dataPrefix;

  private SimpleSerializer serializer;

  private int numBuckets = -1;

  InitializerImpl(String cqId, int numBuckets, SimpleSerializer serializer) {
    this.dataPrefix = Bytes.of(cqId + ":d:");
    this.numBuckets = numBuckets;
    this.serializer = serializer;
  }

  public RowColumnValue convert(K key, V val) {
    byte[] k = serializer.serialize(key);
    int hash = Hashing.murmur3_32().hashBytes(k).asInt();
    String bucketId = CombineQueueImpl.genBucketId(Math.abs(hash % numBuckets), numBuckets);

    BytesBuilder bb = Bytes.builder(dataPrefix.length() + bucketId.length() + 1 + k.length);
    Bytes row = bb.append(dataPrefix).append(bucketId).append(':').append(k).toBytes();
    byte[] v = serializer.serialize(val);

    return new RowColumnValue(row, CombineQueueImpl.DATA_COLUMN, Bytes.of(v));
  }
}

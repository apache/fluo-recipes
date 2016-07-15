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

package org.apache.fluo.recipes.accumulo.export;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.data.Mutation;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;

/**
 * Implemented by users to export data to Accumulo by comparing the differences between a
 * RowColumn/Bytes map that is generated for old and new data and represents how the data should
 * exist in Accumulo. When comparing each row/column/value (RCV) of old and new data, mutations are
 * generated using the following rules:
 * <ul>
 * <li>If old and new data have the same RCV, nothing is done.
 * <li>If old and new data have same row/column but different values, an update mutation is created
 * for the row/column.
 * <li>If old data has a row/column that is not in the new data, a delete mutation is generated.
 * <li>If new data has a row/column that is not in the old data, an insert mutation is generated.
 * <li>Only one mutation is generated per row.
 * <li>The export sequence number is used for the timestamp in the mutation.
 * </ul>
 *
 * @param <K> Export queue key type
 * @param <V> Type of export value object used to generate data
 * @since 1.0.0
 */
public abstract class DifferenceExport<K, V> implements AccumuloExport<K> {

  private Optional<V> oldVal;
  private Optional<V> newVal;

  public DifferenceExport() {}

  public DifferenceExport(Optional<V> oldVal, Optional<V> newVal) {
    Objects.requireNonNull(oldVal);
    Objects.requireNonNull(newVal);
    Preconditions.checkArgument(oldVal.isPresent() || newVal.isPresent(),
        "At least one value must be set");
    this.oldVal = oldVal;
    this.newVal = newVal;
  }

  /**
   * Generates RowColumn/Bytes map of how data should exist in Accumulo. This map is generated for
   * old and new data and compared to create export mutations that will be written to Accumulo.
   * 
   * @param key Export queue key
   * @param val Export value object
   * @return RowColumn/Bytes map of how data should exist in Accumulo
   */
  protected abstract Map<RowColumn, Bytes> generateData(K key, Optional<V> val);

  @Override
  public Collection<Mutation> toMutations(K key, long seq) {
    Map<RowColumn, Bytes> oldData = generateData(key, oldVal);
    Map<RowColumn, Bytes> newData = generateData(key, newVal);

    Map<Bytes, Mutation> mutationMap = new HashMap<>();
    for (Map.Entry<RowColumn, Bytes> entry : oldData.entrySet()) {
      RowColumn rc = entry.getKey();
      if (!newData.containsKey(rc)) {
        Mutation m = mutationMap.computeIfAbsent(rc.getRow(), r -> new Mutation(r.toArray()));
        m.putDelete(rc.getColumn().getFamily().toArray(), rc.getColumn().getQualifier().toArray(),
            seq);
      }
    }
    for (Map.Entry<RowColumn, Bytes> entry : newData.entrySet()) {
      RowColumn rc = entry.getKey();
      Column col = rc.getColumn();
      Bytes newVal = entry.getValue();
      Bytes oldVal = oldData.get(rc);
      if (oldVal == null || !oldVal.equals(newVal)) {
        Mutation m = mutationMap.computeIfAbsent(rc.getRow(), r -> new Mutation(r.toArray()));
        m.put(col.getFamily().toArray(), col.getQualifier().toArray(), seq, newVal.toArray());
      }
    }
    return mutationMap.values();
  }
}

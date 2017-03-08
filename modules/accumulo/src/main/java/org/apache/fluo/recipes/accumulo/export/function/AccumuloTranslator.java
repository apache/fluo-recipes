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

package org.apache.fluo.recipes.accumulo.export.function;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.accumulo.core.data.Mutation;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.recipes.core.export.SequencedExport;

/**
 * This interface is used by {@link AccumuloExporter} to translated exports into Accumulo mutations.
 * 
 * @see AccumuloExporter
 * @since 1.1.0
 */
@FunctionalInterface
public interface AccumuloTranslator<K, V> {
  /**
   * This function should convert the export to zero or more mutations, passing the mutations to the
   * consumer.
   */
  void translate(SequencedExport<K, V> export, Consumer<Mutation> mutationWriter);

  /**
   * Generates Accumulo mutations by comparing the differences between a RowColumn/Bytes map that is
   * generated for old and new data and represents how the data should exist in Accumulo. When
   * comparing each row/column/value (RCV) of old and new data, mutations are generated using the
   * following rules:
   * <ul>
   * <li>If old and new data have the same RCV, nothing is done.
   * <li>If old and new data have same row/column but different values, an update mutation is
   * created for the row/column.
   * <li>If old data has a row/column that is not in the new data, a delete mutation is generated.
   * <li>If new data has a row/column that is not in the old data, an insert mutation is generated.
   * <li>Only one mutation is generated per row.
   * <li>The export sequence number is used for the timestamp in the mutation.
   * </ul>
   *
   * @param consumer generated mutations will be output to this consumer
   * @param oldData Map containing old row/column data
   * @param newData Map containing new row/column data
   * @param seq Export sequence number
   */
  public static void generateMutations(long seq, Map<RowColumn, Bytes> oldData,
      Map<RowColumn, Bytes> newData, Consumer<Mutation> consumer) {
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

    mutationMap.values().forEach(consumer);
  }
}

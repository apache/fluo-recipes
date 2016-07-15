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
import java.util.function.Predicate;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.recipes.core.transaction.LogEntry;
import org.apache.fluo.recipes.core.transaction.TxLog;

/**
 * An implementation of {@link AccumuloExport} that replicates a Fluo table to Accumulo using a
 * TxLog
 *
 * @param <K> Type of export queue key
 * @since 1.0.0
 */
public class ReplicationExport<K> implements AccumuloExport<K> {

  private TxLog txLog;

  public ReplicationExport() {}

  public ReplicationExport(TxLog txLog) {
    Objects.requireNonNull(txLog);
    this.txLog = txLog;
  }

  public static Predicate<LogEntry> getFilter() {
    return le -> le.getOp().equals(LogEntry.Operation.DELETE)
        || le.getOp().equals(LogEntry.Operation.SET);
  }

  @Override
  public Collection<Mutation> toMutations(K key, long seq) {
    Map<Bytes, Mutation> mutationMap = new HashMap<>();
    for (LogEntry le : txLog.getLogEntries()) {
      LogEntry.Operation op = le.getOp();
      Column col = le.getColumn();
      byte[] cf = col.getFamily().toArray();
      byte[] cq = col.getQualifier().toArray();
      byte[] cv = col.getVisibility().toArray();
      if (op.equals(LogEntry.Operation.DELETE) || op.equals(LogEntry.Operation.SET)) {
        Mutation m = mutationMap.computeIfAbsent(le.getRow(), k -> new Mutation(k.toArray()));
        if (op.equals(LogEntry.Operation.DELETE)) {
          if (col.isVisibilitySet()) {
            m.putDelete(cf, cq, new ColumnVisibility(cv), seq);
          } else {
            m.putDelete(cf, cq, seq);
          }
        } else {
          if (col.isVisibilitySet()) {
            m.put(cf, cq, new ColumnVisibility(cv), seq, le.getValue().toArray());
          } else {
            m.put(cf, cq, seq, le.getValue().toArray());
          }
        }
      }
    }
    return mutationMap.values();
  }
}

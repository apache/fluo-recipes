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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.recipes.accumulo.export.AccumuloExporter;
import org.apache.fluo.recipes.accumulo.export.function.AccumuloTranslator;
import org.apache.fluo.recipes.core.export.SequencedExport;
import org.apache.fluo.recipes.core.transaction.LogEntry;
import org.apache.fluo.recipes.core.transaction.RecordingTransaction;
import org.apache.fluo.recipes.core.transaction.TxLog;

/**
 * Supports replicating data to Accumulo using a {@link TxLog}. The method {@link #getTranslator()}
 * can be used with {@link org.apache.fluo.recipes.accumulo.export.function.AccumuloExporter} to
 * export {@link TxLog} objects.
 */
@SuppressWarnings("deprecation")
public class AccumuloReplicator extends AccumuloExporter<String, TxLog> {

  /**
   * @deprecated since 1.1.0 use
   *             {@link org.apache.fluo.recipes.accumulo.export.function.AccumuloExporter} with
   *             {@link #getTranslator()} instead.
   */
  @Override
  protected void translate(SequencedExport<String, TxLog> export, Consumer<Mutation> consumer) {
    generateMutations(export.getSequence(), export.getValue(), consumer);
  }

  /**
   * Returns LogEntry filter for Accumulo replication.
   * 
   * @see RecordingTransaction#wrap(org.apache.fluo.api.client.TransactionBase, Predicate)
   */
  public static Predicate<LogEntry> getFilter() {
    return le -> le.getOp().equals(LogEntry.Operation.DELETE)
        || le.getOp().equals(LogEntry.Operation.SET);
  }

  /**
   * @return A translator from TxLog to Mutations
   * @since 1.1.0
   */
  public static AccumuloTranslator<String, TxLog> getTranslator() {
    return (export, consumer) -> generateMutations(export.getSequence(), export.getValue(),
        consumer);
  }

  /**
   * Generates Accumulo mutations from a Transaction log. Used to Replicate Fluo table to Accumulo.
   *
   * @param txLog Transaction log
   * @param seq Export sequence number
   * @param consumer generated mutations will be output to this consumer
   */
  public static void generateMutations(long seq, TxLog txLog, Consumer<Mutation> consumer) {
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
    mutationMap.values().forEach(consumer);
  }
}

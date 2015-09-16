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

package io.fluo.recipes.accumulo.export;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.recipes.transaction.LogEntry;
import io.fluo.recipes.transaction.TxLog;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;

public class AccumuloReplicator extends AccumuloExporter<Bytes, TxLog> {

  @Override
  protected Collection<Mutation> convert(Bytes key, long seq, TxLog txLog) {
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

  public static Predicate<LogEntry> getFilter() {
    return le -> le.getOp().equals(LogEntry.Operation.DELETE)
        || le.getOp().equals(LogEntry.Operation.SET);
  }
}

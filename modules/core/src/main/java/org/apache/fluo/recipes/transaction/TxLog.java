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

package org.apache.fluo.recipes.transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.RowColumn;

/**
 * Contains list of operations (GET, SET, DELETE) performed during a {@link RecordingTransaction}
 */
public class TxLog {

  private List<LogEntry> logEntries = new ArrayList<>();

  public TxLog() {}

  /**
   * Adds LogEntry to TxLog
   */
  public void add(LogEntry entry) {
    logEntries.add(entry);
  }

  /**
   * Adds LogEntry to TxLog if it passes filter
   */
  public void filteredAdd(LogEntry entry, Predicate<LogEntry> filter) {
    if (filter.test(entry)) {
      add(entry);
    }
  }

  /**
   * Returns all LogEntry in TxLog
   */
  public List<LogEntry> getLogEntries() {
    return Collections.unmodifiableList(logEntries);
  }

  /**
   * Returns true if TxLog is empty
   */
  public boolean isEmpty() {
    return logEntries.isEmpty();
  }

  /**
   * Returns a map of RowColumn changes given an operation
   */
  public Map<RowColumn, Bytes> getOperationMap(LogEntry.Operation op) {
    Map<RowColumn, Bytes> opMap = new HashMap<>();
    for (LogEntry entry : logEntries) {
      if (entry.getOp().equals(op)) {
        opMap.put(new RowColumn(entry.getRow(), entry.getColumn()), entry.getValue());
      }
    }
    return opMap;
  }
}

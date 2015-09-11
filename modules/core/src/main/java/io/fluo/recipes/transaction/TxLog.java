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

package io.fluo.recipes.transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;

public class TxLog {

  private List<LogEntry> logEntries = new ArrayList<LogEntry>();

  public enum TxType {
    SET, DELETE
  }

  public TxLog() {}

  public static class LogEntry {

    private TxType type;
    private Bytes row;
    private Column col;
    private Bytes value;

    private LogEntry() {}

    public LogEntry(TxType type, Bytes row, Column col, Bytes value) {
      Preconditions.checkNotNull(type);
      Preconditions.checkNotNull(row);
      Preconditions.checkNotNull(col);
      Preconditions.checkNotNull(value);
      this.type = type;
      this.row = row;
      this.col = col;
      this.value = value;
    }

    public TxType getType() {
      return type;
    }

    public Bytes getRow() {
      return row;
    }

    public Column getColumn() {
      return col;
    }

    public Bytes getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof LogEntry) {
        LogEntry other = (LogEntry) o;
        return ((type == other.type) && row.equals(other.row) && col.equals(other.col) && value
            .equals(other.value));
      }
      return false;
    }

    @Override
    public String toString() {
      return "LogEntry{type=" + type + ", row=" + row + ", col=" + col + ", value=" + value + '}';
    }
  }

  public void addSet(Bytes row, Column col, Bytes value) {
    logEntries.add(new LogEntry(TxType.SET, row, col, value));
  }

  public void addDelete(Bytes row, Column col) {
    logEntries.add(new LogEntry(TxType.DELETE, row, col, Bytes.EMPTY));
  }

  public List<LogEntry> getLogEntries() {
    return Collections.unmodifiableList(logEntries);
  }
}

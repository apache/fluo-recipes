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

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.exceptions.AlreadySetException;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;

/**
 * An implementation of {@link TransactionBase} that logs all transactions operations (GET, SET, or
 * DELETE) in a {@link TxLog} that can be used for exports
 */
public class RecordingTransactionBase implements TransactionBase {

  private final TransactionBase txb;
  private final TxLog txLog = new TxLog();
  private final Predicate<LogEntry> filter;

  RecordingTransactionBase(TransactionBase txb, Predicate<LogEntry> filter) {
    this.txb = txb;
    this.filter = filter;
  }

  RecordingTransactionBase(TransactionBase txb) {
    this(txb, le -> true);
  }

  @Override
  public void setWeakNotification(Bytes row, Column col) {
    txb.setWeakNotification(row, col);
  }

  @Override
  public void set(Bytes row, Column col, Bytes value) throws AlreadySetException {
    txLog.filteredAdd(LogEntry.newSet(row, col, value), filter);
    txb.set(row, col, value);
  }

  @Override
  public void delete(Bytes row, Column col) {
    txLog.filteredAdd(LogEntry.newDelete(row, col), filter);
    txb.delete(row, col);
  }

  /**
   * Logs GETs for returned Row/Columns. Requests that return no data will not be logged.
   */
  @Override
  public Bytes get(Bytes row, Column col) {
    Bytes val = txb.get(row, col);
    if (val != null) {
      txLog.filteredAdd(LogEntry.newGet(row, col, val), filter);
    }
    return val;
  }

  /**
   * Logs GETs for returned Row/Columns. Requests that return no data will not be logged.
   */
  @Override
  public Map<Column, Bytes> get(Bytes row, Set<Column> columns) {
    Map<Column, Bytes> colVal = txb.get(row, columns);
    for (Map.Entry<Column, Bytes> entry : colVal.entrySet()) {
      txLog.filteredAdd(LogEntry.newGet(row, entry.getKey(), entry.getValue()), filter);
    }
    return colVal;
  }

  /**
   * Logs GETs for returned Row/Columns. Requests that return no data will not be logged.
   */
  @Override
  public Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Set<Column> columns) {
    Map<Bytes, Map<Column, Bytes>> rowColVal = txb.get(rows, columns);
    for (Map.Entry<Bytes, Map<Column, Bytes>> rowEntry : rowColVal.entrySet()) {
      for (Map.Entry<Column, Bytes> colEntry : rowEntry.getValue().entrySet()) {
        txLog.filteredAdd(
            LogEntry.newGet(rowEntry.getKey(), colEntry.getKey(), colEntry.getValue()), filter);
      }
    }
    return rowColVal;
  }

  /**
   * Logs GETs for Row/Columns returned by iterators. Requests that return no data will not be
   * logged.
   */
  @Override
  public RowIterator get(ScannerConfiguration config) {
    final RowIterator rowIter = txb.get(config);
    if (rowIter != null) {
      return new RowIterator() {

        @Override
        public boolean hasNext() {
          return rowIter.hasNext();
        }

        @Override
        public Map.Entry<Bytes, ColumnIterator> next() {
          final Map.Entry<Bytes, ColumnIterator> rowEntry = rowIter.next();
          if ((rowEntry != null) && (rowEntry.getValue() != null)) {
            final ColumnIterator colIter = rowEntry.getValue();
            return new AbstractMap.SimpleEntry<Bytes, ColumnIterator>(rowEntry.getKey(),
                new ColumnIterator() {

                  @Override
                  public boolean hasNext() {
                    return colIter.hasNext();
                  }

                  @Override
                  public Map.Entry<Column, Bytes> next() {
                    Map.Entry<Column, Bytes> colEntry = colIter.next();
                    if (colEntry != null) {
                      txLog.filteredAdd(LogEntry.newGet(rowEntry.getKey(), colEntry.getKey(),
                          colEntry.getValue()), filter);
                    }
                    return colEntry;
                  }
                });
          }
          return rowEntry;
        }
      };
    }
    return rowIter;
  }

  @Override
  public long getStartTimestamp() {
    return txb.getStartTimestamp();
  }

  public TxLog getTxLog() {
    return txLog;
  }

  /**
   * Creates a RecordingTransactionBase by wrapping an existing TransactionBase
   */
  public static RecordingTransactionBase wrap(TransactionBase txb) {
    return new RecordingTransactionBase(txb);
  }

  /**
   * Creates a RecordingTransactionBase using the provided LogEntry filter function and existing
   * TransactionBase
   */
  public static RecordingTransactionBase wrap(TransactionBase txb, Predicate<LogEntry> filter) {
    return new RecordingTransactionBase(txb, filter);
  }
}

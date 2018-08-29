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

package org.apache.fluo.recipes.core.transaction;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.collect.Iterators;
import org.apache.fluo.api.client.AbstractTransactionBase;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.client.scanner.RowScannerBuilder;
import org.apache.fluo.api.client.scanner.ScannerBuilder;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.exceptions.AlreadySetException;

/**
 * An implementation of {@link TransactionBase} that logs all transactions operations (GET, SET, or
 * DELETE) in a {@link TxLog} that can be used for exports
 *
 * @since 1.0.0
 */
public class RecordingTransactionBase extends AbstractTransactionBase implements TransactionBase {

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

  @Override
  public Map<RowColumn, Bytes> get(Collection<RowColumn> rowColumns) {
    Map<RowColumn, Bytes> rowColVal = txb.get(rowColumns);
    for (Map.Entry<RowColumn, Bytes> rce : rowColVal.entrySet()) {
      txLog.filteredAdd(
          LogEntry.newGet(rce.getKey().getRow(), rce.getKey().getColumn(), rce.getValue()), filter);
    }
    return rowColVal;
  }

  private class RtxIterator implements Iterator<RowColumnValue> {

    private Iterator<RowColumnValue> iter;

    public RtxIterator(Iterator<RowColumnValue> iterator) {
      this.iter = iterator;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public RowColumnValue next() {
      RowColumnValue rcv = iter.next();
      txLog.filteredAdd(LogEntry.newGet(rcv.getRow(), rcv.getColumn(), rcv.getValue()), filter);
      return rcv;
    }

  }

  private class RtxCellSanner implements CellScanner {

    private CellScanner scanner;

    public RtxCellSanner(CellScanner scanner) {
      this.scanner = scanner;
    }

    @Override
    public Iterator<RowColumnValue> iterator() {
      return new RtxIterator(scanner.iterator());
    }

  }

  private class RtxCVIterator implements Iterator<ColumnValue> {

    private Iterator<ColumnValue> iter;
    private Bytes row;

    public RtxCVIterator(Bytes row, Iterator<ColumnValue> iterator) {
      this.row = row;
      this.iter = iterator;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public ColumnValue next() {
      ColumnValue cv = iter.next();
      txLog.filteredAdd(LogEntry.newGet(row, cv.getColumn(), cv.getValue()), filter);
      return cv;
    }

  }

  private class RtxColumnScanner implements ColumnScanner {

    private ColumnScanner cs;

    public RtxColumnScanner(ColumnScanner cs) {
      this.cs = cs;
    }

    @Override
    public Iterator<ColumnValue> iterator() {
      return new RtxCVIterator(cs.getRow(), cs.iterator());
    }

    @Override
    public Bytes getRow() {
      return cs.getRow();
    }

    @Override
    public String getsRow() {
      return cs.getsRow();
    }

  }

  private class RtxRowScanner implements RowScanner {

    private RowScanner scanner;

    public RtxRowScanner(RowScanner scanner) {
      this.scanner = scanner;
    }

    @Override
    public Iterator<ColumnScanner> iterator() {
      return Iterators.transform(scanner.iterator(), RtxColumnScanner::new);
    }

  }

  private class RtxRowScannerBuilder implements RowScannerBuilder {

    private RowScannerBuilder rsb;

    public RtxRowScannerBuilder(RowScannerBuilder rsb) {
      this.rsb = rsb;
    }

    @Override
    public RowScanner build() {
      return new RtxRowScanner(rsb.build());
    }

  }

  private class RtxScannerBuilder implements ScannerBuilder {

    private ScannerBuilder sb;

    public RtxScannerBuilder(ScannerBuilder sb) {
      this.sb = sb;
    }

    @Override
    public ScannerBuilder over(Span span) {
      sb = sb.over(span);
      return this;
    }

    @Override
    public ScannerBuilder fetch(Column... columns) {
      sb = sb.fetch(columns);
      return this;
    }

    @Override
    public ScannerBuilder fetch(Collection<Column> columns) {
      sb = sb.fetch(columns);
      return this;
    }

    @Override
    public CellScanner build() {
      return new RtxCellSanner(sb.build());
    }

    @Override
    public RowScannerBuilder byRow() {
      return new RtxRowScannerBuilder(sb.byRow());
    }

  }

  /**
   * Logs GETs for Row/Columns returned by iterators. Requests that return no data will not be
   * logged.
   */
  @Override
  public ScannerBuilder scanner() {
    return new RtxScannerBuilder(txb.scanner());
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

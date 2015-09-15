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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.exceptions.AlreadySetException;
import io.fluo.api.iterator.RowIterator;

public class RecordingTransactionBase implements TransactionBase {

  private final TransactionBase txb;
  private TxLog txLog = new TxLog();

  RecordingTransactionBase(TransactionBase txb) {
    this.txb = txb;
  }

  @Override
  public void setWeakNotification(Bytes row, Column col) {
    txb.setWeakNotification(row, col);
  }

  @Override
  public void set(Bytes row, Column col, Bytes value) throws AlreadySetException {
    txLog.addSet(row, col, value);
    txb.set(row, col, value);
  }

  @Override
  public void delete(Bytes row, Column col) {
    txLog.addDelete(row, col);
    txb.delete(row, col);
  }

  @Override
  public Bytes get(Bytes row, Column col) {
    return txb.get(row, col);
  }

  @Override
  public Map<Column, Bytes> get(Bytes row, Set<Column> columns) {
    return txb.get(row, columns);
  }

  @Override
  public Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Set<Column> columns) {
    return txb.get(rows, columns);
  }

  @Override
  public RowIterator get(ScannerConfiguration config) {
    return txb.get(config);
  }

  public TxLog getTxLog() {
    return txLog;
  }

  public static RecordingTransactionBase wrap(TransactionBase txb) {
    return new RecordingTransactionBase(txb);
  }

  @Override
  public long getStartTimestamp() {
    return txb.getStartTimestamp();
  }
}

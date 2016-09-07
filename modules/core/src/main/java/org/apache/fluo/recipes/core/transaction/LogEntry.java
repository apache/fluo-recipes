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

import java.util.Objects;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;

/**
 * Logs an operation (i.e GET, SET, or DELETE) in a Transaction. Multiple LogEntry objects make up a
 * {@link TxLog}.
 *
 * @since 1.0.0
 */
public class LogEntry {

  /**
   * @since 1.0.0
   */
  public enum Operation {
    GET, SET, DELETE
  }

  private Operation op;
  private Bytes row;
  private Column col;
  private Bytes value;

  private LogEntry() {}

  private LogEntry(Operation op, Bytes row, Column col, Bytes value) {
    Objects.requireNonNull(op);
    Objects.requireNonNull(row);
    Objects.requireNonNull(col);
    Objects.requireNonNull(value);
    this.op = op;
    this.row = row;
    this.col = col;
    this.value = value;
  }

  public Operation getOp() {
    return op;
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
      return ((op == other.op) && row.equals(other.row) && col.equals(other.col) && value
          .equals(other.value));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = op.hashCode();
    result = 31 * result + row.hashCode();
    result = 31 * result + col.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "LogEntry{op=" + op + ", row=" + row + ", col=" + col + ", value=" + value + "}";
  }

  public static LogEntry newGet(CharSequence row, Column col, CharSequence value) {
    return newGet(Bytes.of(row), col, Bytes.of(value));
  }

  public static LogEntry newGet(Bytes row, Column col, Bytes value) {
    return new LogEntry(Operation.GET, row, col, value);
  }

  public static LogEntry newSet(CharSequence row, Column col, CharSequence value) {
    return newSet(Bytes.of(row), col, Bytes.of(value));
  }

  public static LogEntry newSet(Bytes row, Column col, Bytes value) {
    return new LogEntry(Operation.SET, row, col, value);
  }

  public static LogEntry newDelete(CharSequence row, Column col) {
    return newDelete(Bytes.of(row), col);
  }

  public static LogEntry newDelete(Bytes row, Column col) {
    return new LogEntry(Operation.DELETE, row, col, Bytes.EMPTY);
  }
}

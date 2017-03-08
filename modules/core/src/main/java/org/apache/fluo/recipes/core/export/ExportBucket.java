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

package org.apache.fluo.recipes.core.export;

import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.recipes.core.types.StringEncoder;
import org.apache.fluo.recipes.core.types.TypeLayer;
import org.apache.fluo.recipes.core.types.TypedTransactionBase;

/**
 * This class encapsulates a buckets serialization code.
 */
// This class intentionally package private.
class ExportBucket {
  private static final String NOTIFICATION_CF = "fluoRecipes";
  private static final String NOTIFICATION_CQ_PREFIX = "eq:";
  private static final Column EXPORT_COL = new Column("e", "v");
  private static final Column NEXT_COL = new Column("e", "next");

  static Column newNotificationColumn(String queueId) {
    return new Column(NOTIFICATION_CF, NOTIFICATION_CQ_PREFIX + queueId);
  }

  private final TypedTransactionBase ttx;
  private final String qid;
  private final Bytes bucketRow;

  static String genBucketId(int bucket, int maxBucket) {
    Preconditions.checkArgument(bucket >= 0);
    Preconditions.checkArgument(maxBucket > 0);

    int bits = 32 - Integer.numberOfLeadingZeros(maxBucket);
    int bucketLen = bits / 4 + (bits % 4 > 0 ? 1 : 0);

    return Strings.padStart(Integer.toHexString(bucket), bucketLen, '0');
  }

  static Bytes generateBucketRow(String qid, int bucket, int numBuckets) {
    return Bytes.of(qid + ":" + genBucketId(bucket, numBuckets));
  }

  ExportBucket(TransactionBase tx, String qid, int bucket, int numBuckets) {
    // TODO encode in a more robust way... but for now fail early
    Preconditions.checkArgument(!qid.contains(":"), "Export QID can not contain :");
    this.ttx = new TypeLayer(new StringEncoder()).wrap(tx);
    this.qid = qid;
    this.bucketRow = generateBucketRow(qid, bucket, numBuckets);
  }

  ExportBucket(TransactionBase tx, Bytes bucketRow) {
    this.ttx = new TypeLayer(new StringEncoder()).wrap(tx);

    int colonLoc = -1;

    for (int i = 0; i < bucketRow.length(); i++) {
      if (bucketRow.byteAt(i) == ':') {
        colonLoc = i;
        break;
      }
    }

    Preconditions.checkArgument(colonLoc != -1 && colonLoc != bucketRow.length(),
        "Invalid bucket row " + bucketRow);
    Preconditions.checkArgument(bucketRow.byteAt(bucketRow.length() - 1) == ':',
        "Invalid bucket row " + bucketRow);

    this.bucketRow = bucketRow.subSequence(0, bucketRow.length() - 1);
    this.qid = bucketRow.subSequence(0, colonLoc).toString();
  }

  private static void encSeq(BytesBuilder bb, long l) {
    bb.append((byte) (l >>> 56));
    bb.append((byte) (l >>> 48));
    bb.append((byte) (l >>> 40));
    bb.append((byte) (l >>> 32));
    bb.append((byte) (l >>> 24));
    bb.append((byte) (l >>> 16));
    bb.append((byte) (l >>> 8));
    bb.append((byte) (l >>> 0));
  }

  private static long decodeSeq(Bytes seq) {
    return (((long) seq.byteAt(0) << 56) + ((long) (seq.byteAt(1) & 255) << 48)
        + ((long) (seq.byteAt(2) & 255) << 40) + ((long) (seq.byteAt(3) & 255) << 32)
        + ((long) (seq.byteAt(4) & 255) << 24) + ((seq.byteAt(5) & 255) << 16)
        + ((seq.byteAt(6) & 255) << 8) + ((seq.byteAt(7) & 255) << 0));
  }

  public void add(long seq, byte[] key, byte[] value) {
    BytesBuilder builder =
        Bytes.builder(bucketRow.length() + 1 + key.length + 8).append(bucketRow).append(':')
            .append(key);
    encSeq(builder, seq);
    ttx.set(builder.toBytes(), EXPORT_COL, Bytes.of(value));
  }

  /**
   * Computes the minimial row for a bucket
   */
  private Bytes getMinimalRow() {
    return Bytes.builder(bucketRow.length() + 1).append(bucketRow).append(':').toBytes();
  }

  public void notifyExportObserver() {
    ttx.mutate().row(getMinimalRow()).col(newNotificationColumn(qid)).weaklyNotify();
  }

  public Iterator<ExportEntry> getExportIterator(Bytes continueRow) {
    Span span;
    if (continueRow != null) {
      Span tmpSpan = Span.prefix(bucketRow);
      Span nextSpan =
          new Span(new RowColumn(continueRow, EXPORT_COL), true, tmpSpan.getEnd(),
              tmpSpan.isEndInclusive());
      span = nextSpan;
    } else {
      span = Span.prefix(bucketRow);
    }

    CellScanner scanner = ttx.scanner().over(span).fetch(EXPORT_COL).build();

    return new ExportIterator(scanner);
  }

  private class ExportIterator implements Iterator<ExportEntry> {

    private Iterator<RowColumnValue> rowIter;
    private Bytes lastRow;

    public ExportIterator(CellScanner scanner) {
      this.rowIter = scanner.iterator();
    }

    @Override
    public boolean hasNext() {
      return rowIter.hasNext();
    }

    @Override
    public ExportEntry next() {
      RowColumnValue rowColVal = rowIter.next();
      Bytes row = rowColVal.getRow();

      Bytes keyBytes = row.subSequence(bucketRow.length() + 1, row.length() - 8);
      Bytes seqBytes = row.subSequence(row.length() - 8, row.length());

      ExportEntry ee = new ExportEntry();

      ee.key = keyBytes.toArray();
      ee.seq = decodeSeq(seqBytes);
      // TODO maybe leave as Bytes?
      ee.value = rowColVal.getValue().toArray();

      lastRow = row;

      return ee;
    }

    @Override
    public void remove() {
      ttx.mutate().row(lastRow).col(EXPORT_COL).delete();
    }
  }

  public Bytes getContinueRow() {
    return ttx.get(getMinimalRow(), NEXT_COL);
  }

  public void setContinueRow(ExportEntry ee) {
    BytesBuilder builder =
        Bytes.builder(bucketRow.length() + 1 + ee.key.length + 8).append(bucketRow).append(':')
            .append(ee.key);
    encSeq(builder, ee.seq);
    Bytes nextRow = builder.toBytes();
    ttx.set(getMinimalRow(), NEXT_COL, nextRow);
  }

  public void clearContinueRow() {
    ttx.delete(getMinimalRow(), NEXT_COL);
  }
}

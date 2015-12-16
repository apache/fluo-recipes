/*
 * Copyright 2014 Fluo authors (see AUTHORS)
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

package io.fluo.recipes.export;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.api.types.TypedTransactionBase;
import io.fluo.recipes.impl.BucketUtil;

/**
 * This class encapsulates a buckets serialization code.
 */
class ExportBucket {
  private static final String NOTIFICATION_CF = "fluoRecipes";
  private static final String NOTIFICATION_CQ_PREFIX = "eq:";
  private static final Column EXPORT_COL = new Column("exp", "val");

  static Column newNotificationColumn(String queueId) {
    return new Column(NOTIFICATION_CF, NOTIFICATION_CQ_PREFIX + queueId);
  }

  private final TypedTransactionBase ttx;
  private final String qid;
  private final Bytes bucketRow;

  static Bytes generateBucketRow(String qid, int bucket, int numBuckets) {
    return Bytes.of(qid + ":" + BucketUtil.genBucketId(bucket, numBuckets));
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

    Preconditions.checkArgument(colonLoc != -1, "Invalid bucket row " + bucketRow);

    this.bucketRow = bucketRow;
    this.qid = bucketRow.subSequence(0, colonLoc).toString();
  }

  // this method is 10x faster than String.format("%016x",seq)
  private static byte[] encSeq(long l) {
    byte[] encodedSeq =
        new byte[] {'0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'};
    String seqString = Long.toString(l, 16);

    for (int i = 0, j = 16 - seqString.length(); i < seqString.length(); i++) {
      encodedSeq[j++] = (byte) seqString.charAt(i);
    }
    return encodedSeq;
  }

  public void add(long seq, byte[] key, byte[] value) {
    Bytes row =
        Bytes.newBuilder(bucketRow.length() + 1 + key.length + 16).append(bucketRow).append(":")
            .append(key).append(encSeq(seq)).toBytes();
    ttx.set(row, EXPORT_COL, Bytes.of(value));
  }

  public void notifyExportObserver() {
    ttx.mutate().row(bucketRow).col(newNotificationColumn(qid)).weaklyNotify();
  }

  public Iterator<ExportEntry> getExportIterator() {
    ScannerConfiguration sc = new ScannerConfiguration();
    sc.setSpan(Span.prefix(bucketRow));
    sc.fetchColumn(EXPORT_COL.getFamily(), EXPORT_COL.getQualifier());
    RowIterator iter = ttx.get(sc);

    if (iter.hasNext()) {
      return new ExportIterator(iter);
    } else {
      return Collections.<ExportEntry>emptySet().iterator();
    }
  }

  private class ExportIterator implements Iterator<ExportEntry> {

    private RowIterator rowIter;
    private Bytes lastRow;

    public ExportIterator(RowIterator rowIter) {
      this.rowIter = rowIter;
    }

    @Override
    public boolean hasNext() {
      return rowIter.hasNext();
    }

    @Override
    public ExportEntry next() {
      Entry<Bytes, ColumnIterator> rowCol = rowIter.next();
      Bytes row = rowCol.getKey();
      Bytes keyBytes = row.subSequence(bucketRow.length() + 1, row.length() - 16);
      Bytes seqBytes = row.subSequence(row.length() - 16, row.length());

      ExportEntry ee = new ExportEntry();

      ee.key = keyBytes.toArray();
      ee.seq = Long.parseLong(seqBytes.toString(), 16);
      // TODO maybe leave as Bytes?
      ee.value = rowCol.getValue().next().getValue().toArray();

      lastRow = row;

      return ee;
    }

    @Override
    public void remove() {
      ttx.mutate().row(lastRow).col(EXPORT_COL).delete();
    }
  }
}

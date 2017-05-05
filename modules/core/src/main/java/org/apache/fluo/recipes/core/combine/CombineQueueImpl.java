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

package org.apache.fluo.recipes.core.combine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.observer.ObserverProvider.Registry;
import org.apache.fluo.recipes.core.combine.ChangeObserver.Change;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;

// intentionally package private
class CombineQueueImpl<K, V> implements CombineQueue<K, V> {
  static final Column DATA_COLUMN = new Column("data", "current");
  static final Column UPDATE_COL = new Column("u", "v");
  static final Column NEXT_COL = new Column("u", "next");

  private Bytes updatePrefix;
  private Bytes dataPrefix;
  private Column notifyColumn;

  private final String cqId;
  private final Class<K> keyType;
  private final Class<V> valType;
  private final int numBuckets;
  private final long bufferSize;
  private SimpleSerializer serializer;

  @SuppressWarnings("unchecked")
  CombineQueueImpl(String cqId, SimpleConfiguration appConfig) throws Exception {
    this.cqId = cqId;
    this.updatePrefix = Bytes.of(cqId + ":u:");
    this.dataPrefix = Bytes.of(cqId + ":d:");
    this.notifyColumn = new Column("fluoRecipes", "cfm:" + cqId);
    this.keyType =
        (Class<K>) getClass().getClassLoader()
            .loadClass(CqConfigurator.getKeyType(cqId, appConfig));
    this.valType =
        (Class<V>) getClass().getClassLoader().loadClass(
            CqConfigurator.getValueType(cqId, appConfig));
    this.numBuckets = CqConfigurator.getNumBucket(cqId, appConfig);
    this.bufferSize = CqConfigurator.getBufferSize(cqId, appConfig);
    this.serializer = SimpleSerializer.getInstance(appConfig);
  }

  private static byte[] encSeq(long l) {
    byte[] ret = new byte[8];
    ret[0] = (byte) (l >>> 56);
    ret[1] = (byte) (l >>> 48);
    ret[2] = (byte) (l >>> 40);
    ret[3] = (byte) (l >>> 32);
    ret[4] = (byte) (l >>> 24);
    ret[5] = (byte) (l >>> 16);
    ret[6] = (byte) (l >>> 8);
    ret[7] = (byte) (l >>> 0);
    return ret;
  }

  static String genBucketId(int bucket, int maxBucket) {
    Preconditions.checkArgument(bucket >= 0);
    Preconditions.checkArgument(maxBucket > 0);

    int bits = 32 - Integer.numberOfLeadingZeros(maxBucket);
    int bucketLen = bits / 4 + (bits % 4 > 0 ? 1 : 0);

    return Strings.padStart(Integer.toHexString(bucket), bucketLen, '0');
  }

  @Override
  public void addAll(TransactionBase tx, Map<K, V> updates) {
    Preconditions.checkState(numBuckets > 0, "Not initialized");

    Set<String> buckets = new HashSet<>();

    BytesBuilder rowBuilder = Bytes.builder();
    rowBuilder.append(updatePrefix);
    int prefixLength = rowBuilder.getLength();

    byte[] startTs = encSeq(tx.getStartTimestamp());

    for (Entry<K, V> entry : updates.entrySet()) {
      byte[] k = serializer.serialize(entry.getKey());
      int hash = Hashing.murmur3_32().hashBytes(k).asInt();
      String bucketId = genBucketId(Math.abs(hash % numBuckets), numBuckets);

      // reset to the common row prefix
      rowBuilder.setLength(prefixLength);

      Bytes row = rowBuilder.append(bucketId).append(':').append(k).append(startTs).toBytes();
      Bytes val = Bytes.of(serializer.serialize(entry.getValue()));

      // TODO set if not exists would be comforting here.... but
      // collisions on bucketId+key+uuid should never occur
      tx.set(row, UPDATE_COL, val);

      buckets.add(bucketId);
    }

    for (String bucketId : buckets) {
      rowBuilder.setLength(prefixLength);
      rowBuilder.append(bucketId).append(':');

      Bytes row = rowBuilder.toBytes();

      tx.setWeakNotification(row, notifyColumn);
    }
  }

  private Map<Bytes, Map<Column, Bytes>> getCurrentValues(TransactionBase tx, BytesBuilder prefix,
      Set<Bytes> keySet) {

    Set<Bytes> rows = new HashSet<>();

    int prefixLen = prefix.getLength();
    for (Bytes key : keySet) {
      prefix.setLength(prefixLen);
      rows.add(prefix.append(key).toBytes());
    }

    try {
      return tx.get(rows, Collections.singleton(DATA_COLUMN));
    } catch (IllegalArgumentException e) {
      System.out.println(rows.size());
      throw e;
    }
  }

  private V deserVal(Bytes val) {
    return serializer.deserialize(val.toArray(), valType);
  }

  private Bytes getKeyFromUpdateRow(Bytes prefix, Bytes row) {
    return row.subSequence(prefix.length(), row.length() - 8);
  }

  void process(TransactionBase tx, Bytes ntfyRow, Column col, Combiner<K, V> combiner,
      ChangeObserver<K, V> changeObserver) throws Exception {

    Preconditions.checkState(ntfyRow.startsWith(updatePrefix));

    Bytes nextKey = tx.get(ntfyRow, NEXT_COL);

    Span span;

    if (nextKey != null) {
      Bytes startRow =
          Bytes.builder(ntfyRow.length() + nextKey.length()).append(ntfyRow).append(nextKey)
              .toBytes();
      Span tmpSpan = Span.prefix(ntfyRow);
      Span nextSpan =
          new Span(new RowColumn(startRow, UPDATE_COL), false, tmpSpan.getEnd(),
              tmpSpan.isEndInclusive());
      span = nextSpan;
    } else {
      span = Span.prefix(ntfyRow);
    }

    Iterator<RowColumnValue> iter = tx.scanner().over(span).fetch(UPDATE_COL).build().iterator();

    Map<Bytes, List<Bytes>> updates = new HashMap<>();

    long approxMemUsed = 0;

    Bytes partiallyReadKey = null;
    boolean setNextKey = false;

    if (iter.hasNext()) {
      Bytes lastKey = null;
      while (iter.hasNext() && approxMemUsed < bufferSize) {
        RowColumnValue rcv = iter.next();
        Bytes curRow = rcv.getRow();

        tx.delete(curRow, UPDATE_COL);

        Bytes serializedKey = getKeyFromUpdateRow(ntfyRow, curRow);
        lastKey = serializedKey;

        List<Bytes> updateList = updates.get(serializedKey);
        if (updateList == null) {
          updateList = new ArrayList<>();
          updates.put(serializedKey, updateList);
        }

        Bytes val = rcv.getValue();
        updateList.add(val);

        approxMemUsed += curRow.length();
        approxMemUsed += val.length();
      }

      if (iter.hasNext()) {
        RowColumnValue rcv = iter.next();
        Bytes curRow = rcv.getRow();

        // check if more updates for last key
        if (getKeyFromUpdateRow(ntfyRow, curRow).equals(lastKey)) {
          // there are still more updates for this key
          partiallyReadKey = lastKey;

          // start next time at the current key
          tx.set(ntfyRow, NEXT_COL, partiallyReadKey);
        } else {
          // start next time at the next possible key
          Bytes nextPossible =
              Bytes.builder(lastKey.length() + 1).append(lastKey).append(0).toBytes();
          tx.set(ntfyRow, NEXT_COL, nextPossible);
        }

        setNextKey = true;
      } else if (nextKey != null) {
        // clear nextKey
        tx.delete(ntfyRow, NEXT_COL);
      }
    } else if (nextKey != null) {
      tx.delete(ntfyRow, NEXT_COL);
    }

    if (nextKey != null || setNextKey) {
      // If not all data was read need to run again in the future. If scanning was started in the
      // middle of the bucket, its possible there is new data before nextKey that still needs to be
      // processed. If scanning stopped before reading the entire bucket there may be data after the
      // stop point.
      tx.setWeakNotification(ntfyRow, col);
    }



    BytesBuilder rowBuilder = Bytes.builder();
    rowBuilder.append(dataPrefix);
    rowBuilder.append(ntfyRow.subSequence(updatePrefix.length(), ntfyRow.length()));
    int rowPrefixLen = rowBuilder.getLength();

    Set<Bytes> keysToFetch = updates.keySet();
    if (partiallyReadKey != null) {
      final Bytes prk = partiallyReadKey;
      keysToFetch = Sets.filter(keysToFetch, b -> !b.equals(prk));
    }
    Map<Bytes, Map<Column, Bytes>> currentVals = getCurrentValues(tx, rowBuilder, keysToFetch);

    ArrayList<Change<K, V>> updatesToReport = new ArrayList<>(updates.size());

    for (Entry<Bytes, List<Bytes>> entry : updates.entrySet()) {
      rowBuilder.setLength(rowPrefixLen);
      Bytes currentValueRow = rowBuilder.append(entry.getKey()).toBytes();
      Bytes currVal =
          currentVals.getOrDefault(currentValueRow, Collections.emptyMap()).get(DATA_COLUMN);

      K kd = serializer.deserialize(entry.getKey().toArray(), keyType);

      if (partiallyReadKey != null && partiallyReadKey.equals(entry.getKey())) {
        // not all updates were read for this key, so requeue the combined updates as an update
        Optional<V> nv = combiner.combine(new InputImpl<>(kd, this::deserVal, entry.getValue()));
        if (nv.isPresent()) {
          addAll(tx, Collections.singletonMap(kd, nv.get()));
        }
      } else {
        Optional<V> nv =
            combiner.combine(new InputImpl<>(kd, this::deserVal, currVal, entry.getValue()));
        Bytes newVal = nv.isPresent() ? Bytes.of(serializer.serialize(nv.get())) : null;
        if (newVal != null ^ currVal != null || (currVal != null && !currVal.equals(newVal))) {
          if (newVal == null) {
            tx.delete(currentValueRow, DATA_COLUMN);
          } else {
            tx.set(currentValueRow, DATA_COLUMN, newVal);
          }

          Optional<V> cvd = Optional.ofNullable(currVal).map(this::deserVal);
          updatesToReport.add(new ChangeImpl<>(kd, cvd, nv));
        }
      }
    }

    // TODO could clear these as converted to objects to avoid double memory usage
    updates.clear();
    currentVals.clear();

    if (updatesToReport.size() > 0) {
      changeObserver.process(tx, updatesToReport);
    }
  }

  @Override
  public void registerObserver(Registry obsRegistry, Combiner<K, V> combiner,
      ChangeObserver<K, V> changeObserver) {
    obsRegistry.forColumn(notifyColumn, NotificationType.WEAK).withId("combineq-" + cqId)
        .useObserver((tx, row, col) -> process(tx, row, col, combiner, changeObserver));
  }
}

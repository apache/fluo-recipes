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

package io.fluo.recipes.map;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.configuration.Configuration;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.hash.Hashing;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.recipes.serialization.SimpleSerializer;

public abstract class CollisionFreeMap<K, V, U> {

  private String mapId;

  private Class<K> keyType;
  private Class<V> valType;
  private Class<U> updateType;
  private SimpleSerializer serializer;

  private V defaultValue;

  static final String UPDATE_CF_PREFIX = "upd:";
  private int numBuckets = -1;

  protected CollisionFreeMap(String mapId, Class<K> keyType, Class<V> valType, Class<U> updateType,
      SimpleSerializer serializer, V defaultValue) {
    this.mapId = mapId;
    this.keyType = keyType;
    this.valType = valType;
    this.updateType = updateType;
    this.defaultValue = defaultValue;
    this.serializer = serializer;
  }

  void process(TransactionBase tx, Bytes ntfyRow, Column col) throws Exception {
    ScannerConfiguration sc = new ScannerConfiguration();
    sc.setSpan(Span.prefix(ntfyRow, new Column(UPDATE_CF_PREFIX)));
    RowIterator iter = tx.get(sc);

    Map<Bytes, List<Bytes>> updates = new HashMap<>();

    if (iter.hasNext()) {
      ColumnIterator cols = iter.next().getValue();
      while (cols.hasNext()) {
        // TODO processing too many at once increases chance of
        // collisions... may want to process a max # and notify self

        // TODO if there is too much in memory, then stop and processes
        // whats there and notify self.. need to ensure entire keys are
        // processed to avoid processing newer updates and not older
        // ones... if single key does not fit in mem, could make
        // multiple passes
        Entry<Column, Bytes> colVal = cols.next();
        tx.delete(ntfyRow, colVal.getKey());

        Bytes fam = colVal.getKey().getFamily();
        Bytes serializedKey = fam.subSequence(UPDATE_CF_PREFIX.length(), fam.length());

        List<Bytes> updateList = updates.get(serializedKey);
        if (updateList == null) {
          updateList = new ArrayList<>();
          updates.put(serializedKey, updateList);
        }

        updateList.add(colVal.getValue());
      }
    }

    Map<Bytes, Map<Column, Bytes>> currentVals = getCurrentValues(tx, ntfyRow, updates.keySet());

    Prefix prefix = new Prefix(ntfyRow);

    for (Entry<Bytes, List<Bytes>> entry : updates.entrySet()) {
      Bytes currentValueRow = prefix.append(entry.getKey());
      Bytes currentVal = null;
      Map<Column, Bytes> cols = currentVals.get(currentValueRow);
      if (cols != null) {
        currentVal = cols.get(DATA_COLUMN);
      }
      V cvd = defaultValue;
      if (currentVal == null) {
        cvd = defaultValue;
      } else {
        cvd = serializer.deserialize(currentVal.toArray(), valType);
      }

      Iterator<U> ui = Iterators.transform(entry.getValue().iterator(), new Function<Bytes, U>() {
        @Override
        public U apply(Bytes b) {
          return serializer.deserialize(b.toArray(), updateType);
        }
      });

      K kd = serializer.deserialize(entry.getKey().toArray(), keyType);
      V nv = combine(kd, cvd, ui);

      Bytes newValBytes = Bytes.of(serializer.serialize(nv));
      if (currentVal == null || !currentVal.equals(newValBytes)) {
        // TODO passing null may be confusing.. could call another method
        // instead like setting value
        updatingValue(tx, kd, currentVal == null ? null : cvd, nv);
        tx.set(currentValueRow, DATA_COLUMN, newValBytes);
      }
    }
  }

  private static final Column DATA_COLUMN = new Column("data", "current");

  private static class Prefix {
    byte[] tmp;
    int prefixLen;

    Prefix(Bytes p) {
      tmp = new byte[p.length() + 32];
      System.arraycopy(p.toArray(), 0, tmp, 0, p.length());
      tmp[p.length()] = ':';
      prefixLen = p.length() + 1;
    }

    Bytes append(Bytes s) {
      if (tmp.length < prefixLen + s.length()) {
        byte[] newTmp = new byte[prefixLen + s.length() * 2];
        System.arraycopy(tmp, 0, newTmp, 0, prefixLen);
        tmp = newTmp;
      }

      System.arraycopy(s.toArray(), 0, tmp, prefixLen, s.length());
      return Bytes.of(tmp, 0, prefixLen + s.length());
    }
  }

  private Map<Bytes, Map<Column, Bytes>> getCurrentValues(TransactionBase tx, Bytes ntfyRow,
      Set<Bytes> keySet) {
    if (keySet.size() == 0) {
      // TODO fluo should be able to handle this... it gets when passed an empty set of rows
      return Collections.emptyMap();
    }

    Prefix prefix = new Prefix(ntfyRow);

    Set<Bytes> rows = new HashSet<>();

    for (Bytes key : keySet) {
      rows.add(prefix.append(key));
    }

    try {
      return tx.get(rows, Collections.singleton(DATA_COLUMN));
    } catch (IllegalArgumentException e) {
      System.out.println(rows.size());
      throw e;
    }
  }

  // TODO want to support deleting data
  protected abstract V combine(K key, V currentValue, Iterator<U> updates);

  protected void updatingValue(TransactionBase tx, K key, V oldValue, V currentValue) {}

  public V get(TransactionBase tx, K key) {
    // TODO implement (should combine current value and updates)
    throw new UnsupportedOperationException();
  }

  String getId() {
    return mapId;
  }

  public void update(TransactionBase tx, Map<K, U> updates) {
    Preconditions.checkState(numBuckets > 0, "Not initialized");

    UUID uuid = UUID.randomUUID();
    Bytes cq = createCq(uuid);

    Set<Integer> buckets = new HashSet<>();

    for (Entry<K, U> entry : updates.entrySet()) {
      byte[] k = serializer.serialize(entry.getKey());
      int hash = Hashing.murmur3_32().hashBytes(k).asInt();
      int bucketId = Math.abs(hash % numBuckets);

      Bytes row = createRow(mapId, bucketId);
      Bytes cf = createCf(k);
      Bytes val = Bytes.of(serializer.serialize(entry.getValue()));

      // TODO set if not exists would be comforting here.... but
      // collisions on bucketId+key+uuid should never occur
      tx.set(row, new Column(cf, cq), val);

      buckets.add(bucketId);
    }

    for (Integer bucketId : buckets) {
      Bytes row = createRow(mapId, bucketId);
      // TODO is ok to call set weak notification multiple times for the
      // same row col???
      tx.setWeakNotification(row, new Column("fluoRecipes", "hc:" + mapId));
    }
  }

  public static class Options {
    private int numBuckets;

    public Options(int numBuckets) {
      Preconditions.checkArgument(numBuckets > 0);
      this.numBuckets = numBuckets;
    }

    public int getNumBuckets() {
      return numBuckets;
    }
  }

  public ObserverConfiguration getObserverConfiguration() {
    return new ObserverConfiguration(CollisionFreeMapObserver.class.getName())
        .setParameters(ImmutableMap.of("cfmClass", this.getClass().getName()));
  }

  public void setAppConfiguration(Configuration appConfig, Options cfmo) {
    appConfig.setProperty("recipes.cfm." + getId() + ".buckets", cfmo.getNumBuckets() + "");
  }

  public void init(Configuration appConfig) {
    this.numBuckets = appConfig.getInt("recipes.cfm." + getId() + ".buckets");
  }

  private Bytes createRow(String mapId, int bucketId) {
    return Bytes.of(mapId + ":" + bucketId);
  }

  private Bytes createCf(byte[] k) {
    byte[] family = new byte[UPDATE_CF_PREFIX.length() + k.length];
    byte[] prefix = UPDATE_CF_PREFIX.getBytes();
    System.arraycopy(prefix, 0, family, 0, prefix.length);
    System.arraycopy(k, 0, family, prefix.length, k.length);
    return Bytes.of(family);
  }

  private Bytes createCq(UUID uuid) {
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    bb.rewind();
    return Bytes.of(bb);
  }
}

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

import java.io.Serializable;
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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.hash.Hashing;
import io.fluo.api.client.SnapshotBase;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumnValue;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.recipes.common.Pirtos;
import io.fluo.recipes.common.RowRange;
import io.fluo.recipes.common.TransientRegistry;
import io.fluo.recipes.serialization.SimpleSerializer;
import org.apache.commons.configuration.Configuration;

public class CollisionFreeMap<K, V, U> {

  private String mapId;

  private Class<K> keyType;
  private Class<V> valType;
  private Class<U> updateType;
  private SimpleSerializer serializer;
  private Combiner<K, V, U> combiner;
  UpdateObserver<K, V> updateObserver;

  static final Bytes UPDATE_CF_PREFIX = Bytes.of("upd:");
  private int numBuckets = -1;


  @SuppressWarnings("unchecked")
  CollisionFreeMap(Options opts, SimpleSerializer serializer) throws Exception {

    this.mapId = opts.mapId;
    // TODO defer loading classes
    // TODO centralize class loading
    // TODO try to check type params
    this.numBuckets = opts.numBuckets;
    this.keyType = (Class<K>) getClass().getClassLoader().loadClass(opts.keyType);
    this.valType = (Class<V>) getClass().getClassLoader().loadClass(opts.valueType);
    this.updateType = (Class<U>) getClass().getClassLoader().loadClass(opts.updateType);
    this.combiner =
        (Combiner<K, V, U>) getClass().getClassLoader().loadClass(opts.combinerType).newInstance();
    this.serializer = serializer;
    if (opts.updateObserverType != null) {
      this.updateObserver =
          getClass().getClassLoader().loadClass(opts.updateObserverType)
              .asSubclass(UpdateObserver.class).newInstance();
    } else {
      this.updateObserver = new NullUpdateObserver<>();
    }
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
        // when stopping could set a continue key

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

    byte[] dataPrefix = ntfyRow.toArray();
    // TODO this is awful... no sanity check... hard to read
    dataPrefix[Bytes.of(mapId).length() + 1] = 'd';

    Prefix prefix = new Prefix(dataPrefix);

    Map<Bytes, Map<Column, Bytes>> currentVals = getCurrentValues(tx, prefix, updates.keySet());

    ArrayList<Update<K, V>> updatesToReport = new ArrayList<>(updates.size());

    for (Entry<Bytes, List<Bytes>> entry : updates.entrySet()) {
      Bytes currentValueRow = prefix.append(entry.getKey());
      Bytes currVal = null;
      Map<Column, Bytes> cols = currentVals.get(currentValueRow);
      if (cols != null) {
        currVal = cols.get(DATA_COLUMN);
      }
      Optional<V> cvd;
      if (currVal == null) {
        cvd = Optional.absent();
      } else {
        cvd = Optional.of(serializer.deserialize(currVal.toArray(), valType));
      }

      Iterator<U> ui = Iterators.transform(entry.getValue().iterator(), new Function<Bytes, U>() {
        @Override
        public U apply(Bytes b) {
          return serializer.deserialize(b.toArray(), updateType);
        }
      });

      K kd = serializer.deserialize(entry.getKey().toArray(), keyType);
      Optional<V> nv = combiner.combine(kd, cvd, ui);

      Bytes newVal = nv.isPresent() ? Bytes.of(serializer.serialize(nv.get())) : null;
      if (newVal != null ^ currVal != null || (currVal != null && !currVal.equals(newVal))) {
        if (newVal == null) {
          tx.delete(currentValueRow, DATA_COLUMN);
        } else {
          tx.set(currentValueRow, DATA_COLUMN, newVal);
        }
        V cv = null;
        if (currVal != null) {
          // deserialize again in case combiner mutated Object
          cv = serializer.deserialize(currVal.toArray(), valType);
        }
        updatesToReport.add(new Update<K, V>(kd, cv, nv.orNull()));
      }
    }

    // TODO could clear these as converted to objects to avoid double memory usage
    updates.clear();
    currentVals.clear();

    updateObserver.updatingValues(tx, updatesToReport.iterator());
  }

  private static final Column DATA_COLUMN = new Column("data", "current");

  private static class Prefix {
    byte[] tmp;
    int prefixLen;

    Prefix(byte[] p) {
      tmp = new byte[p.length + 32];
      System.arraycopy(p, 0, tmp, 0, p.length);
      tmp[p.length] = ':';
      prefixLen = p.length + 1;
    }

    Prefix(Bytes p) {
      tmp = new byte[p.length() + 32];
      System.arraycopy(p.toArray(), 0, tmp, 0, p.length());
      tmp[p.length()] = ':';
      prefixLen = p.length() + 1;
    }

    Bytes append(Bytes s) {
      return append(s.toArray());
    }

    public Bytes append(byte[] s) {
      if (tmp.length < prefixLen + s.length) {
        byte[] newTmp = new byte[prefixLen + s.length * 2];
        System.arraycopy(tmp, 0, newTmp, 0, prefixLen);
        tmp = newTmp;
      }

      System.arraycopy(s, 0, tmp, prefixLen, s.length);
      return Bytes.of(tmp, 0, prefixLen + s.length);
    }
  }

  private Map<Bytes, Map<Column, Bytes>> getCurrentValues(TransactionBase tx, Prefix prefix,
      Set<Bytes> keySet) {
    if (keySet.size() == 0) {
      // TODO fluo should be able to handle this... it gets when passed an empty set of rows
      return Collections.emptyMap();
    }

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

  public V get(SnapshotBase tx, K key) {

    byte[] k = serializer.serialize(key);

    int hash = Hashing.murmur3_32().hashBytes(k).asInt();
    int bucketId = Math.abs(hash % numBuckets);

    Bytes updatesRow = createUpdateRow(mapId, bucketId);

    Prefix cfPrefix = new Prefix(UPDATE_CF_PREFIX.subSequence(0, UPDATE_CF_PREFIX.length() - 1));

    ScannerConfiguration sc = new ScannerConfiguration();
    sc.setSpan(Span.exact(updatesRow, new Column(cfPrefix.append(k))));

    List<Bytes> updateList = new ArrayList<>();
    RowIterator iter = tx.get(sc);

    if (iter.hasNext()) {
      ColumnIterator cols = iter.next().getValue();
      while (cols.hasNext()) {
        Entry<Column, Bytes> colVal = cols.next();
        updateList.add(colVal.getValue());
      }
    }

    Bytes dataRow = new Prefix(createDataRowPrefix(mapId, bucketId)).append(k);

    Bytes cv = tx.get(dataRow, DATA_COLUMN);

    if (updateList.size() == 0) {
      if (cv == null) {
        return null;
      } else {
        return serializer.deserialize(cv.toArray(), valType);
      }
    }

    Optional<V> cvd;
    if (cv == null) {
      cvd = Optional.absent();
    } else {
      cvd = Optional.of(serializer.deserialize(cv.toArray(), valType));
    }

    Iterator<U> ui = Iterators.transform(updateList.iterator(), new Function<Bytes, U>() {
      @Override
      public U apply(Bytes b) {
        return serializer.deserialize(b.toArray(), updateType);
      }
    });

    return combiner.combine(key, cvd, ui).orNull();
  }

  String getId() {
    return mapId;
  }

  public void update(TransactionBase tx, Map<K, U> updates) {
    Preconditions.checkState(numBuckets > 0, "Not initialized");

    UUID uuid = UUID.randomUUID();
    Prefix cfPrefix = new Prefix(UPDATE_CF_PREFIX.subSequence(0, UPDATE_CF_PREFIX.length() - 1));
    Bytes cq = createCq(uuid);

    Set<Integer> buckets = new HashSet<>();

    for (Entry<K, U> entry : updates.entrySet()) {
      byte[] k = serializer.serialize(entry.getKey());
      int hash = Hashing.murmur3_32().hashBytes(k).asInt();
      int bucketId = Math.abs(hash % numBuckets);

      Bytes row = createUpdateRow(mapId, bucketId);
      Bytes cf = cfPrefix.append(k);
      Bytes val = Bytes.of(serializer.serialize(entry.getValue()));

      // TODO set if not exists would be comforting here.... but
      // collisions on bucketId+key+uuid should never occur
      tx.set(row, new Column(cf, cq), val);

      buckets.add(bucketId);
    }

    for (Integer bucketId : buckets) {
      Bytes row = createUpdateRow(mapId, bucketId);
      // TODO is ok to call set weak notification multiple times for the
      // same row col???
      tx.setWeakNotification(row, new Column("fluoRecipes", "cfm:" + mapId));
    }
  }


  public static <K2, V2, U2> CollisionFreeMap<K2, V2, U2> getInstance(String mapId,
      Configuration appConfig) {
    Options opts = new Options(mapId, appConfig);
    try {
      return new CollisionFreeMap<K2, V2, U2>(opts, SimpleSerializer.getInstance(appConfig));
    } catch (Exception e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  /**
   * A @link {@link CollisionFreeMap} stores data in its own data format in the Fluo table. When
   * initializing a Fluo table with something like Map Reduce or Spark, data will need to be written
   * in this format. Thats the purpose of this method, it provide a simple class that can do this
   * conversion.
   *
   */
  public static <K2, V2> Initializer<K2, V2> getInitializer(String mapId, int numBuckets,
      SimpleSerializer serializer) {
    return new Initializer<K2, V2>(mapId, numBuckets, serializer);
  }


  /**
   * @see CollisionFreeMap#getInitializer(String, int, SimpleSerializer)
   */
  public static class Initializer<K2, V2> implements Serializable {

    private static final long serialVersionUID = 1L;

    private String mapId;

    private SimpleSerializer serializer;

    private int numBuckets = -1;

    private Initializer(String mapId, int numBuckets, SimpleSerializer serializer) {
      this.mapId = mapId;
      this.numBuckets = numBuckets;
      this.serializer = serializer;
    }

    public RowColumnValue convert(K2 key, V2 val) {
      byte[] k = serializer.serialize(key);
      int hash = Hashing.murmur3_32().hashBytes(k).asInt();
      int bucketId = Math.abs(hash % numBuckets);

      Bytes row = new Prefix(createDataRowPrefix(mapId, bucketId)).append(k);
      byte[] v = serializer.serialize(val);

      return new RowColumnValue(row, DATA_COLUMN, Bytes.of(v));
    }
  }

  public static class Options {
    int numBuckets;

    String keyType;
    String valueType;
    String updateType;
    String combinerType;
    String updateObserverType;
    String mapId;

    private static final String PREFIX = "recipes.cfm.";

    Options(String mapId, Configuration appConfig) {
      this.mapId = mapId;

      this.numBuckets = appConfig.getInt(PREFIX + mapId + ".buckets");
      this.combinerType = appConfig.getString(PREFIX + mapId + ".combiner");
      this.keyType = appConfig.getString(PREFIX + mapId + ".key");
      this.valueType = appConfig.getString(PREFIX + mapId + ".val");
      this.updateType = appConfig.getString(PREFIX + mapId + ".update");
      this.updateObserverType = appConfig.getString(PREFIX + mapId + ".updateObserver", null);
    }

    public Options(String mapId, String combinerType, String keyType, String valueType,
        String updateType, int buckets) {
      Preconditions.checkArgument(buckets > 0);

      this.mapId = mapId;
      this.numBuckets = buckets;
      this.combinerType = combinerType;
      this.updateObserverType = null;
      this.keyType = keyType;
      this.valueType = valueType;
      this.updateType = updateType;
    }

    public Options(String mapId, String combinerType, String updateObserverType, String keyType,
        String valueType, String updateType, int buckets) {
      Preconditions.checkArgument(buckets > 0);

      this.mapId = mapId;
      this.numBuckets = buckets;
      this.combinerType = combinerType;
      this.updateObserverType = updateObserverType;
      this.keyType = keyType;
      this.valueType = valueType;
      this.updateType = updateType;
    }

    public <K, V, U> Options(String mapId, Class<? extends Combiner<K, V, U>> combiner,
        Class<K> keyType, Class<V> valueType, Class<U> updateType, int buckets) {
      this(mapId, combiner.getName(), keyType.getName(), valueType.getName(), updateType.getName(),
          buckets);
    }

    public <K, V, U> Options(String mapId, Class<? extends Combiner<K, V, U>> combiner,
        Class<? extends UpdateObserver<K, V>> updateObserver, Class<K> keyType, Class<V> valueType,
        Class<U> updateType, int buckets) {
      this(mapId, combiner.getName(), updateObserver.getName(), keyType.getName(), valueType
          .getName(), updateType.getName(), buckets);
    }

    void save(Configuration appConfig) {
      appConfig.setProperty(PREFIX + mapId + ".buckets", numBuckets + "");
      appConfig.setProperty(PREFIX + mapId + ".combiner", combinerType + "");
      appConfig.setProperty(PREFIX + mapId + ".key", keyType);
      appConfig.setProperty(PREFIX + mapId + ".val", valueType);
      appConfig.setProperty(PREFIX + mapId + ".update", updateType);
      if (updateObserverType != null) {
        appConfig.setProperty(PREFIX + mapId + ".updateObserver", updateObserverType + "");
      }
    }
  }

  public static Pirtos configure(FluoConfiguration fluoConfig, Options opts) {
    opts.save(fluoConfig.getAppConfiguration());
    fluoConfig.addObserver(new ObserverConfiguration(CollisionFreeMapObserver.class.getName())
        .setParameters(ImmutableMap.of("mapId", opts.mapId)));

    List<Bytes> splits = new ArrayList<>();

    Bytes updateRangeStart = Bytes.of(opts.mapId + ":u");
    Bytes updateRangeEnd = Bytes.of(opts.mapId + ":u;");

    splits.add(Bytes.of(opts.mapId + ":"));
    splits.add(updateRangeStart);
    splits.add(updateRangeEnd);

    new TransientRegistry(fluoConfig.getAppConfiguration()).addTransientRange("cfm." + opts.mapId,
        new RowRange(updateRangeStart, updateRangeEnd));

    Pirtos pirtos = new Pirtos();
    pirtos.setSplits(splits);

    return pirtos;
  }

  private Bytes createUpdateRow(String mapId, int bucketId) {
    return Bytes.of(mapId + ":u:" + bucketId);
  }

  private static Bytes createDataRowPrefix(String mapId, int bucketId) {
    return Bytes.of(mapId + ":d:" + bucketId);
  }

  private Bytes createCq(UUID uuid) {
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    bb.rewind();
    return Bytes.of(bb);
  }
}

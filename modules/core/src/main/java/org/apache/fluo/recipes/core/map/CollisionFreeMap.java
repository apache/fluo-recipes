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

package org.apache.fluo.recipes.core.map;

import java.io.Serializable;
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
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.recipes.core.common.TableOptimizations;
import org.apache.fluo.recipes.core.common.TableOptimizations.TableOptimizationsFactory;
import org.apache.fluo.recipes.core.common.RowRange;
import org.apache.fluo.recipes.core.common.TransientRegistry;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;

/**
 * See the project level documentation for information about this recipe.
 *
 * @since 1.0.0
 */
public class CollisionFreeMap<K, V> {

  private static final String UPDATE_RANGE_END = ":u:~";

  private static final String DATA_RANGE_END = ":d:~";

  private String mapId;

  private Class<K> keyType;
  private Class<V> valType;
  private SimpleSerializer serializer;
  private Combiner<K, V> combiner;
  UpdateObserver<K, V> updateObserver;
  private long bufferSize;

  static final Column UPDATE_COL = new Column("u", "v");
  static final Column NEXT_COL = new Column("u", "next");

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
    this.combiner =
        (Combiner<K, V>) getClass().getClassLoader().loadClass(opts.combinerType).newInstance();
    this.serializer = serializer;
    if (opts.updateObserverType != null) {
      this.updateObserver =
          getClass().getClassLoader().loadClass(opts.updateObserverType)
              .asSubclass(UpdateObserver.class).newInstance();
    } else {
      this.updateObserver = new NullUpdateObserver<>();
    }
    this.bufferSize = opts.getBufferSize();
  }

  private V deserVal(Bytes val) {
    return serializer.deserialize(val.toArray(), valType);
  }

  private Bytes getKeyFromUpdateRow(Bytes prefix, Bytes row) {
    return row.subSequence(prefix.length(), row.length() - 8);
  }

  void process(TransactionBase tx, Bytes ntfyRow, Column col) throws Exception {

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
              Bytes.builder(lastKey.length() + 1).append(lastKey).append(new byte[] {0}).toBytes();
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

    byte[] dataPrefix = ntfyRow.toArray();
    // TODO this is awful... no sanity check... hard to read
    dataPrefix[Bytes.of(mapId).length() + 1] = 'd';

    BytesBuilder rowBuilder = Bytes.builder();
    rowBuilder.append(dataPrefix);
    int rowPrefixLen = rowBuilder.getLength();

    Set<Bytes> keysToFetch = updates.keySet();
    if (partiallyReadKey != null) {
      final Bytes prk = partiallyReadKey;
      keysToFetch = Sets.filter(keysToFetch, b -> !b.equals(prk));
    }
    Map<Bytes, Map<Column, Bytes>> currentVals = getCurrentValues(tx, rowBuilder, keysToFetch);

    ArrayList<Update<K, V>> updatesToReport = new ArrayList<>(updates.size());

    for (Entry<Bytes, List<Bytes>> entry : updates.entrySet()) {
      rowBuilder.setLength(rowPrefixLen);
      Bytes currentValueRow = rowBuilder.append(entry.getKey()).toBytes();
      Bytes currVal =
          currentVals.getOrDefault(currentValueRow, Collections.emptyMap()).get(DATA_COLUMN);

      Iterator<V> ui = Iterators.transform(entry.getValue().iterator(), this::deserVal);

      K kd = serializer.deserialize(entry.getKey().toArray(), keyType);

      if (partiallyReadKey != null && partiallyReadKey.equals(entry.getKey())) {
        // not all updates were read for this key, so requeue the combined updates as an update
        Optional<V> nv = combiner.combine(kd, ui);
        if (nv.isPresent()) {
          update(tx, Collections.singletonMap(kd, nv.get()));
        }
      } else {
        Optional<V> nv = combiner.combine(kd, concat(ui, currVal));
        Bytes newVal = nv.isPresent() ? Bytes.of(serializer.serialize(nv.get())) : null;
        if (newVal != null ^ currVal != null || (currVal != null && !currVal.equals(newVal))) {
          if (newVal == null) {
            tx.delete(currentValueRow, DATA_COLUMN);
          } else {
            tx.set(currentValueRow, DATA_COLUMN, newVal);
          }

          Optional<V> cvd = Optional.ofNullable(currVal).map(this::deserVal);
          updatesToReport.add(new Update<>(kd, cvd, nv));
        }
      }
    }

    // TODO could clear these as converted to objects to avoid double memory usage
    updates.clear();
    currentVals.clear();

    if (updatesToReport.size() > 0) {
      updateObserver.updatingValues(tx, updatesToReport.iterator());
    }
  }

  private static final Column DATA_COLUMN = new Column("data", "current");

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

  private Iterator<V> concat(Iterator<V> updates, Bytes currentVal) {
    if (currentVal == null) {
      return updates;
    }

    return Iterators.concat(updates, Iterators.singletonIterator(deserVal(currentVal)));
  }

  /**
   * This method will retrieve the current value for key and any outstanding updates and combine
   * them using the configured {@link Combiner}. The result from the combiner is returned.
   */
  public V get(SnapshotBase tx, K key) {

    byte[] k = serializer.serialize(key);

    int hash = Hashing.murmur3_32().hashBytes(k).asInt();
    String bucketId = genBucketId(Math.abs(hash % numBuckets), numBuckets);


    BytesBuilder rowBuilder = Bytes.builder();
    rowBuilder.append(mapId).append(":u:").append(bucketId).append(":").append(k);

    Iterator<RowColumnValue> iter =
        tx.scanner().over(Span.prefix(rowBuilder.toBytes())).build().iterator();

    Iterator<V> ui;

    if (iter.hasNext()) {
      ui = Iterators.transform(iter, rcv -> deserVal(rcv.getValue()));
    } else {
      ui = Collections.<V>emptyList().iterator();
    }

    rowBuilder.setLength(mapId.length());
    rowBuilder.append(":d:").append(bucketId).append(":").append(k);

    Bytes dataRow = rowBuilder.toBytes();

    Bytes cv = tx.get(dataRow, DATA_COLUMN);

    if (!ui.hasNext()) {
      if (cv == null) {
        return null;
      } else {
        return deserVal(cv);
      }
    }

    return combiner.combine(key, concat(ui, cv)).orElse(null);
  }

  String getId() {
    return mapId;
  }

  /**
   * Queues updates for a collision free map. These updates will be made by an Observer executing
   * another transaction. This method will not collide with other transaction queuing updates for
   * the same keys.
   *
   * @param tx This transaction will be used to make the updates.
   * @param updates The keys in the map should correspond to keys in the collision free map being
   *        updated. The values in the map will be queued for updating.
   */
  public void update(TransactionBase tx, Map<K, V> updates) {
    Preconditions.checkState(numBuckets > 0, "Not initialized");

    Set<String> buckets = new HashSet<>();

    BytesBuilder rowBuilder = Bytes.builder();
    rowBuilder.append(mapId).append(":u:");
    int prefixLength = rowBuilder.getLength();

    byte[] startTs = encSeq(tx.getStartTimestamp());

    for (Entry<K, V> entry : updates.entrySet()) {
      byte[] k = serializer.serialize(entry.getKey());
      int hash = Hashing.murmur3_32().hashBytes(k).asInt();
      String bucketId = genBucketId(Math.abs(hash % numBuckets), numBuckets);

      // reset to the common row prefix
      rowBuilder.setLength(prefixLength);

      Bytes row = rowBuilder.append(bucketId).append(":").append(k).append(startTs).toBytes();
      Bytes val = Bytes.of(serializer.serialize(entry.getValue()));

      // TODO set if not exists would be comforting here.... but
      // collisions on bucketId+key+uuid should never occur
      tx.set(row, UPDATE_COL, val);

      buckets.add(bucketId);
    }

    for (String bucketId : buckets) {
      rowBuilder.setLength(prefixLength);
      rowBuilder.append(bucketId).append(":");

      Bytes row = rowBuilder.toBytes();

      tx.setWeakNotification(row, new Column("fluoRecipes", "cfm:" + mapId));
    }
  }

  static String genBucketId(int bucket, int maxBucket) {
    Preconditions.checkArgument(bucket >= 0);
    Preconditions.checkArgument(maxBucket > 0);

    int bits = 32 - Integer.numberOfLeadingZeros(maxBucket);
    int bucketLen = bits / 4 + (bits % 4 > 0 ? 1 : 0);

    return Strings.padStart(Integer.toHexString(bucket), bucketLen, '0');
  }

  public static <K2, V2> CollisionFreeMap<K2, V2> getInstance(String mapId,
      SimpleConfiguration appConf) {
    Options opts = new Options(mapId, appConf);
    try {
      return new CollisionFreeMap<>(opts, SimpleSerializer.getInstance(appConf));
    } catch (Exception e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  /**
   * A {@link CollisionFreeMap} stores data in its own data format in the Fluo table. When
   * initializing a Fluo table with something like Map Reduce or Spark, data will need to be written
   * in this format. Thats the purpose of this method, it provide a simple class that can do this
   * conversion.
   */
  public static <K2, V2> Initializer<K2, V2> getInitializer(String mapId, int numBuckets,
      SimpleSerializer serializer) {
    return new Initializer<>(mapId, numBuckets, serializer);
  }

  /**
   * @see CollisionFreeMap#getInitializer(String, int, SimpleSerializer)
   *
   * @since 1.0.0
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
      String bucketId = genBucketId(Math.abs(hash % numBuckets), numBuckets);

      BytesBuilder bb = Bytes.builder();
      Bytes row = bb.append(mapId).append(":d:").append(bucketId).append(":").append(k).toBytes();
      byte[] v = serializer.serialize(val);

      return new RowColumnValue(row, DATA_COLUMN, Bytes.of(v));
    }
  }

  /**
   * @since 1.0.0
   */
  public static class Options {

    static final long DEFAULT_BUFFER_SIZE = 1 << 22;
    static final int DEFAULT_BUCKETS_PER_TABLET = 10;

    int numBuckets;
    Integer bucketsPerTablet = null;

    Long bufferSize;

    String keyType;
    String valueType;
    String combinerType;
    String updateObserverType;
    String mapId;

    private static final String PREFIX = "recipes.cfm.";

    Options(String mapId, SimpleConfiguration appConfig) {
      this.mapId = mapId;

      this.numBuckets = appConfig.getInt(PREFIX + mapId + ".buckets");
      this.combinerType = appConfig.getString(PREFIX + mapId + ".combiner");
      this.keyType = appConfig.getString(PREFIX + mapId + ".key");
      this.valueType = appConfig.getString(PREFIX + mapId + ".val");
      this.updateObserverType = appConfig.getString(PREFIX + mapId + ".updateObserver", null);
      this.bufferSize = appConfig.getLong(PREFIX + mapId + ".bufferSize", DEFAULT_BUFFER_SIZE);
      this.bucketsPerTablet =
          appConfig.getInt(PREFIX + mapId + ".bucketsPerTablet", DEFAULT_BUCKETS_PER_TABLET);
    }

    public Options(String mapId, String combinerType, String keyType, String valType, int buckets) {
      Preconditions.checkArgument(buckets > 0);
      Preconditions.checkArgument(!mapId.contains(":"), "Map id cannot contain ':'");

      this.mapId = mapId;
      this.numBuckets = buckets;
      this.combinerType = combinerType;
      this.updateObserverType = null;
      this.keyType = keyType;
      this.valueType = valType;
    }

    public Options(String mapId, String combinerType, String updateObserverType, String keyType,
        String valueType, int buckets) {
      Preconditions.checkArgument(buckets > 0);
      Preconditions.checkArgument(!mapId.contains(":"), "Map id cannot contain ':'");

      this.mapId = mapId;
      this.numBuckets = buckets;
      this.combinerType = combinerType;
      this.updateObserverType = updateObserverType;
      this.keyType = keyType;
      this.valueType = valueType;
    }

    /**
     * Sets a limit on the amount of serialized updates to read into memory. Additional memory will
     * be used to actually deserialize and process the updates. This limit does not account for
     * object overhead in java, which can be significant.
     *
     * <p>
     * The way memory read is calculated is by summing the length of serialized key and value byte
     * arrays. Once this sum exceeds the configured memory limit, no more update key values are
     * processed in the current transaction. When not everything is processed, the observer
     * processing updates will notify itself causing another transaction to continue processing
     * later
     */
    public Options setBufferSize(long bufferSize) {
      Preconditions.checkArgument(bufferSize > 0, "Buffer size must be positive");
      this.bufferSize = bufferSize;
      return this;
    }

    long getBufferSize() {
      if (bufferSize == null) {
        return DEFAULT_BUFFER_SIZE;
      }

      return bufferSize;
    }

    /**
     * Sets the number of buckets per tablet to generate. This affects how many split points will be
     * generated when optimizing the Accumulo table.
     *
     */
    public Options setBucketsPerTablet(int bucketsPerTablet) {
      Preconditions.checkArgument(bucketsPerTablet > 0, "bucketsPerTablet is <= 0 : "
          + bucketsPerTablet);
      this.bucketsPerTablet = bucketsPerTablet;
      return this;
    }

    int getBucketsPerTablet() {
      if (bucketsPerTablet == null) {
        return DEFAULT_BUCKETS_PER_TABLET;
      }

      return bucketsPerTablet;
    }

    public <K, V> Options(String mapId, Class<? extends Combiner<K, V>> combiner, Class<K> keyType,
        Class<V> valueType, int buckets) {
      this(mapId, combiner.getName(), keyType.getName(), valueType.getName(), buckets);
    }

    public <K, V> Options(String mapId, Class<? extends Combiner<K, V>> combiner,
        Class<? extends UpdateObserver<K, V>> updateObserver, Class<K> keyType, Class<V> valueType,
        int buckets) {
      this(mapId, combiner.getName(), updateObserver.getName(), keyType.getName(), valueType
          .getName(), buckets);
    }

    void save(SimpleConfiguration appConfig) {
      appConfig.setProperty(PREFIX + mapId + ".buckets", numBuckets + "");
      appConfig.setProperty(PREFIX + mapId + ".combiner", combinerType + "");
      appConfig.setProperty(PREFIX + mapId + ".key", keyType);
      appConfig.setProperty(PREFIX + mapId + ".val", valueType);
      if (updateObserverType != null) {
        appConfig.setProperty(PREFIX + mapId + ".updateObserver", updateObserverType + "");
      }
      if (bufferSize != null) {
        appConfig.setProperty(PREFIX + mapId + ".bufferSize", bufferSize);
      }
      if (bucketsPerTablet != null) {
        appConfig.setProperty(PREFIX + mapId + ".bucketsPerTablet", bucketsPerTablet);
      }
    }
  }

  /**
   * This method configures a collision free map for use. It must be called before initializing
   * Fluo.
   */
  public static void configure(FluoConfiguration fluoConfig, Options opts) {
    opts.save(fluoConfig.getAppConfiguration());
    fluoConfig.addObserver(new ObserverSpecification(CollisionFreeMapObserver.class.getName(),
        ImmutableMap.of("mapId", opts.mapId)));

    Bytes dataRangeEnd = Bytes.of(opts.mapId + DATA_RANGE_END);
    Bytes updateRangeEnd = Bytes.of(opts.mapId + UPDATE_RANGE_END);

    new TransientRegistry(fluoConfig.getAppConfiguration()).addTransientRange("cfm." + opts.mapId,
        new RowRange(dataRangeEnd, updateRangeEnd));

    TableOptimizations.registerOptimization(fluoConfig.getAppConfiguration(), opts.mapId,
        Optimizer.class);
  }

  public static class Optimizer implements TableOptimizationsFactory {

    /**
     * Return suggested Fluo table optimizations for the specified collisiong free map.
     *
     * @param appConfig Must pass in the application configuration obtained from
     *        {@code FluoClient.getAppConfiguration()} or
     *        {@code FluoConfiguration.getAppConfiguration()}
     */
    @Override
    public TableOptimizations getTableOptimizations(String mapId, SimpleConfiguration appConfig) {
      Options opts = new Options(mapId, appConfig);

      BytesBuilder rowBuilder = Bytes.builder();
      rowBuilder.append(mapId);

      List<Bytes> dataSplits = new ArrayList<>();
      for (int i = opts.getBucketsPerTablet(); i < opts.numBuckets; i += opts.getBucketsPerTablet()) {
        String bucketId = genBucketId(i, opts.numBuckets);
        rowBuilder.setLength(mapId.length());
        dataSplits.add(rowBuilder.append(":d:").append(bucketId).toBytes());
      }
      Collections.sort(dataSplits);

      List<Bytes> updateSplits = new ArrayList<>();
      for (int i = opts.getBucketsPerTablet(); i < opts.numBuckets; i += opts.getBucketsPerTablet()) {
        String bucketId = genBucketId(i, opts.numBuckets);
        rowBuilder.setLength(mapId.length());
        updateSplits.add(rowBuilder.append(":u:").append(bucketId).toBytes());
      }
      Collections.sort(updateSplits);

      Bytes dataRangeEnd = Bytes.of(opts.mapId + DATA_RANGE_END);
      Bytes updateRangeEnd = Bytes.of(opts.mapId + UPDATE_RANGE_END);

      List<Bytes> splits = new ArrayList<>();
      splits.add(dataRangeEnd);
      splits.add(updateRangeEnd);
      splits.addAll(dataSplits);
      splits.addAll(updateSplits);

      TableOptimizations tableOptim = new TableOptimizations();
      tableOptim.setSplits(splits);

      tableOptim.setTabletGroupingRegex(Pattern.quote(mapId + ":") + "[du]:");

      return tableOptim;
    }
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
}

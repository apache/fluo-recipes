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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import io.fluo.api.client.SnapshotBase;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.BytesBuilder;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumnValue;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.recipes.common.Pirtos;
import io.fluo.recipes.common.RowRange;
import io.fluo.recipes.common.TransientRegistry;
import io.fluo.recipes.impl.BucketUtil;
import io.fluo.recipes.serialization.SimpleSerializer;
import org.apache.commons.configuration.Configuration;

/**
 * See the project level documentation for information about this recipe.
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
    return row.subSequence(prefix.length(), row.length() - 16);
  }

  void process(TransactionBase tx, Bytes ntfyRow, Column col) throws Exception {
    ScannerConfiguration sc = new ScannerConfiguration();
    sc.setSpan(Span.prefix(ntfyRow));
    sc.fetchColumn(UPDATE_COL.getFamily(), UPDATE_COL.getQualifier());
    RowIterator iter = tx.get(sc);

    Map<Bytes, List<Bytes>> updates = new HashMap<>();

    long approxMemUsed = 0;

    Bytes partiallyReadKey = null;

    if (iter.hasNext()) {
      Bytes lastKey = null;
      while (iter.hasNext() && approxMemUsed < bufferSize) {
        Entry<Bytes, ColumnIterator> rowCol = iter.next();
        Bytes curRow = rowCol.getKey();

        // TODO processing too many at once increases chance of
        // collisions... may want to process a max # and notify self
        // when stopping could set a continue key

        tx.delete(curRow, UPDATE_COL);

        Bytes serializedKey = getKeyFromUpdateRow(ntfyRow, curRow);
        lastKey = serializedKey;

        List<Bytes> updateList = updates.get(serializedKey);
        if (updateList == null) {
          updateList = new ArrayList<>();
          updates.put(serializedKey, updateList);
          approxMemUsed += serializedKey.length();
        }

        Bytes val = rowCol.getValue().next().getValue();
        updateList.add(val);
        approxMemUsed += val.length();
      }

      if (iter.hasNext()) {
        Entry<Bytes, ColumnIterator> rowCol = iter.next();
        Bytes curRow = rowCol.getKey();

        // check if more updates for last key
        if (getKeyFromUpdateRow(ntfyRow, curRow).equals(lastKey)) {
          // there are still more updates for this key
          partiallyReadKey = lastKey;
        }

        // may not read all data because of mem limit, so notify self
        tx.setWeakNotification(ntfyRow, col);
      }
    }

    byte[] dataPrefix = ntfyRow.toArray();
    // TODO this is awful... no sanity check... hard to read
    dataPrefix[Bytes.of(mapId).length() + 1] = 'd';

    BytesBuilder rowBuilder = Bytes.newBuilder();
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
    String bucketId = BucketUtil.genBucketId(Math.abs(hash % numBuckets), numBuckets);


    BytesBuilder rowBuilder = Bytes.newBuilder();
    rowBuilder.append(mapId).append(":u:").append(bucketId).append(":").append(k);

    ScannerConfiguration sc = new ScannerConfiguration();
    sc.setSpan(Span.prefix(rowBuilder.toBytes()));

    RowIterator iter = tx.get(sc);

    Iterator<V> ui;

    if (iter.hasNext()) {
      ui = Iterators.transform(iter, e -> deserVal(e.getValue().next().getValue()));
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

    UUID uuid = UUID.randomUUID();

    Set<String> buckets = new HashSet<>();

    BytesBuilder rowBuilder = Bytes.newBuilder();
    rowBuilder.append(mapId).append(":u:");
    int prefixLength = rowBuilder.getLength();

    for (Entry<K, V> entry : updates.entrySet()) {
      byte[] k = serializer.serialize(entry.getKey());
      int hash = Hashing.murmur3_32().hashBytes(k).asInt();
      String bucketId = BucketUtil.genBucketId(Math.abs(hash % numBuckets), numBuckets);

      // reset to the common row prefix
      rowBuilder.setLength(prefixLength);

      Bytes row =
          rowBuilder.append(bucketId).append(":").append(k).append(uuidToBytes(uuid)).toBytes();
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


  public static <K2, V2> CollisionFreeMap<K2, V2> getInstance(String mapId, Configuration appConf) {
    Options opts = new Options(mapId, appConf);
    try {
      return new CollisionFreeMap<>(opts, SimpleSerializer.getInstance(appConf));
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
    return new Initializer<>(mapId, numBuckets, serializer);
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
      String bucketId = BucketUtil.genBucketId(Math.abs(hash % numBuckets), numBuckets);

      BytesBuilder bb = Bytes.newBuilder();
      Bytes row = bb.append(mapId).append(":d:").append(bucketId).append(":").append(k).toBytes();
      byte[] v = serializer.serialize(val);

      return new RowColumnValue(row, DATA_COLUMN, Bytes.of(v));
    }
  }

  public static class Options {

    private static final long DEFAULT_BUFFER_SIZE = 1 << 22;

    int numBuckets;
    private Long bufferSize;

    String keyType;
    String valueType;
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
      this.updateObserverType = appConfig.getString(PREFIX + mapId + ".updateObserver", null);
      this.bufferSize = appConfig.getLong(PREFIX + mapId + ".bufferSize", DEFAULT_BUFFER_SIZE);
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

    void save(Configuration appConfig) {
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
    }
  }

  /**
   * This method configures a collision free map for use. It must be called before initializing
   * Fluo.
   */
  public static void configure(FluoConfiguration fluoConfig, Options opts) {
    opts.save(fluoConfig.getAppConfiguration());
    fluoConfig.addObserver(new ObserverConfiguration(CollisionFreeMapObserver.class.getName())
        .setParameters(ImmutableMap.of("mapId", opts.mapId)));

    Bytes dataRangeEnd = Bytes.of(opts.mapId + DATA_RANGE_END);
    Bytes updateRangeEnd = Bytes.of(opts.mapId + UPDATE_RANGE_END);

    new TransientRegistry(fluoConfig.getAppConfiguration()).addTransientRange("cfm." + opts.mapId,
        new RowRange(dataRangeEnd, updateRangeEnd));
  }

  /**
   * Return suggested Fluo table optimizations for all previously configured collision free maps.
   *
   * @param appConfig Must pass in the application configuration obtained from
   *        {@code FluoClient.getAppConfiguration()} or
   *        {@code FluoConfiguration.getAppConfiguration()}
   */
  public static Pirtos getTableOptimizations(Configuration appConfig) {
    HashSet<String> mapIds = new HashSet<>();
    appConfig.getKeys(Options.PREFIX.substring(0, Options.PREFIX.length() - 1)).forEachRemaining(
        k -> mapIds.add(k.substring(Options.PREFIX.length()).split("\\.", 2)[0]));

    Pirtos pirtos = new Pirtos();
    mapIds.forEach(mid -> pirtos.merge(getTableOptimizations(mid, appConfig)));

    return pirtos;
  }

  /**
   * Return suggested Fluo table optimizations for the specified collisiong free map.
   *
   * @param appConfig Must pass in the application configuration obtained from
   *        {@code FluoClient.getAppConfiguration()} or
   *        {@code FluoConfiguration.getAppConfiguration()}
   */
  public static Pirtos getTableOptimizations(String mapId, Configuration appConfig) {
    Options opts = new Options(mapId, appConfig);

    BytesBuilder rowBuilder = Bytes.newBuilder();
    rowBuilder.append(mapId);

    List<Bytes> dataSplits = new ArrayList<>();
    for (int i = 1; i < opts.numBuckets; i++) {
      String bucketId = BucketUtil.genBucketId(i, opts.numBuckets);
      rowBuilder.setLength(mapId.length());
      dataSplits.add(rowBuilder.append(":d:").append(bucketId).toBytes());
    }
    Collections.sort(dataSplits);

    List<Bytes> updateSplits = new ArrayList<>();
    for (int i = 1; i < opts.numBuckets; i++) {
      String bucketId = BucketUtil.genBucketId(i, opts.numBuckets);
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

    Pirtos pirtos = new Pirtos();
    pirtos.setSplits(splits);

    pirtos.setTabletGroupingRegex(Pattern.quote(mapId + ":") + "[du]:");

    return pirtos;
  }

  private Bytes uuidToBytes(UUID uuid) {
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    bb.rewind();
    return Bytes.of(bb);
  }
}

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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.hash.Hashing;
import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.api.observer.StringObserver;
import org.apache.fluo.recipes.core.combine.CombineQueue;
import org.apache.fluo.recipes.core.common.TableOptimizations;
import org.apache.fluo.recipes.core.common.TableOptimizations.TableOptimizationsFactory;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;

/**
 * See the project level documentation for information about this recipe.
 *
 * @since 1.0.0
 * @deprecated since 1.1.0 use {@link CombineQueue}
 */
public class CollisionFreeMap<K, V> {

  private Bytes updatePrefix;
  private Bytes dataPrefix;

  private Class<V> valType;
  private SimpleSerializer serializer;
  private Combiner<K, V> combiner;
  UpdateObserver<K, V> updateObserver;

  static final Column UPDATE_COL = new Column("u", "v");
  static final Column NEXT_COL = new Column("u", "next");

  private int numBuckets = -1;

  private CombineQueue<K, V> combineQ;
  private Observer combineQueueObserver;

  private static class CfmRegistry implements ObserverProvider.Registry {

    Observer observer;

    @Override
    public void register(Column observedColumn, NotificationType ntfyType, Observer observer) {
      this.observer = observer;
    }

    @Override
    public void registers(Column observedColumn, NotificationType ntfyType, StringObserver observer) {
      this.observer = observer;
    }
  }

  @SuppressWarnings("unchecked")
  CollisionFreeMap(SimpleConfiguration appConfig, Options opts, SimpleSerializer serializer)
      throws Exception {
    this.updatePrefix = Bytes.of(opts.mapId + ":u:");
    this.dataPrefix = Bytes.of(opts.mapId + ":d:");

    this.numBuckets = opts.numBuckets;
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

    combineQ = CombineQueue.getInstance(opts.mapId, appConfig);

    // When this class was deprecated, most of its code was copied to CombineQueue. The following
    // code is a round about way of using that copied code, with having to make anything in
    // CombineQueue public.
    CfmRegistry obsRegistry = new CfmRegistry();
    combineQ.registerObserver(obsRegistry, i -> this.combiner.combine(i.getKey(), i.iterator()), (
        tx, changes) -> this.updateObserver.updatingValues(tx, Update.transform(changes)));
    combineQueueObserver = obsRegistry.observer;
  }

  private V deserVal(Bytes val) {
    return serializer.deserialize(val.toArray(), valType);
  }

  void process(TransactionBase tx, Bytes ntfyRow, Column col) throws Exception {
    combineQueueObserver.process(tx, ntfyRow, col);
  }

  private static final Column DATA_COLUMN = new Column("data", "current");

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
    rowBuilder.append(updatePrefix).append(bucketId).append(':').append(k);

    Iterator<RowColumnValue> iter =
        tx.scanner().over(Span.prefix(rowBuilder.toBytes())).build().iterator();

    Iterator<V> ui;

    if (iter.hasNext()) {
      ui = Iterators.transform(iter, rcv -> deserVal(rcv.getValue()));
    } else {
      ui = Collections.<V>emptyList().iterator();
    }

    rowBuilder.setLength(0);
    rowBuilder.append(dataPrefix).append(bucketId).append(':').append(k);

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
    combineQ.addAll(tx, updates);
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
      return new CollisionFreeMap<>(appConf, opts, SimpleSerializer.getInstance(appConf));
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
   * @deprecated since 1.1.0
   */
  @Deprecated
  public static class Initializer<K2, V2> implements Serializable {

    private static final long serialVersionUID = 1L;

    private org.apache.fluo.recipes.core.combine.CombineQueue.Initializer<K2, V2> initializer;

    private Initializer(String mapId, int numBuckets, SimpleSerializer serializer) {
      this.initializer = CombineQueue.getInitializer(mapId, numBuckets, serializer);
    }

    public RowColumnValue convert(K2 key, V2 val) {
      return initializer.convert(key, val);
    }
  }

  /**
   * @since 1.0.0
   * @deprecated since 1.1.0
   */
  @Deprecated
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
     */
    public Options setBucketsPerTablet(int bucketsPerTablet) {
      Preconditions.checkArgument(bucketsPerTablet > 0, "bucketsPerTablet is <= 0 : "
          + bucketsPerTablet);
      this.bucketsPerTablet = bucketsPerTablet;
      return this;
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
      appConfig.setProperty(PREFIX + mapId + ".combiner", combinerType + "");
      if (updateObserverType != null) {
        appConfig.setProperty(PREFIX + mapId + ".updateObserver", updateObserverType + "");
      }
    }
  }

  /**
   * This method configures a collision free map for use. It must be called before initializing
   * Fluo.
   */
  public static void configure(FluoConfiguration fluoConfig, Options opts) {
    org.apache.fluo.recipes.core.combine.CombineQueue.FluentOptions cqopts =
        CombineQueue.configure(opts.mapId).keyType(opts.keyType).valueType(opts.valueType)
            .buckets(opts.numBuckets);
    if (opts.bucketsPerTablet != null) {
      cqopts.bucketsPerTablet(opts.bucketsPerTablet);
    }
    if (opts.bufferSize != null) {
      cqopts.bufferSize(opts.bufferSize);
    }
    cqopts.save(fluoConfig);

    opts.save(fluoConfig.getAppConfiguration());

    fluoConfig.addObserver(new ObserverSpecification(CollisionFreeMapObserver.class.getName(),
        ImmutableMap.of("mapId", opts.mapId)));
  }

  /**
   * @deprecated since 1.1.0 use {@link org.apache.fluo.recipes.core.combine.CombineQueue.Optimizer}
   */
  @Deprecated
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
      return new org.apache.fluo.recipes.core.combine.CombineQueue.Optimizer()
          .getTableOptimizations(mapId, appConfig);
    }
  }
}

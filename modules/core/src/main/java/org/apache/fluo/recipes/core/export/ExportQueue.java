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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.recipes.core.common.TableOptimizations;
import org.apache.fluo.recipes.core.common.TableOptimizations.TableOptimizationsFactory;
import org.apache.fluo.recipes.core.common.RowRange;
import org.apache.fluo.recipes.core.common.TransientRegistry;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;

/**
 * @since 1.0.0
 */
public class ExportQueue<K, V> {

  private static final String RANGE_BEGIN = "#";
  private static final String RANGE_END = ":~";

  private int numBuckets;
  private SimpleSerializer serializer;
  private String queueId;

  // usage hint : could be created once in an observers init method
  // usage hint : maybe have a queue for each type of data being exported???
  // maybe less queues are
  // more efficient though because more batching at export time??
  ExportQueue(Options opts, SimpleSerializer serializer) throws Exception {
    // TODO sanity check key type based on type params
    // TODO defer creating classes until needed.. so that its not done during Fluo init
    this.queueId = opts.queueId;
    this.numBuckets = opts.numBuckets;
    this.serializer = serializer;
  }

  public void add(TransactionBase tx, K key, V value) {
    addAll(tx, Collections.singleton(new Export<>(key, value)).iterator());
  }

  public void addAll(TransactionBase tx, Iterator<Export<K, V>> exports) {

    Set<Integer> bucketsNotified = new HashSet<>();
    while (exports.hasNext()) {
      Export<K, V> export = exports.next();

      byte[] k = serializer.serialize(export.getKey());
      byte[] v = serializer.serialize(export.getValue());

      int hash = Hashing.murmur3_32().hashBytes(k).asInt();
      int bucketId = Math.abs(hash % numBuckets);

      ExportBucket bucket = new ExportBucket(tx, queueId, bucketId, numBuckets);
      bucket.add(tx.getStartTimestamp(), k, v);

      if (!bucketsNotified.contains(bucketId)) {
        bucket.notifyExportObserver();
        bucketsNotified.add(bucketId);
      }
    }
  }

  public static <K2, V2> ExportQueue<K2, V2> getInstance(String exportQueueId,
      SimpleConfiguration appConfig) {
    Options opts = new Options(exportQueueId, appConfig);
    try {
      return new ExportQueue<>(opts, SimpleSerializer.getInstance(appConfig));
    } catch (Exception e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  /**
   * Call this method before initializing Fluo.
   *
   * @param fluoConfig The configuration that will be used to initialize fluo.
   */
  public static void configure(FluoConfiguration fluoConfig, Options opts) {
    SimpleConfiguration appConfig = fluoConfig.getAppConfiguration();
    opts.save(appConfig);

    fluoConfig.addObserver(new ObserverSpecification(ExportObserver.class.getName(), Collections
        .singletonMap("queueId", opts.queueId)));

    Bytes exportRangeStart = Bytes.of(opts.queueId + RANGE_BEGIN);
    Bytes exportRangeStop = Bytes.of(opts.queueId + RANGE_END);

    new TransientRegistry(fluoConfig.getAppConfiguration()).addTransientRange("exportQueue."
        + opts.queueId, new RowRange(exportRangeStart, exportRangeStop));

    TableOptimizations.registerOptimization(appConfig, opts.queueId, Optimizer.class);
  }

  public static class Optimizer implements TableOptimizationsFactory {

    /**
     * Return suggested Fluo table optimizations for the specified export queue.
     *
     * @param appConfig Must pass in the application configuration obtained from
     *        {@code FluoClient.getAppConfiguration()} or
     *        {@code FluoConfiguration.getAppConfiguration()}
     */
    @Override
    public TableOptimizations getTableOptimizations(String queueId, SimpleConfiguration appConfig) {
      Options opts = new Options(queueId, appConfig);

      List<Bytes> splits = new ArrayList<>();

      Bytes exportRangeStart = Bytes.of(opts.queueId + RANGE_BEGIN);
      Bytes exportRangeStop = Bytes.of(opts.queueId + RANGE_END);

      splits.add(exportRangeStart);
      splits.add(exportRangeStop);

      List<Bytes> exportSplits = new ArrayList<>();
      for (int i = opts.getBucketsPerTablet(); i < opts.numBuckets; i += opts.getBucketsPerTablet()) {
        exportSplits.add(ExportBucket.generateBucketRow(opts.queueId, i, opts.numBuckets));
      }
      Collections.sort(exportSplits);
      splits.addAll(exportSplits);

      TableOptimizations tableOptim = new TableOptimizations();
      tableOptim.setSplits(splits);

      // the tablet with end row <queueId># does not contain any data for the export queue and
      // should not be grouped with the export queue
      tableOptim.setTabletGroupingRegex(Pattern.quote(queueId + ":"));

      return tableOptim;
    }

  }

  /**
   * @since 1.0.0
   */
  public static class Options {

    private static final String PREFIX = "recipes.exportQueue.";
    static final long DEFAULT_BUFFER_SIZE = 1 << 20;
    static final int DEFAULT_BUCKETS_PER_TABLET = 10;

    int numBuckets;
    Integer bucketsPerTablet = null;
    Long bufferSize;

    String keyType;
    String valueType;
    String exporterType;
    String queueId;
    SimpleConfiguration exporterConfig;

    Options(String queueId, SimpleConfiguration appConfig) {
      this.queueId = queueId;

      this.numBuckets = appConfig.getInt(PREFIX + queueId + ".buckets");
      this.exporterType = appConfig.getString(PREFIX + queueId + ".exporter");
      this.keyType = appConfig.getString(PREFIX + queueId + ".key");
      this.valueType = appConfig.getString(PREFIX + queueId + ".val");
      this.bufferSize = appConfig.getLong(PREFIX + queueId + ".bufferSize", DEFAULT_BUFFER_SIZE);
      this.bucketsPerTablet =
          appConfig.getInt(PREFIX + queueId + ".bucketsPerTablet", DEFAULT_BUCKETS_PER_TABLET);

      this.exporterConfig = appConfig.subset(PREFIX + queueId + ".exporterCfg");
    }

    public Options(String queueId, String exporterType, String keyType, String valueType,
        int buckets) {
      Preconditions.checkArgument(buckets > 0);

      this.queueId = queueId;
      this.numBuckets = buckets;
      this.exporterType = exporterType;
      this.keyType = keyType;
      this.valueType = valueType;
    }

    public <K, V> Options(String queueId, Class<? extends Exporter<K, V>> exporter,
        Class<K> keyType, Class<V> valueType, int buckets) {
      this(queueId, exporter.getName(), keyType.getName(), valueType.getName(), buckets);
    }

    /**
     * Sets a limit on the amount of serialized updates to read into memory. Additional memory will
     * be used to actually deserialize and process the updates. This limit does not account for
     * object overhead in java, which can be significant.
     *
     * <p>
     * The way memory read is calculated is by summing the length of serialized key and value byte
     * arrays. Once this sum exceeds the configured memory limit, no more export key values are
     * processed in the current transaction. When not everything is processed, the observer
     * processing exports will notify itself causing another transaction to continue processing
     * later.
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

    /**
     * Sets exporter specific configuration. This configuration will be made available to an
     * {@link Exporter} via {@link Exporter.Context#getExporterConfiguration()}.
     */
    public Options setExporterConfiguration(SimpleConfiguration config) {
      Objects.requireNonNull(config);
      this.exporterConfig = config;
      return this;
    }

    public SimpleConfiguration getExporterConfiguration() {
      if (exporterConfig == null) {
        return new SimpleConfiguration();
      }

      return exporterConfig;
    }

    public String getQueueId() {
      return queueId;
    }

    void save(SimpleConfiguration appConfig) {
      appConfig.setProperty(PREFIX + queueId + ".buckets", numBuckets + "");
      appConfig.setProperty(PREFIX + queueId + ".exporter", exporterType + "");
      appConfig.setProperty(PREFIX + queueId + ".key", keyType);
      appConfig.setProperty(PREFIX + queueId + ".val", valueType);

      if (bufferSize != null) {
        appConfig.setProperty(PREFIX + queueId + ".bufferSize", bufferSize);
      }

      if (bucketsPerTablet != null) {
        appConfig.setProperty(PREFIX + queueId + ".bucketsPerTablet", bucketsPerTablet);
      }

      if (exporterConfig != null) {
        Iterator<String> keys = exporterConfig.getKeys();
        while (keys.hasNext()) {
          String key = keys.next();
          appConfig.setProperty(PREFIX + queueId + ".exporterCfg." + key,
              exporterConfig.getRawString(key));
        }
      }
    }
  }
}

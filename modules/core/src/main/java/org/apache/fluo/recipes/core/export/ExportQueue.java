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
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.recipes.core.common.TableOptimizations;
import org.apache.fluo.recipes.core.common.TableOptimizations.TableOptimizationsFactory;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;

/**
 * @since 1.0.0
 */
public class ExportQueue<K, V> {

  static final String RANGE_BEGIN = "#";
  static final String RANGE_END = ":~";

  private int numBuckets;
  private SimpleSerializer serializer;
  private String queueId;
  private FluentConfigurator opts;

  // usage hint : could be created once in an observers init method
  // usage hint : maybe have a queue for each type of data being exported???
  // maybe less queues are
  // more efficient though because more batching at export time??
  ExportQueue(FluentConfigurator opts, SimpleSerializer serializer) throws Exception {
    // TODO sanity check key type based on type params
    // TODO defer creating classes until needed.. so that its not done during Fluo init
    this.queueId = opts.queueId;
    this.numBuckets = opts.buckets;
    this.serializer = serializer;
    this.opts = opts;
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

  // TODO maybe add for stream and interable

  public static <K2, V2> ExportQueue<K2, V2> getInstance(String exportQueueId,
      SimpleConfiguration appConfig) {
    FluentConfigurator opts = FluentConfigurator.load(exportQueueId, appConfig);
    try {
      return new ExportQueue<>(opts, SimpleSerializer.getInstance(appConfig));
    } catch (Exception e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  /**
   * Part of a fluent API for configuring a export queue.
   * 
   * @since 1.1.0
   */
  public static interface FluentArg1 {
    public FluentArg2 keyType(String keyType);

    public FluentArg2 keyType(Class<?> keyType);
  }

  /**
   * Part of a fluent API for configuring a export queue.
   * 
   * @since 1.1.0
   */
  public static interface FluentArg2 {
    public FluentArg3 valueType(String keyType);

    public FluentArg3 valueType(Class<?> keyType);
  }

  /**
   * Part of a fluent API for configuring a export queue.
   * 
   * @since 1.1.0
   */
  public static interface FluentArg3 {
    FluentOptions buckets(int numBuckets);
  }

  /**
   * Part of a fluent API for configuring a export queue.
   * 
   * @since 1.1.0
   */
  public static interface FluentOptions {
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
    public FluentOptions bufferSize(long bufferSize);

    /**
     * Sets the number of buckets per tablet to generate. This affects how many split points will be
     * generated when optimizing the Accumulo table.
     */
    public FluentOptions bucketsPerTablet(int bucketsPerTablet);

    /**
     * Adds properties to the Fluo application configuration for this CombineQueue.
     */
    public void save(FluoConfiguration fluoConfig);
  }

  /**
   * A Fluent API for configuring an Export Queue. Use this method in conjunction with
   * {@link #registerObserver(ObserverProvider.Registry, org.apache.fluo.recipes.core.export.function.Exporter)}
   *
   * @param exportQueueId An id that uniquely identifies an export queue. This id is used in the
   *        keys in the Fluo table and in the keys in the Fluo application configuration.
   * @since 1.1.0
   */
  public static FluentArg1 configure(String exportQueueId) {
    return new FluentConfigurator(Objects.requireNonNull(exportQueueId));
  }

  /**
   * Call this method before initializing Fluo.
   *
   * @param fluoConfig The configuration that will be used to initialize fluo.
   * @deprecated since 1.1.0 use {@link #configure(String)} and
   *             {@link #registerObserver(ObserverProvider.Registry, org.apache.fluo.recipes.core.export.function.Exporter)}
   *             instead.
   */
  @Deprecated
  public static void configure(FluoConfiguration fluoConfig, Options opts) {
    SimpleConfiguration appConfig = fluoConfig.getAppConfiguration();
    opts.save(appConfig);

    fluoConfig.addObserver(new ObserverSpecification(ExportObserver.class.getName(), Collections
        .singletonMap("queueId", opts.fluentCfg.queueId)));
  }

  /**
   * @since 1.0.0
   */
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
      FluentConfigurator opts = FluentConfigurator.load(queueId, appConfig);

      List<Bytes> splits = new ArrayList<>();

      Bytes exportRangeStart = Bytes.of(opts.queueId + RANGE_BEGIN);
      Bytes exportRangeStop = Bytes.of(opts.queueId + RANGE_END);

      splits.add(exportRangeStart);
      splits.add(exportRangeStop);

      List<Bytes> exportSplits = new ArrayList<>();
      for (int i = opts.getBucketsPerTablet(); i < opts.buckets; i += opts.getBucketsPerTablet()) {
        exportSplits.add(ExportBucket.generateBucketRow(opts.queueId, i, opts.buckets));
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
   * Registers an observer that will export queued data. Use this method in conjunction with
   * {@link ExportQueue#configure(String)}.
   * 
   * @since 1.1.0
   */
  public void registerObserver(ObserverProvider.Registry obsRegistry,
      org.apache.fluo.recipes.core.export.function.Exporter<K, V> exporter) {
    Preconditions
        .checkState(
            opts.exporterType == null,
            "Expected exporter type not be set, it was set to %s.  Cannot not use the old and new way of configuring "
                + "exporters at the same time.", opts.exporterType);
    Observer obs;
    try {
      obs = new ExportObserverImpl<K, V>(queueId, opts, serializer, exporter);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    obsRegistry.forColumn(ExportBucket.newNotificationColumn(queueId), NotificationType.WEAK)
        .withId("exportq-" + queueId).useObserver(obs);
  }

  /**
   * @since 1.0.0
   * @deprecated since 1.1.0 use {@link ExportQueue#configure(String)}
   */
  public static class Options {

    private static final String PREFIX = FluentConfigurator.PREFIX;

    FluentConfigurator fluentCfg;
    SimpleConfiguration exporterConfig;

    Options(String queueId, SimpleConfiguration appConfig) {
      fluentCfg = FluentConfigurator.load(queueId, appConfig);
      this.exporterConfig = appConfig.subset(PREFIX + queueId + ".exporterCfg");
    }

    public Options(String queueId, String exporterType, String keyType, String valueType,
        int buckets) {
      this(queueId, keyType, valueType, buckets);
      fluentCfg.exporterType = Objects.requireNonNull(exporterType);
    }

    public <K, V> Options(String queueId, Class<? extends Exporter<K, V>> exporter,
        Class<K> keyType, Class<V> valueType, int buckets) {
      this(queueId, exporter.getName(), keyType.getName(), valueType.getName(), buckets);
    }

    // intentionally package private
    Options(String queueId, String keyType, String valueType, int buckets) {
      Preconditions.checkArgument(buckets > 0);
      this.fluentCfg =
          (FluentConfigurator) new FluentConfigurator(queueId).keyType(keyType)
              .valueType(valueType).buckets(buckets);
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
      fluentCfg.bufferSize(bufferSize);
      return this;
    }

    long getBufferSize() {
      return fluentCfg.getBufferSize();
    }

    /**
     * Sets the number of buckets per tablet to generate. This affects how many split points will be
     * generated when optimizing the Accumulo table.
     *
     */
    public Options setBucketsPerTablet(int bucketsPerTablet) {
      fluentCfg.bucketsPerTablet(bucketsPerTablet);
      return this;
    }

    int getBucketsPerTablet() {
      return fluentCfg.getBucketsPerTablet();
    }

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
      return fluentCfg.queueId;
    }

    void save(SimpleConfiguration appConfig) {
      fluentCfg.save(appConfig);

      if (exporterConfig != null) {
        Iterator<String> keys = exporterConfig.getKeys();
        while (keys.hasNext()) {
          String key = keys.next();
          appConfig.setProperty(PREFIX + fluentCfg.queueId + ".exporterCfg." + key,
              exporterConfig.getRawString(key));
        }
      }
    }
  }
}

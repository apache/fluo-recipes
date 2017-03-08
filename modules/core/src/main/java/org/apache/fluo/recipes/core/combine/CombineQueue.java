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

import java.io.Serializable;
import java.util.Map;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.observer.ObserverProvider.Registry;
import org.apache.fluo.recipes.core.common.TableOptimizations;
import org.apache.fluo.recipes.core.common.TableOptimizations.TableOptimizationsFactory;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;

/**
 * See the project level documentation for information about this recipe.
 *
 * @since 1.1.0
 */
public interface CombineQueue<K, V> {

  /**
   * Queues updates for this combine queue. These updates will be made by an Observer executing in
   * another transaction. This method will not collide with other transaction queuing updates for
   * the same keys.
   *
   * @param tx This transaction will be used to make the updates.
   * @param updates The keys in the map should correspond to keys in the collision free map being
   *        updated. The values in the map will be queued for updating.
   */
  public void addAll(TransactionBase tx, Map<K, V> updates);

  /**
   * Used to register a Fluo Observer that processes updates to this combine queue. If this is not
   * called, then updates will never be processed.
   * 
   */
  public void registerObserver(Registry obsRegistry, Combiner<K, V> combiner,
      ChangeObserver<K, V> updateObserver);

  /**
   * Get a combiner queue instance.
   * 
   * @param combineQueueId This should be the same id passed to {@link #configure(String)} before
   *        initializing Fluo.
   * @param appConfig Application configuration obtained from
   *        {@code FluoClient.getAppConfiguration()},
   *        {@code FluoConfiguration.getAppConfiguration()}, or
   *        {@code ObserverProvider.Context.getAppConfiguration()}
   */
  public static <K2, V2> CombineQueue<K2, V2> getInstance(String combineQueueId,
      SimpleConfiguration appConfig) {
    try {
      return new CombineQueueImpl<>(combineQueueId, appConfig);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Part of a fluent API for configuring a combine queue.
   * 
   * @since 1.1.0
   */
  public static interface FluentArg1 {
    public FluentArg2 keyType(String keyType);

    public FluentArg2 keyType(Class<?> keyType);
  }

  /**
   * Part of a fluent API for configuring a combine queue.
   * 
   * @since 1.1.0
   */
  public static interface FluentArg2 {
    public FluentArg3 valueType(String valType);

    public FluentArg3 valueType(Class<?> valType);
  }

  /**
   * Part of a fluent API for configuring a combine queue.
   * 
   * @since 1.1.0
   */
  public static interface FluentArg3 {
    FluentOptions buckets(int numBuckets);
  }

  /**
   * Part of a fluent API for configuring a combine queue.
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
     * arrays. Once this sum exceeds the configured memory limit, no more update key values are
     * processed in the current transaction. When not everything is processed, the observer
     * processing updates will notify itself causing another transaction to continue processing
     * later
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
   * Call this method before initializing Fluo to configure a combine queue.
   * 
   * @param combineQueueId An id that uniquely identifies a combine queue. This id is used in the
   *        keys in the Fluo table and in the keys in the Fluo application configuration.
   * @return A Fluent configurator.
   */
  public static FluentArg1 configure(String combineQueueId) {
    return new CqConfigurator(combineQueueId);
  }

  /**
   * @since 1.1.0
   */
  public static interface Initializer<K2, V2> extends Serializable {
    public RowColumnValue convert(K2 key, V2 val);
  }

  /**
   * A {@link CombineQueue} stores data in its own data format in the Fluo table. When initializing
   * a Fluo table with something like Map Reduce or Spark, data will need to be written in this
   * format. Thats the purpose of this method, it provides a simple class that can do this
   * conversion.
   */
  public static <K2, V2> Initializer<K2, V2> getInitializer(String cqId, int numBuckets,
      SimpleSerializer serializer) {
    return new InitializerImpl<>(cqId, numBuckets, serializer);
  }

  /**
   * @since 1.1.0
   */
  public static class Optimizer implements TableOptimizationsFactory {
    /**
     * Return suggested Fluo table optimizations for the specified combine queue.
     *
     * @param appConfig Must pass in the application configuration obtained from
     *        {@code FluoClient.getAppConfiguration()} or
     *        {@code FluoConfiguration.getAppConfiguration()}
     */
    @Override
    public TableOptimizations getTableOptimizations(String cqId, SimpleConfiguration appConfig) {
      return CqOptimizer.getTableOptimizations(cqId, appConfig);
    }
  }
}

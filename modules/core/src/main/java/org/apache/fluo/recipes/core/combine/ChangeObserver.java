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

import java.util.Optional;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.recipes.core.export.ExportQueue;

/**
 * {@link CombineQueue} uses this interface to notify of changes to a keys value. It provides the
 * new and old value for a key. For efficiency, the {@link CombineQueue} processes batches of key
 * updates at once. It is strongly advised to only use the passed in transaction for writes that are
 * unlikely to collide. If one write collides, then it will cause the whole batch to fail. Examples
 * of writes that will not collide are updating an {@link ExportQueue} or another
 * {@link CombineQueue}.
 *
 * <p>
 * It was advised to only do writes because reads for each key will slow down processing a batch. If
 * reading data is necessary then consider doing batch reads.
 *
 * @since 1.1.0
 */
@FunctionalInterface
public interface ChangeObserver<K, V> {

  /**
   * @since 1.1.0
   */
  public static interface Change<K2, V2> {
    public K2 getKey();

    public Optional<V2> getNewValue();

    public Optional<V2> getOldValue();
  }

  void process(TransactionBase tx, Iterable<Change<K, V>> changes);
}

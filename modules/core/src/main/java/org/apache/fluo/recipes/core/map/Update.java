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

import java.util.Iterator;
import java.util.Optional;

import com.google.common.collect.Iterators;
import org.apache.fluo.recipes.core.combine.ChangeObserver.Change;

/**
 * @since 1.0.0
 * @deprecated since 1.1.0
 */
@Deprecated
public class Update<K, V> {

  private final K key;
  private final Optional<V> oldValue;
  private final Optional<V> newValue;

  Update(K key, Optional<V> oldValue, Optional<V> newValue) {
    this.key = key;
    this.oldValue = oldValue;
    this.newValue = newValue;
  }

  public K getKey() {
    return key;
  }

  public Optional<V> getNewValue() {
    return newValue;
  }

  public Optional<V> getOldValue() {
    return oldValue;
  }

  static <K2, V2> Iterator<Update<K2, V2>> transform(Iterable<Change<K2, V2>> changes) {
    return Iterators.transform(changes.iterator(), change -> new Update<K2, V2>(change.getKey(),
        change.getOldValue(), change.getNewValue()));
  }
}

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

package org.apache.fluo.recipes.export;

import java.util.Objects;

public class Export<K, V> {
  private final K key;
  private final V value;

  public Export(K key, V val) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(val);
    this.key = key;
    this.value = val;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }
}

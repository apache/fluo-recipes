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

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.collect.Iterators;
import org.apache.fluo.api.data.Bytes;

// intentionally package priave
class InputImpl<K, V> implements Combiner.Input<K, V> {
  private K key;
  private Collection<Bytes> valuesCollection;
  private Bytes currentValue;
  private Function<Bytes, V> valDeser;

  InputImpl(K k, Function<Bytes, V> valDeser, Collection<Bytes> serializedValues) {
    this.key = k;
    this.valDeser = valDeser;
    this.valuesCollection = serializedValues;
  }

  InputImpl(K k, Function<Bytes, V> valDeser, Bytes currentValue, Collection<Bytes> serializedValues) {
    this(k, valDeser, serializedValues);
    this.currentValue = currentValue;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public Stream<V> stream() {
    Stream<Bytes> bytesStream = valuesCollection.stream();
    if (currentValue != null) {
      bytesStream = Stream.concat(Stream.of(currentValue), bytesStream);
    }

    return bytesStream.map(valDeser);
  }

  @Override
  public Iterator<V> iterator() {
    Iterator<Bytes> bytesIter = valuesCollection.iterator();
    if (currentValue != null) {
      bytesIter = Iterators.concat(bytesIter, Iterators.singletonIterator(currentValue));
    }
    return Iterators.transform(bytesIter, valDeser::apply);
  }

}

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

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * This class was created as an alternative to {@link Combiner}. It supports easy and efficient use
 * of java streams when implementing combiners using lambdas.
 * 
 * @since 1.1.0
 */
@FunctionalInterface
public interface Combiner<K, V> {

  /**
   * 
   * @since 1.1.0
   */
  public static interface Input<KI, VI> extends Iterable<VI> {
    KI getKey();

    Stream<VI> stream();

    Iterator<VI> iterator();
  }

  /**
   * This function is called to combine the current value of a key with updates that were queued for
   * the key. See the collision free map project level documentation for more information.
   *
   * @return Then new value for the key. Returning Optional.empty() will cause the key to be
   *         deleted.
   */
  Optional<V> combine(Input<K, V> input);
}

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

import java.util.Optional;

/**
 * Sums long values and returns Optional.empty() when the sum is zero.
 * 
 * @since 1.1.0
 */
public class SummingCombiner<K> implements ICombiner<K, Long> {

  @Override
  public Optional<Long> combine(Input<K, Long> input) {
    long sum = 0;
    for (Long l : input) {
      sum += l;
    }

    return sum == 0 ? Optional.empty() : Optional.of(sum);
  }

}

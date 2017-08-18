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

package org.apache.fluo.recipes.core.map.it;

import java.util.Iterator;
import java.util.Optional;

@Deprecated
// TODO move to CombineQueue test when removing CFM
public class WordCountCombiner implements org.apache.fluo.recipes.core.map.Combiner<String, Long> {
  @Override
  public Optional<Long> combine(String key, Iterator<Long> updates) {
    long sum = 0;

    while (updates.hasNext()) {
      sum += updates.next();
    }

    if (sum == 0) {
      return Optional.empty();
    } else {
      return Optional.of(sum);
    }
  }
}

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

package io.fluo.recipes.map;

import java.util.Iterator;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;

public class WordCountMap extends CollisionFreeMap<String, Long, Long> {

  protected WordCountMap() {
    super("wc", String.class, Long.class, Long.class, new TestSerializer(), 0l);
  }

  @Override
  protected Long combine(String key, Long currentValue, Iterator<Long> updates) {
    long sum = currentValue;
    while (updates.hasNext()) {
      sum += updates.next();
    }

    return sum;
  }

  @Override
  protected void updatingValue(TransactionBase tx, String word, Long oldValue, Long newValue) {
    // update an inverted word count index
    if (oldValue != null) {
      String oldRow = String.format("iwc:%09d:%s", oldValue, word);
      tx.delete(Bytes.of(oldRow), new Column(Bytes.EMPTY, Bytes.EMPTY));
    }

    String newRow = String.format("iwc:%09d:%s", newValue, word);
    tx.set(Bytes.of(newRow), new Column(Bytes.EMPTY, Bytes.EMPTY), Bytes.EMPTY);
  }
}

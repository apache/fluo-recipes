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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.types.TypedObserver;
import io.fluo.api.types.TypedTransactionBase;

public class DocumentObserver extends TypedObserver {

  CollisionFreeMap<String, Long, Long> wcm;

  @Override
  public void init(Context context) throws Exception {
    wcm = CollisionFreeMap.getInstance(CollisionFreeMapIT.MAP_ID, context.getAppConfiguration());
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(new Column("content", "new"), NotificationType.STRONG);
  }

  static Map<String, Long> getWordCounts(String doc) {
    Map<String, Long> wordCounts = new HashMap<>();
    String[] words = doc.split(" ");
    for (String word : words) {
      if (word.isEmpty()) {
        continue;
      }
      wordCounts.merge(word, 1L, Long::sum);
    }

    return wordCounts;
  }

  @Override
  public void process(TypedTransactionBase tx, Bytes row, Column col) {
    String newContent = tx.get().row(row).col(col).toString();
    String currentContent = tx.get().row(row).fam("content").qual("current").toString("");

    Map<String, Long> newWordCounts = getWordCounts(newContent);
    Map<String, Long> currentWordCounts = getWordCounts(currentContent);

    Map<String, Long> changes = new HashMap<>();

    for (Entry<String, Long> entry : newWordCounts.entrySet()) {
      String word = entry.getKey();
      long newCount = entry.getValue();
      long currentCount = currentWordCounts.getOrDefault(word, 0L);
      changes.put(word, newCount - currentCount);
    }

    for (Entry<String, Long> entry : currentWordCounts.entrySet()) {
      String word = entry.getKey();
      long currentCount = entry.getValue();
      if (!newWordCounts.containsKey(word)) {
        changes.put(word, -1 * currentCount);
      }
    }

    wcm.update(tx, changes);

    tx.mutate().row(row).fam("content").qual("current").set(newContent);
  }
}

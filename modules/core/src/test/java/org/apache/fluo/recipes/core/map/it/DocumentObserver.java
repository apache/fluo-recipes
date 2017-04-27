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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.StringObserver;
import org.apache.fluo.recipes.core.map.CollisionFreeMap;

public class DocumentObserver implements StringObserver {

  CollisionFreeMap<String, Long> wcm;

  DocumentObserver(CollisionFreeMap<String, Long> wcm) {
    this.wcm = wcm;
  }

  static final Column CURRENT_COL = new Column("content", "current");

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
  public void process(TransactionBase tx, String row, Column col) {
    String newContent = tx.gets(row, col);
    String currentContent = tx.gets(row, CURRENT_COL, "");

    Map<String, Long> newWordCounts = getWordCounts(newContent);
    Map<String, Long> currentWordCounts = getWordCounts(currentContent);

    Map<String, Long> changes = calculateChanges(newWordCounts, currentWordCounts);

    wcm.update(tx, changes);

    tx.set(row, CURRENT_COL, newContent);
  }

  private static Map<String, Long> calculateChanges(Map<String, Long> newCounts,
      Map<String, Long> currCounts) {
    Map<String, Long> changes = new HashMap<>();

    // guava Maps class
    MapDifference<String, Long> diffs = Maps.difference(currCounts, newCounts);

    // compute the diffs for words that changed
    changes.putAll(Maps.transformValues(diffs.entriesDiffering(), vDiff -> vDiff.rightValue()
        - vDiff.leftValue()));

    // add all new words
    changes.putAll(diffs.entriesOnlyOnRight());

    // subtract all words no longer present
    changes.putAll(Maps.transformValues(diffs.entriesOnlyOnLeft(), l -> l * -1));

    return changes;
  }
}

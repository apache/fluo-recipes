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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.types.TypedObserver;
import io.fluo.api.types.TypedTransactionBase;

public class DocumentObserver extends TypedObserver {

  WordCountMap wcm;

  @Override
  public void init(Context context) throws Exception {
    wcm = new WordCountMap();
    wcm.init(context.getAppConfiguration());
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(new Column("content", "new"), NotificationType.STRONG);
  }

  @Override
  public void process(TypedTransactionBase tx, Bytes row, Column col) {
    String newContent = tx.get().row(row).col(col).toString();
    Set<String> newWords = new HashSet<>(Arrays.asList(newContent.split(" ")));
    Set<String> currentWords =
        new HashSet<>(Arrays.asList(tx.get().row(row).fam("content").qual("current").toString("")
            .split(" ")));

    // TODO inefficient
    newWords.remove("");
    currentWords.remove("");

    Map<String, Long> changes = new HashMap<>();
    for (String word : newWords) {
      if (!currentWords.contains(word)) {
        changes.put(word, 1l);
      }
    }

    for (String word : currentWords) {
      if (!newWords.contains(word)) {
        changes.put(word, -1l);
      }
    }

    wcm.update(tx, changes);

    tx.mutate().row(row).fam("content").qual("current").set(newContent);
  }
}

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

import com.google.common.base.Optional;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;

public class WordCountObserver extends UpdateObserver<String, Long> {

  @Override
  public void updatingValues(TransactionBase tx, Iterator<Update<String, Long>> updates) {

    while (updates.hasNext()) {
      Update<String, Long> update = updates.next();

      Optional<Long> oldVal = update.getOldValue();
      Optional<Long> newVal = update.getNewValue();

      if (oldVal.isPresent()) {
        String oldRow = String.format("iwc:%09d:%s", oldVal.get(), update.getKey());
        tx.delete(Bytes.of(oldRow), new Column(Bytes.EMPTY, Bytes.EMPTY));
      }

      if (newVal.isPresent()) {
        String newRow = String.format("iwc:%09d:%s", newVal.get(), update.getKey());
        tx.set(Bytes.of(newRow), new Column(Bytes.EMPTY, Bytes.EMPTY), Bytes.EMPTY);
      }
    }
  }

}

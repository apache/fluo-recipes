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

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;

@Deprecated
// TODO move to CombineQueue test when removing CFM
public class WordCountObserver extends
    org.apache.fluo.recipes.core.map.UpdateObserver<String, Long> {

  @Override
  public void updatingValues(TransactionBase tx,
      Iterator<org.apache.fluo.recipes.core.map.Update<String, Long>> updates) {

    while (updates.hasNext()) {
      org.apache.fluo.recipes.core.map.Update<String, Long> update = updates.next();

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

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
import io.fluo.api.observer.Observer.Context;

public abstract class UpdateObserver<K, V> {
  public void init(String mapId, Context observerContext) throws Exception {}

  // TODO change to Iterator
  public abstract void updatingValues(TransactionBase tx, Iterator<Update<K, V>> updates);

  // TODO add close
}

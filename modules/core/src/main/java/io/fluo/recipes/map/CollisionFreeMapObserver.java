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

import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.observer.AbstractObserver;

public class CollisionFreeMapObserver extends AbstractObserver {

  @SuppressWarnings("rawtypes")
  private CollisionFreeMap cfm;

  public CollisionFreeMapObserver() {}

  @Override
  public void init(Context context) throws Exception {
    String clazz = context.getParameters().get("cfmClass");
    this.cfm =
        this.getClass().getClassLoader().loadClass(clazz).asSubclass(CollisionFreeMap.class)
            .newInstance();
  }

  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
    cfm.process(tx, row, col);
  }

  @Override
  public ObservedColumn getObservedColumn() {
    // TODO constants
    return new ObservedColumn(new Column("fluoRecipes", "hc:" + cfm.getId()), NotificationType.WEAK);
  }
}

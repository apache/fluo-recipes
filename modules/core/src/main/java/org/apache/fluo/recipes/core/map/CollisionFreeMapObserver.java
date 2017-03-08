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

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;

/**
 * This class is configured for use by CollisionFreeMap.configure(FluoConfiguration,
 * CollisionFreeMap.Options) . This class should never have to be used directly.
 *
 * @since 1.0.0
 * @deprecated since 1.1.0
 */
@Deprecated
public class CollisionFreeMapObserver extends AbstractObserver {

  @SuppressWarnings("rawtypes")
  private CollisionFreeMap cfm;
  private String mapId;

  public CollisionFreeMapObserver() {}

  @Override
  public void init(Context context) throws Exception {
    this.mapId = context.getObserverConfiguration().getString("mapId");
    cfm = CollisionFreeMap.getInstance(mapId, context.getAppConfiguration());
    cfm.updateObserver.init(mapId, context);
  }

  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
    cfm.process(tx, row, col);
  }

  @Override
  public ObservedColumn getObservedColumn() {
    // TODO constants
    return new ObservedColumn(new Column("fluoRecipes", "cfm:" + mapId), NotificationType.WEAK);
  }
}

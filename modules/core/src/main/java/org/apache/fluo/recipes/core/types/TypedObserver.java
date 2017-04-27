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

package org.apache.fluo.recipes.core.types;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.Observer;

/**
 * An {@link Observer} that uses a {@link TypeLayer}
 *
 * @since 1.0.0
 */
public abstract class TypedObserver implements Observer {

  private final TypeLayer tl;

  public TypedObserver() {
    tl = new TypeLayer(new StringEncoder());
  }

  public TypedObserver(TypeLayer tl) {
    this.tl = tl;
  }

  @Override
  public void process(TransactionBase tx, Bytes row, Column col) {
    process(tl.wrap(tx), row, col);
  }

  public abstract void process(TypedTransactionBase tx, Bytes row, Column col);
}

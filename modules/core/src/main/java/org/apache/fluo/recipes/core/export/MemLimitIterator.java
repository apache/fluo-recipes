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

package org.apache.fluo.recipes.core.export;

import java.util.Iterator;
import java.util.NoSuchElementException;

// This class intentionally package private.
class MemLimitIterator implements Iterator<ExportEntry> {

  private long memConsumed = 0;
  private long memLimit;
  private int extraPerKey;
  private Iterator<ExportEntry> source;

  public MemLimitIterator(Iterator<ExportEntry> input, long limit, int extraPerKey) {
    this.source = input;
    this.memLimit = limit;
    this.extraPerKey = extraPerKey;
  }

  @Override
  public boolean hasNext() {
    return memConsumed < memLimit && source.hasNext();
  }

  @Override
  public ExportEntry next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    ExportEntry ee = source.next();
    memConsumed += ee.key.length + extraPerKey + ee.value.length;
    return ee;
  }

  @Override
  public void remove() {
    source.remove();
  }
}

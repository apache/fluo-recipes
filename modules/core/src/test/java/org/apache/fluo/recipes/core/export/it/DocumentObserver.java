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

package org.apache.fluo.recipes.core.export.it;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.StringObserver;
import org.apache.fluo.recipes.core.export.ExportQueue;

public class DocumentObserver implements StringObserver {

  ExportQueue<String, RefUpdates> refExportQueue;

  private static final Column CURRENT_COL = new Column("content", "current");

  DocumentObserver(ExportQueue<String, RefUpdates> refExportQueue) {
    this.refExportQueue = refExportQueue;
  }

  @Override
  public void process(TransactionBase tx, String row, Column col) {
    String newContent = tx.gets(row, col).toString();
    Set<String> newRefs = new HashSet<>(Arrays.asList(newContent.split(" ")));
    Set<String> currentRefs =
        new HashSet<>(Arrays.asList(tx.gets(row, CURRENT_COL, "").split(" ")));

    Set<String> addedRefs = new HashSet<>(newRefs);
    addedRefs.removeAll(currentRefs);

    Set<String> deletedRefs = new HashSet<>(currentRefs);
    deletedRefs.removeAll(newRefs);

    String key = row.toString().substring(2);
    RefUpdates val = new RefUpdates(addedRefs, deletedRefs);

    refExportQueue.add(tx, key, val);

    tx.set(row, CURRENT_COL, newContent);
  }
}

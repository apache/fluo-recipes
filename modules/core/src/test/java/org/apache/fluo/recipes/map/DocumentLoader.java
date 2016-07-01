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

package org.apache.fluo.recipes.map;

import org.apache.fluo.recipes.types.TypedLoader;
import org.apache.fluo.recipes.types.TypedTransactionBase;

public class DocumentLoader extends TypedLoader {

  String docid;
  String doc;

  DocumentLoader(String docid, String doc) {
    this.docid = docid;
    this.doc = doc;
  }

  @Override
  public void load(TypedTransactionBase tx, Context context) throws Exception {
    tx.mutate().row("d:" + docid).fam("content").qual("new").set(doc);
  }
}

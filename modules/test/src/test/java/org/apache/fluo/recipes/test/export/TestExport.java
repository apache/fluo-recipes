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

package org.apache.fluo.recipes.test.export;

import java.util.Collection;
import java.util.Collections;

import org.apache.fluo.recipes.accumulo.export.AccumuloExport;
import org.apache.accumulo.core.data.Mutation;

public class TestExport implements AccumuloExport<String> {

  private String value;

  public TestExport() {}

  public TestExport(String value) {
    this.value = value;
  }

  @Override
  public Collection<Mutation> toMutations(String key, long seq) {
    Mutation m = new Mutation(key);
    m.put("cf", "cq", seq, value);
    return Collections.singletonList(m);
  }
}

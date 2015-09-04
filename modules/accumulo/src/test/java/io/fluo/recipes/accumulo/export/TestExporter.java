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

package io.fluo.recipes.accumulo.export;

import org.apache.accumulo.core.data.Mutation;

import io.fluo.recipes.accumulo.export.AccumuloExporter;
import io.fluo.recipes.serialization.StringSerializer;

public class TestExporter extends AccumuloExporter<String, String> {

  public static final String QUEUE_ID = "aeqt";

  public TestExporter() {
    super(QUEUE_ID, new StringSerializer(), new StringSerializer());
  }

  @Override
  protected Mutation convert(String key, long seq, String value) {
    Mutation m = new Mutation(key);
    m.put("cf", "cq", seq, value);
    return m;
  }
}

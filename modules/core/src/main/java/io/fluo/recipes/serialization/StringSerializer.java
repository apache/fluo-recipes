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

package io.fluo.recipes.serialization;

import com.google.common.base.Charsets;

public class StringSerializer implements SimpleSerializer<String> {

  @Override
  public String deserialize(byte[] b) {
    return new String(b, Charsets.UTF_8);
  }

  @Override
  public byte[] serialize(String e) {
    return e.getBytes(Charsets.UTF_8);
  }
}

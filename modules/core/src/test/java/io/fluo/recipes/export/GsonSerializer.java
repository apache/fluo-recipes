/*
 * Copyright 2016 Fluo authors (see AUTHORS)
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

package io.fluo.recipes.export;

import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;
import io.fluo.recipes.serialization.SimpleSerializer;
import org.apache.commons.configuration.Configuration;

public class GsonSerializer implements SimpleSerializer {

  private Gson gson = new Gson();

  @Override
  public void init(Configuration appConfig) {

  }

  @Override
  public <T> byte[] serialize(T obj) {
    return gson.toJson(obj).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public <T> T deserialize(byte[] serObj, Class<T> clazz) {
    return gson.fromJson(new String(serObj, StandardCharsets.UTF_8), clazz);
  }
}

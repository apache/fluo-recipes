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

package org.apache.fluo.recipes.core.serialization;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;

/**
 * @since 1.0.0
 */
public interface SimpleSerializer {

  /**
   * Called immediately after construction and passed Fluo application configuration.
   */
  void init(SimpleConfiguration appConfig);

  // TODO refactor to support reuse of objects and byte arrays???
  <T> byte[] serialize(T obj);

  <T> T deserialize(byte[] serObj, Class<T> clazz);

  static void setSerializer(FluoConfiguration fluoConfig,
      Class<? extends SimpleSerializer> serializerType) {
    setSerializer(fluoConfig, serializerType.getName());
  }

  static void setSerializer(FluoConfiguration fluoConfig, String serializerType) {
    fluoConfig.getAppConfiguration().setProperty("recipes.serializer", serializerType);
  }

  static SimpleSerializer getInstance(SimpleConfiguration appConfig) {
    String serType =
        appConfig.getString("recipes.serializer",
            "org.apache.fluo.recipes.kryo.KryoSimplerSerializer");
    try {
      SimpleSerializer simplerSer =
          SimpleSerializer.class.getClassLoader().loadClass(serType)
              .asSubclass(SimpleSerializer.class).newInstance();
      simplerSer.init(appConfig);
      return simplerSer;
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}

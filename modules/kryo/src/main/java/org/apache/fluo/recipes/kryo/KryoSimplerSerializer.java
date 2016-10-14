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

package org.apache.fluo.recipes.kryo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoCallback;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.google.common.base.Preconditions;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;

/***
 * @since 1.0.0
 */
public class KryoSimplerSerializer implements SimpleSerializer, Serializable {

  private static final long serialVersionUID = 1L;

  private static final String KRYO_FACTORY_PROP = "recipes.serializer.kryo.factory";
  private static Map<String, KryoPool> pools = new ConcurrentHashMap<>();
  private String factoryType = null;
  private transient KryoFactory factory = null;

  private static KryoFactory getFactory(String factoryType) {
    try {
      return KryoSimplerSerializer.class.getClassLoader().loadClass(factoryType)
          .asSubclass(KryoFactory.class).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private KryoPool getPool() {
    Preconditions.checkState(factory != null || factoryType != null, "KryFactory not initialized");
    if (factory == null) {
      return pools.computeIfAbsent(factoryType, ft -> new KryoPool.Builder(getFactory(ft))
          .softReferences().build());
    } else {
      return pools.computeIfAbsent(factory.getClass().getName(),
          ft -> new KryoPool.Builder(factory).softReferences().build());
    }
  }

  /**
   * @since 1.0.0
   */
  public static class DefaultFactory implements KryoFactory {
    @Override
    public Kryo create() {
      Kryo kryo = new Kryo();
      return kryo;
    }
  }

  @Override
  public <T> byte[] serialize(T obj) {
    return getPool().run(new KryoCallback<byte[]>() {
      @Override
      public byte[] execute(Kryo kryo) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeClassAndObject(output, obj);
        output.close();
        return baos.toByteArray();
      }
    });
  }

  @Override
  public <T> T deserialize(byte[] serObj, Class<T> clazz) {
    return getPool().run(new KryoCallback<T>() {
      @Override
      public T execute(Kryo kryo) {
        ByteArrayInputStream bais = new ByteArrayInputStream(serObj);
        Input input = new Input(bais);
        return clazz.cast(kryo.readClassAndObject(input));
      }
    });
  }

  @Override
  public void init(SimpleConfiguration appConfig) {
    Preconditions.checkArgument(factory == null && factoryType == null, "Already initialized");
    factoryType = appConfig.getString(KRYO_FACTORY_PROP, DefaultFactory.class.getName());
  }

  public KryoSimplerSerializer() {}

  /**
   * Can call this method to create a serializer w/o calling {@link #init(SimpleConfiguration)}
   */
  public KryoSimplerSerializer(KryoFactory factory) {
    factoryType = factory.getClass().getName();
    this.factory = factory;
  }

  /**
   * Call this to configure a KryoFactory type before initializing Fluo.
   */
  public static void setKryoFactory(FluoConfiguration config, String factoryType) {
    config.getAppConfiguration().setProperty(KRYO_FACTORY_PROP, factoryType);
  }

  /**
   * Call this to configure a KryoFactory type before initializing Fluo.
   */
  public static void setKryoFactory(FluoConfiguration config,
      Class<? extends KryoFactory> factoryType) {
    config.getAppConfiguration().setProperty(KRYO_FACTORY_PROP, factoryType.getName());
  }
}

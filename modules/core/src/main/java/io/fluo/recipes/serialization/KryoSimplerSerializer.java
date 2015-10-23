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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoCallback;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import io.fluo.api.config.FluoConfiguration;
import org.apache.commons.configuration.Configuration;

// TODO maybe put in own module so that fluo-recipes does not depend on Kryo
public class KryoSimplerSerializer implements SimpleSerializer {

  private static final String KRYO_FACTORY_PROP = "recipes.serializer.kryo.factory";
  private static Map<String, KryoPool> pools = new ConcurrentHashMap<>();
  private String factoryType = DefaultFactory.class.getName();

  private static KryoFactory getFactory(String factoryType) {
    try {
      return KryoSimplerSerializer.class.getClassLoader().loadClass(factoryType)
          .asSubclass(KryoFactory.class).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private KryoPool getPool() {
    return pools.computeIfAbsent(factoryType, ft -> new KryoPool.Builder(getFactory(ft))
        .softReferences().build());
  }

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
  public void init(Configuration appConfig) {
    factoryType = appConfig.getString(KRYO_FACTORY_PROP, DefaultFactory.class.getName());

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

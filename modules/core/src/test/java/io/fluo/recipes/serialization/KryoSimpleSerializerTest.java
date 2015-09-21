/*
 * Copyright 2015 Fluo authors (see AUTHORS)
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

import io.fluo.accumulo.data.MutableBytes;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import org.junit.Assert;
import org.junit.Test;

public class KryoSimpleSerializerTest {

  @Test
  public void testColumn() {
    SimpleSerializer serializer = new KryoSimplerSerializer();
    Column before = new Column("a", "b");
    byte[] barray = serializer.serialize(before);
    Column after = serializer.deserialize(barray, Column.class);
    Assert.assertEquals(before, after);
  }

  @Test
  public void testBytes() {
    SimpleSerializer serializer = new KryoSimplerSerializer();
    Bytes before = Bytes.of("test");
    byte[] barray = serializer.serialize(before);
    Bytes after = serializer.deserialize(barray, Bytes.class);
    Assert.assertEquals(before, after);
  }
}

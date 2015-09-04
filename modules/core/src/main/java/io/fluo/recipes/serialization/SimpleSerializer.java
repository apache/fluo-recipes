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

// TODO is there something more standard that can be used? Do not want to use Hadoop writable and
// then depend on hadoop
// TODO avoid object array allocations for repeated use?
public interface SimpleSerializer<T> {
  // TODO use data input/output
  T deserialize(byte[] b);

  byte[] serialize(T e);
}

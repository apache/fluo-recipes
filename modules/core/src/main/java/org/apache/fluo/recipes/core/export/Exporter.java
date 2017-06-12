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

package org.apache.fluo.recipes.core.export;

import java.util.Iterator;

import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.observer.Observer;

/**
 * @since 1.0.0
 * @deprecated since 1.1.0 replaced by {@link org.apache.fluo.recipes.core.export.function.Exporter}
 */
@Deprecated
public abstract class Exporter<K, V> {

  public interface Context {
    String getQueueId();

    SimpleConfiguration getExporterConfiguration();

    Observer.Context getObserverContext();
  }

  public void init(Exporter.Context exporterContext) throws Exception {}

  /**
   * Must be able to handle same key being exported multiple times and key being exported out of
   * order. The sequence number is meant to help with this.
   *
   * <p>
   * If multiple export entries with the same key are passed in, then the entries with the same key
   * will be consecutive and in ascending sequence order.
   *
   * <p>
   * If the call to process exports is unexpectedly terminated, it will be called again later with
   * at least the same data. For example suppose an exporter was passed the following entries.
   *
   * <ul>
   * <li>key=0 sequence=9 value=abc
   * <li>key=1 sequence=13 value=d
   * <li>key=1 sequence=17 value=e
   * <li>key=1 sequence=23 value=f
   * <li>key=2 sequence=19 value=x
   * </ul>
   *
   * <p>
   * Assume the exporter exports some of these and then fails before completing all of them. The
   * next time its called it will be passed what it saw before, but it could also be passed more.
   *
   * <ul>
   * <li>key=0 sequence=9 value=abc
   * <li>key=1 sequence=13 value=d
   * <li>key=1 sequence=17 value=e
   * <li>key=1 sequence=23 value=f
   * <li>key=1 sequence=29 value=g
   * <li>key=2 sequence=19 value=x
   * <li>key=2 sequence=77 value=y
   * </ul>
   *
   */
  protected abstract void processExports(Iterator<SequencedExport<K, V>> exports);
}

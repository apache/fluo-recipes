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

package io.fluo.recipes.test.export;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import io.fluo.recipes.test.FluoITHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class ExportITBase {

  static FluoITHelper fluoIT;
  static AtomicInteger exportTableCounter = new AtomicInteger(1);

  public abstract void setupExporter() throws Exception;

  @BeforeClass
  public static void setupIT() {
    fluoIT = new FluoITHelper(Paths.get("target/miniAccumulo"));
  }

  @AfterClass
  public static void teardownIT() {
    fluoIT.close();
  }

  @Before
  public void setupTest() throws Exception {
    setupExporter();
    fluoIT.setupFluo();
  }

  @After
  public void teardownTest() {
    fluoIT.teardownFluo();
  }
}

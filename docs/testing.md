<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Testing

Fluo includes MiniFluo which makes it possible to write an integeration test that
runs against a real Fluo instance.  Fluo Recipes provides the following utility
code for writing an integration test.

 * [FluoITHelper][1] A class with utility methods for comparing expected data with whats in Fluo.
 * [AccumuloExportITBase][2] A base class for writing an integration test that exports data from Fluo to an external Accumulo table.

[1]: ../modules/test/src/main/java/org/apache/fluo/recipes/test/FluoITHelper.java
[2]: ../modules/test/src/main/java/org/apache/fluo/recipes/test/AccumuloExportITBase.java

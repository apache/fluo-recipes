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
# Apache Spark helper code

Fluo Recipes has some helper code for [Apache Spark][spark].  Most of the helper code is currently
related to bulk importing data into Accumulo.  This is useful for initializing a new Fluo table with
historical data via Spark.  The Spark helper code is found at
[modules/spark/src/main/java/org/apache/fluo/recipes/spark/][sdir].

For information on using Spark to load data into Fluo, check out this [blog post][blog].

If you know of other Spark+Fluo integration code that would be useful, then please consider [opening
an issue](https://github.com/apache/fluo-recipes/issues/new).

[spark]: https://spark.apache.org
[sdir]: ../modules/spark/src/main/java/org/apache/fluo/recipes/spark/
[blog]: https://fluo.apache.org/blog/2016/12/22/spark-load/


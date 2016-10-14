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
# RecordingTransaction recipe

A `RecordingTransaction` is an implementation of `Transaction` that logs all transaction operations
(i.e GET, SET, or DELETE) to a `TxLog` object for later uses such as exporting data.  The code below
shows how a RecordingTransaction is created by wrapping a Transaction object:

```java
RecordingTransactionBase rtx = RecordingTransactionBase.wrap(tx);
```

A predicate function can be passed to wrap method to select which log entries to record.  The code
below only records log entries whose column family is `meta`:

```java
RecordingTransactionBase rtx = RecordingTransactionBase.wrap(tx,
                               le -> le.getColumn().getFamily().toString().equals("meta"));
```

After creating a RecordingTransaction, users can use it as they would use a Transaction object.

```java
Bytes value = rtx.get(Bytes.of("r1"), new Column("cf1", "cq1"));
```

While SET or DELETE operations are always recorded to the log, GET operations are only recorded if a
value was found at the requested row/column.  Also, if a GET method returns an iterator, only the GET
operations that are retrieved from the iterator are logged.  GET operations are logged as they are
necessary if you want to determine the changes made by the transaction.
 
When you are done operating on the transaction, you can retrieve the TxLog using the following code:

```java
TxLog myTxLog = rtx.getTxLog()
```

Below is example code of how a RecordingTransaction can be used in an observer to record all operations
performed by the transaction in a TxLog.  In this example, a GET (if data exists) and SET operation
will be logged.  This TxLog can be added to an export queue and later used to export updates from 
Fluo.

```java
public class MyObserver extends AbstractObserver {

    private static final TYPEL = new TypeLayer(new StringEncoder());
    
    private ExportQueue<Bytes, TxLog> exportQueue;

    @Override
    public void process(TransactionBase tx, Bytes row, Column col) {

        // create recording transaction (rtx)
        RecordingTransactionBase rtx = RecordingTransactionBase.wrap(tx);
        
        // use rtx to create a typed transaction & perform operations
        TypedTransactionBase ttx = TYPEL.wrap(rtx);
        int count = ttx.get().row(row).fam("meta").qual("counter1").toInteger(0);
        ttx.mutate().row(row).fam("meta").qual("counter1").set(count+1);
        
        // when finished performing operations, retrieve transaction log
        TxLog txLog = rtx.getTxLog()

        // add txLog to exportQueue if not empty
        if (!txLog.isEmpty()) {
          //do not pass rtx to exportQueue.add()
          exportQueue.add(tx, row, txLog)
        }
    }
}
```

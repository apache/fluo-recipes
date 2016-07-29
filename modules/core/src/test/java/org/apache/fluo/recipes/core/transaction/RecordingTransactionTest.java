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

package org.apache.fluo.recipes.core.transaction;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import com.google.common.collect.Iterators;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.client.scanner.RowScannerBuilder;
import org.apache.fluo.api.client.scanner.ScannerBuilder;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.recipes.core.types.StringEncoder;
import org.apache.fluo.recipes.core.types.TypeLayer;
import org.apache.fluo.recipes.core.types.TypedTransaction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class RecordingTransactionTest {

  private Transaction tx;
  private RecordingTransaction rtx;
  private TypeLayer tl = new TypeLayer(new StringEncoder());

  @Before
  public void setUp() {
    tx = mock(Transaction.class);
    rtx = RecordingTransaction.wrap(tx);
  }

  @Test
  public void testTx() {
    rtx.set(Bytes.of("r1"), new Column("cf1"), Bytes.of("v1"));
    rtx.set(Bytes.of("r2"), new Column("cf2", "cq2"), Bytes.of("v2"));
    rtx.delete(Bytes.of("r3"), new Column("cf3"));
    expect(tx.get(Bytes.of("r4"), new Column("cf4"))).andReturn(Bytes.of("v4"));
    replay(tx);
    rtx.get(Bytes.of("r4"), new Column("cf4"));

    List<LogEntry> entries = rtx.getTxLog().getLogEntries();
    Assert.assertEquals(4, entries.size());
    Assert.assertEquals("LogEntry{op=SET, row=r1, col=cf1  , value=v1}", entries.get(0).toString());
    Assert.assertEquals("LogEntry{op=SET, row=r2, col=cf2 cq2 , value=v2}", entries.get(1)
        .toString());
    Assert
        .assertEquals("LogEntry{op=DELETE, row=r3, col=cf3  , value=}", entries.get(2).toString());
    Assert.assertEquals("LogEntry{op=GET, row=r4, col=cf4  , value=v4}", entries.get(3).toString());
    Assert.assertEquals("{r4 cf4  =v4}", rtx.getTxLog().getOperationMap(LogEntry.Operation.GET)
        .toString());
    Assert.assertEquals("{r1 cf1  =v1, r2 cf2 cq2 =v2}", new TreeMap<>(rtx.getTxLog()
        .getOperationMap(LogEntry.Operation.SET)).toString());
    Assert.assertEquals("{r3 cf3  =}", rtx.getTxLog().getOperationMap(LogEntry.Operation.DELETE)
        .toString());
  }

  @Test
  public void testTypedTx() {
    TypedTransaction ttx = tl.wrap(rtx);
    ttx.mutate().row("r5").fam("cf5").qual("cq5").set("1");
    ttx.mutate().row("r6").fam("cf6").qual("cq6").set("1");
    List<LogEntry> entries = rtx.getTxLog().getLogEntries();
    Assert.assertEquals(2, entries.size());
    Assert.assertEquals("LogEntry{op=SET, row=r5, col=cf5 cq5 , value=1}", entries.get(0)
        .toString());
    Assert.assertEquals("LogEntry{op=SET, row=r6, col=cf6 cq6 , value=1}", entries.get(1)
        .toString());
  }

  @Test
  public void testFilter() {
    rtx = RecordingTransaction.wrap(tx, le -> le.getColumn().getFamily().toString().equals("cfa"));
    TypedTransaction ttx = tl.wrap(rtx);
    ttx.mutate().row("r1").fam("cfa").qual("cq1").set("1");
    ttx.mutate().row("r2").fam("cfb").qual("cq2").set("2");
    ttx.mutate().row("r3").fam("cfa").qual("cq3").set("3");
    List<LogEntry> entries = rtx.getTxLog().getLogEntries();
    Assert.assertEquals(2, entries.size());
    Assert.assertEquals("LogEntry{op=SET, row=r1, col=cfa cq1 , value=1}", entries.get(0)
        .toString());
    Assert.assertEquals("LogEntry{op=SET, row=r3, col=cfa cq3 , value=3}", entries.get(1)
        .toString());
  }

  @Test
  public void testClose() {
    tx.close();
    replay(tx);
    rtx.close();
    verify(tx);
  }

  @Test
  public void testCommit() {
    tx.commit();
    replay(tx);
    rtx.commit();
    verify(tx);
  }

  @Test
  public void testDelete() {
    tx.delete(Bytes.of("r"), Column.EMPTY);
    replay(tx);
    rtx.delete(Bytes.of("r"), Column.EMPTY);
    verify(tx);
  }

  @Test
  public void testGet() {
    expect(tx.get(Bytes.of("r"), Column.EMPTY)).andReturn(Bytes.of("v"));
    replay(tx);
    Assert.assertEquals(Bytes.of("v"), rtx.get(Bytes.of("r"), Column.EMPTY));
    verify(tx);
  }

  @Test
  public void testGetColumns() {
    expect(tx.get(Bytes.of("r"), Collections.emptySet())).andReturn(Collections.emptyMap());
    replay(tx);
    Assert.assertEquals(Collections.emptyMap(), rtx.get(Bytes.of("r"), Collections.emptySet()));
    verify(tx);
  }

  @Test
  public void testGetRows() {
    expect(tx.get(Collections.emptyList(), Collections.emptySet())).andReturn(
        Collections.emptyMap());
    replay(tx);
    Assert.assertEquals(Collections.emptyMap(),
        rtx.get(Collections.emptyList(), Collections.emptySet()));
    verify(tx);
  }

  @Test
  public void testGetScanIter() {
    ScannerBuilder sb = mock(ScannerBuilder.class);
    expect(sb.build()).andReturn(new CellScanner() {
      @Override
      public Iterator<RowColumnValue> iterator() {
        return Iterators
            .singletonIterator(new RowColumnValue("r7", new Column("cf7", "cq7"), "v7"));
      }
    });

    expect(tx.scanner()).andReturn(sb);

    replay(tx, sb);

    Iterator<RowColumnValue> iter = rtx.scanner().build().iterator();
    Assert.assertTrue(rtx.getTxLog().isEmpty());
    Assert.assertTrue(iter.hasNext());
    Assert.assertEquals(new RowColumnValue("r7", new Column("cf7", "cq7"), "v7"), iter.next());
    Assert.assertFalse(rtx.getTxLog().isEmpty());
    List<LogEntry> entries = rtx.getTxLog().getLogEntries();
    Assert.assertEquals(1, entries.size());
    Assert.assertEquals("LogEntry{op=GET, row=r7, col=cf7 cq7 , value=v7}", entries.get(0)
        .toString());

    verify(tx, sb);
  }

  @Test
  public void testGetRowScanner() {
    ColumnScanner cs = mock(ColumnScanner.class);
    RowScanner rs = mock(RowScanner.class);
    RowScannerBuilder rsb = mock(RowScannerBuilder.class);
    ScannerBuilder sb = mock(ScannerBuilder.class);

    expect(cs.getRow()).andReturn(Bytes.of("r7")).times(2);
    expect(cs.iterator()).andReturn(
        Iterators.singletonIterator(new ColumnValue(new Column("cf7", "cq7"), "v7")));
    expect(rs.iterator()).andReturn(Iterators.singletonIterator(cs));
    expect(rsb.build()).andReturn(rs);
    expect(sb.byRow()).andReturn(rsb);
    expect(tx.scanner()).andReturn(sb);

    replay(tx, sb, rsb, rs, cs);

    Iterator<ColumnScanner> riter = rtx.scanner().byRow().build().iterator();
    Assert.assertTrue(riter.hasNext());
    ColumnScanner cscanner = riter.next();
    Assert.assertEquals(Bytes.of("r7"), cscanner.getRow());
    Iterator<ColumnValue> citer = cscanner.iterator();
    Assert.assertTrue(citer.hasNext());
    Assert.assertTrue(rtx.getTxLog().isEmpty());
    Assert.assertEquals(new ColumnValue(new Column("cf7", "cq7"), "v7"), citer.next());
    List<LogEntry> entries = rtx.getTxLog().getLogEntries();
    Assert.assertEquals(1, entries.size());
    Assert.assertEquals("LogEntry{op=GET, row=r7, col=cf7 cq7 , value=v7}", entries.get(0)
        .toString());

    verify(tx, sb, rsb, rs, cs);
  }

  @Test
  public void testGetTimestamp() {
    expect(tx.getStartTimestamp()).andReturn(5L);
    replay(tx);
    Assert.assertEquals(5L, rtx.getStartTimestamp());
    verify(tx);
  }
}

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

package io.fluo.recipes.transaction;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.fluo.api.client.Transaction;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.api.types.TypedTransaction;
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
    rtx.get(Bytes.of("r4"), new Column("cf4"));

    List<TxLog.LogEntry> entries = rtx.getTxLog().getLogEntries();
    Assert.assertEquals(3, entries.size());
    Assert.assertEquals("LogEntry{type=SET, row=r1, col=cf1  , value=v1}", entries.get(0)
        .toString());
    Assert.assertEquals("LogEntry{type=SET, row=r2, col=cf2 cq2 , value=v2}", entries.get(1)
        .toString());
    Assert.assertEquals("LogEntry{type=DELETE, row=r3, col=cf3  , value=}", entries.get(2)
        .toString());
  }

  @Test
  public void testTypedTx() {
    TypeLayer tl = new TypeLayer(new StringEncoder());
    TypedTransaction ttx = tl.wrap(rtx);
    ttx.mutate().row("r5").fam("cf5").qual("cq5").set("1");
    ttx.mutate().row("r6").fam("cf6").qual("cq6").set("1");
    List<TxLog.LogEntry> entries = rtx.getTxLog().getLogEntries();
    Assert.assertEquals(2, entries.size());
    Assert.assertEquals("LogEntry{type=SET, row=r5, col=cf5 cq5 , value=1}", entries.get(0)
        .toString());
    Assert.assertEquals("LogEntry{type=SET, row=r6, col=cf6 cq6 , value=1}", entries.get(1)
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
    expect(tx.get(Bytes.of("r"), Collections.EMPTY_SET)).andReturn(Collections.EMPTY_MAP);
    replay(tx);
    Assert.assertEquals(Collections.EMPTY_MAP, rtx.get(Bytes.of("r"), Collections.EMPTY_SET));
    verify(tx);
  }

  @Test
  public void testGetRows() {
    expect(tx.get(Collections.EMPTY_LIST, Collections.EMPTY_SET)).andReturn(Collections.EMPTY_MAP);
    replay(tx);
    Assert.assertEquals(Collections.EMPTY_MAP, rtx.get(Collections.EMPTY_LIST, Collections.EMPTY_SET));
    verify(tx);
  }

  @Test
  public void testGetScan() {
    ScannerConfiguration scanConfig = new ScannerConfiguration();
    expect(tx.get(scanConfig)).andReturn(null);
    replay(tx);
    Assert.assertNull(rtx.get(scanConfig));
    verify(tx);
  }
}

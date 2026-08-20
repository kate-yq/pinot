/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.broker.broker.helix;

import java.util.List;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.model.Message;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


public class BrokerResourceOnlineOfflineStateModelTest {
  private static final String TABLE_NAME = "testTable_OFFLINE";

  @Test
  public void testPreRoutingRemovalBeforeOfflineTransition() {
    TestContext context = new TestContext(false);
    context.transition(false);

    InOrder inOrder = inOrder(context._listener, context._routingManager, context._queryQuotaManager);
    inOrder.verify(context._listener).preRoutingRemoval(TABLE_NAME);
    inOrder.verify(context._routingManager).removeRouting(TABLE_NAME);
    inOrder.verify(context._queryQuotaManager).dropTableQueryQuota(TABLE_NAME);
  }

  @Test
  public void testPreRoutingRemovalBeforeDroppedTransition() {
    TestContext context = new TestContext(false);
    context.transition(true);

    InOrder inOrder = inOrder(context._listener, context._routingManager);
    inOrder.verify(context._listener).preRoutingRemoval(TABLE_NAME);
    inOrder.verify(context._routingManager).removeRouting(TABLE_NAME);
  }

  @Test
  public void testPreRoutingRemovalForLogicalTable() {
    TestContext context = new TestContext(true);
    context.transition(false);

    InOrder inOrder = inOrder(context._listener, context._routingManager);
    inOrder.verify(context._listener).preRoutingRemoval(TABLE_NAME);
    inOrder.verify(context._routingManager).removeRoutingForLogicalTable(TABLE_NAME);
    verify(context._routingManager, never()).removeRouting(TABLE_NAME);
  }

  @Test
  public void testPreRoutingRemovalFailureAbortsRemoval() {
    TestContext context = new TestContext(false);
    RuntimeException failure = new RuntimeException("failure");
    doThrow(failure).when(context._listener).preRoutingRemoval(TABLE_NAME);

    RuntimeException thrown = Assert.expectThrows(RuntimeException.class, () -> context.transition(false));
    Assert.assertSame(thrown, failure);
    verifyNoInteractions(context._routingManager, context._queryQuotaManager);
  }

  @Test
  public void testDroppedFromOfflineDoesNotInvokeListener() {
    TestContext context = new TestContext(false);
    context._stateModel.onBecomeDroppedFromOffline(context._message, null);

    verifyNoInteractions(context._listener, context._routingManager, context._queryQuotaManager);
  }

  private static class TestContext {
    private final BrokerRoutingLifecycleListener _listener = mock(BrokerRoutingLifecycleListener.class);
    private final BrokerRoutingManager _routingManager = mock(BrokerRoutingManager.class);
    private final HelixExternalViewBasedQueryQuotaManager _queryQuotaManager =
        mock(HelixExternalViewBasedQueryQuotaManager.class);
    private final Message _message = mock(Message.class);
    private final BrokerResourceOnlineOfflineStateModelFactory.BrokerResourceOnlineOfflineStateModel _stateModel;
    private final ZkHelixPropertyStore<ZNRecord> _propertyStore = mock(ZkHelixPropertyStore.class);
    private final boolean _logicalTable;

    private TestContext(boolean logicalTable) {
      _logicalTable = logicalTable;
      when(_message.getPartitionName()).thenReturn(TABLE_NAME);
      BrokerResourceOnlineOfflineStateModelFactory factory =
          new BrokerResourceOnlineOfflineStateModelFactory(_propertyStore, mock(HelixDataAccessor.class),
              _routingManager, _queryQuotaManager, List.of(_listener));
      _stateModel = (BrokerResourceOnlineOfflineStateModelFactory.BrokerResourceOnlineOfflineStateModel)
          factory.createNewStateModel("brokerResource", TABLE_NAME);
    }

    private void transition(boolean dropped) {
      try (MockedStatic<ZKMetadataProvider> metadataProvider = mockStatic(ZKMetadataProvider.class);
          MockedStatic<QueryRewriterFactory> queryRewriterFactory = mockStatic(QueryRewriterFactory.class)) {
        metadataProvider.when(() -> ZKMetadataProvider.isLogicalTableExists(_propertyStore, TABLE_NAME))
            .thenReturn(_logicalTable);
        metadataProvider.when(() -> ZKMetadataProvider.getTableConfig(_propertyStore, TABLE_NAME))
            .thenReturn(mock(TableConfig.class));
        queryRewriterFactory.when(QueryRewriterFactory::getQueryRewriters).thenReturn(List.of());
        if (dropped) {
          _stateModel.onBecomeDroppedFromOnline(_message, null);
        } else {
          _stateModel.onBecomeOfflineFromOnline(_message, null);
        }
      }
    }
  }
}

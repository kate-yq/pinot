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

import java.util.Collections;
import java.util.List;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriter;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.apache.pinot.sql.parsers.rewriter.TableQueryRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;


/**
 * Broker Resource layer state model to take over how to operate on:
 * Adding an external view to routing table.
 *
 *
 */
public class BrokerResourceOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerResourceOnlineOfflineStateModelFactory.class);

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final HelixDataAccessor _helixDataAccessor;
  private final BrokerRoutingManager _routingManager;
  private final HelixExternalViewBasedQueryQuotaManager _queryQuotaManager;
  private final List<BrokerRoutingLifecycleListener> _brokerRoutingLifecycleListeners;

  public BrokerResourceOnlineOfflineStateModelFactory(ZkHelixPropertyStore<ZNRecord> propertyStore,
      HelixDataAccessor helixDataAccessor, BrokerRoutingManager routingManager,
      HelixExternalViewBasedQueryQuotaManager queryQuotaManager) {
    this(propertyStore, helixDataAccessor, routingManager, queryQuotaManager, Collections.emptyList());
  }

  public BrokerResourceOnlineOfflineStateModelFactory(ZkHelixPropertyStore<ZNRecord> propertyStore,
      HelixDataAccessor helixDataAccessor, BrokerRoutingManager routingManager,
      HelixExternalViewBasedQueryQuotaManager queryQuotaManager,
      List<BrokerRoutingLifecycleListener> brokerRoutingLifecycleListeners) {
    _helixDataAccessor = helixDataAccessor;
    _propertyStore = propertyStore;
    _routingManager = routingManager;
    _queryQuotaManager = queryQuotaManager;
    _brokerRoutingLifecycleListeners = List.copyOf(brokerRoutingLifecycleListeners);
  }

  public static String getStateModelDef() {
    return "BrokerResourceOnlineOfflineStateModel";
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    return new BrokerResourceOnlineOfflineStateModel();
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'DROPPED'}", initialState = "OFFLINE")
  public class BrokerResourceOnlineOfflineStateModel extends StateModel {

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      String physicalOrLogicalTable = message.getPartitionName();
      LOGGER.info("Processing transition from OFFLINE to ONLINE for table: {}", physicalOrLogicalTable);
      try {
        if (ZKMetadataProvider.isLogicalTableExists(_propertyStore, physicalOrLogicalTable)) {
          _routingManager.buildRoutingForLogicalTable(physicalOrLogicalTable);
          _queryQuotaManager.initOrUpdateLogicalTableQueryQuota(physicalOrLogicalTable);
        } else {
          _routingManager.buildRouting(physicalOrLogicalTable);
          TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, physicalOrLogicalTable);
          _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig,
              _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().externalView(BROKER_RESOURCE_INSTANCE)));
          _queryQuotaManager.createDatabaseRateLimiter(
              DatabaseUtils.extractDatabaseFromFullyQualifiedTableName(physicalOrLogicalTable));
          List<QueryRewriter> rewriters = QueryRewriterFactory.getQueryRewriters();
          for (QueryRewriter rewriter : rewriters) {
            if (rewriter instanceof TableQueryRewriter) {
              ((TableQueryRewriter) rewriter).registerTable(tableConfig);
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error("Caught exception while processing transition from OFFLINE to ONLINE for table: {}",
            physicalOrLogicalTable, e);
        throw e;
      }
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      String physicalOrLogicalTable = message.getPartitionName();
      LOGGER.info("Processing transition from ONLINE to OFFLINE for table: {}", physicalOrLogicalTable);
      try {
        removeRouting(physicalOrLogicalTable);
      } catch (Exception e) {
        LOGGER.error("Caught exception while processing transition from ONLINE to OFFLINE for table: {}",
            physicalOrLogicalTable, e);
        throw e;
      }
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOGGER.info("Transition from OFFLINE to DROPPED is no-op for table: {}", message.getPartitionName());
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      String physicalOrLogicalTable = message.getPartitionName();
      LOGGER.info("Processing transition from ONLINE to DROPPED for table: {}", physicalOrLogicalTable);
      try {
        removeRouting(physicalOrLogicalTable);
      } catch (Exception e) {
        LOGGER.error("Caught exception while processing transition from ONLINE to DROPPED for table: {}",
            physicalOrLogicalTable, e);
        throw e;
      }
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      LOGGER.info("Transition from ERROR to OFFLINE is no-op for table: {}", message.getPartitionName());
    }

    private void removeRouting(String physicalOrLogicalTable) {
      for (BrokerRoutingLifecycleListener brokerRoutingLifecycleListener : _brokerRoutingLifecycleListeners) {
        brokerRoutingLifecycleListener.preRoutingRemoval(physicalOrLogicalTable);
      }
      if (ZKMetadataProvider.isLogicalTableExists(_propertyStore, physicalOrLogicalTable)) {
        _routingManager.removeRoutingForLogicalTable(physicalOrLogicalTable);
        _queryQuotaManager.dropTableQueryQuota(physicalOrLogicalTable);
      } else {
        _routingManager.removeRouting(physicalOrLogicalTable);
        _queryQuotaManager.dropTableQueryQuota(physicalOrLogicalTable);
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, physicalOrLogicalTable);
        List<QueryRewriter> rewriters = QueryRewriterFactory.getQueryRewriters();
        for (QueryRewriter rewriter : rewriters) {
          if (rewriter instanceof TableQueryRewriter) {
            ((TableQueryRewriter) rewriter).deregisterTable(tableConfig);
          }
        }
      }
    }
  }
}

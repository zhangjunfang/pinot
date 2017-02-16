/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import javax.annotation.Nullable;


public class PinotSegmentRebalancer extends PinotZKChanger {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRebalancer.class);
  static final String rebalanceTableCmd = "rebalanceTable";
  static final String rebalanceTenantCmd = "rebalanceTenant";

  private boolean dryRun = true;

  public PinotSegmentRebalancer(String zkAddress, String clusterName, boolean dryRun) {
    super(zkAddress, clusterName);
    this.dryRun = dryRun;
  }

  /**
   * return true if IdealState = ExternalView
   * @return
   */
  public int isStable(String tableName) {
    IdealState idealState = helixAdmin.getResourceIdealState(clusterName, tableName);
    ExternalView externalView = helixAdmin.getResourceExternalView(clusterName, tableName);
    Map<String, Map<String, String>> mapFieldsIS = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> mapFieldsEV = externalView.getRecord().getMapFields();
    int numDiff = 0;
    for (String segment : mapFieldsIS.keySet()) {
      Map<String, String> mapIS = mapFieldsIS.get(segment);
      Map<String, String> mapEV = mapFieldsEV.get(segment);

      for (String server : mapIS.keySet()) {
        String state = mapIS.get(server);
        if (mapEV == null || mapEV.get(server) == null || !mapEV.get(server).equals(state)) {
          LOGGER.info("Mismatch: segment" + segment + " server:" + server + " state:" + state);
          numDiff = numDiff + 1;
        }
      }
    }
    return numDiff;
  }

  /**
   * rebalances all tables for the tenant
   * @param tenantName
   */
  public void rebalanceTenantTables(String tenantName) throws Exception {
    String tableConfigPath = "/CONFIGS/TABLE";
    List<Stat> stats = new ArrayList<>();
    List<ZNRecord> tableConfigs = propertyStore.getChildren(tableConfigPath, stats, 0);
    String rawTenantName = tenantName.replaceAll("_OFFLINE", "").replace("_REALTIME", "");
    int nRebalances = 0;
    for (ZNRecord znRecord : tableConfigs) {
      AbstractTableConfig tableConfig;
      try {
        tableConfig = AbstractTableConfig.fromZnRecord(znRecord);
      } catch (Exception e) {
        LOGGER.warn("Failed to parse table configuration for ZnRecord id: {}. Skipping", znRecord.getId());
        continue;
      }
      if (tableConfig.getTenantConfig().getServer().equals(rawTenantName)) {
        LOGGER.info(tableConfig.getTableName() + ":" + tableConfig.getTenantConfig().getServer());
        nRebalances++;
        rebalanceTable(tableConfig.getTableName(), tenantName);
      }
    }
    if (nRebalances == 0) {
      LOGGER.info("No tables found for tenant " + tenantName);
    }
  }

  /**
   * Rebalances a table
   * @param tableName
   * @throws Exception
   */
  public void rebalanceTable(String tableName) throws Exception {
    String tableConfigPath = "/CONFIGS/TABLE/" + tableName;
    Stat stat = new Stat();
    ZNRecord znRecord = propertyStore.get(tableConfigPath, stat, 0);
    AbstractTableConfig tableConfig = AbstractTableConfig.fromZnRecord(znRecord);
    String tenantName = tableConfig.getTenantConfig().getServer().replaceAll(TableType.OFFLINE.toString(), "")
        .replace(TableType.OFFLINE.toString(), "");
    rebalanceTable(tableName, tenantName);
  }

  /**
   * Rebalances a table within a tenant
   * @param tableName
   * @param tenantName
   * @throws Exception
   */
  public void rebalanceTable(String tableName, String tenantName) throws Exception {

    final TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (!tableType.equals(TableType.OFFLINE)) {
      // Rebalancing works for offline tables, not any other.
      LOGGER.warn("Don't know how to rebalance table " + tableName);
      return;
    }
    IdealState currentIdealState = helixAdmin.getResourceIdealState(clusterName, tableName);
    List<String> partitions = Lists.newArrayList(currentIdealState.getPartitionSet());
    LinkedHashMap<String, Integer> states = new LinkedHashMap<>();
    int numReplicasInIdealState = Integer.parseInt(currentIdealState.getReplicas());
    final AbstractTableConfig offlineTableConfig =
        ZKMetadataProvider.getOfflineTableConfig(propertyStore, tableName);
    final int numReplicasInTableConfig = Integer.parseInt(offlineTableConfig.getValidationConfig().getReplication());

    final int targetNumReplicas = numReplicasInTableConfig;
    if (numReplicasInTableConfig < numReplicasInIdealState) {
      // AutoRebalanceStrategy,computePartitionAssignment works correctly if we increase the number of partitions,
      // but not if we decrease it. We need to use the PinotNumReplicaChanger to reduce the number of replicas.
      LOGGER.info("You first need to reduce the number of replicas from {} to {} for table {}. Use the ChangeNumReplicas command",
          numReplicasInIdealState, numReplicasInTableConfig, tableName);
      return;
    }

    states.put("OFFLINE", 0);
    states.put("ONLINE", targetNumReplicas);
    Map<String, Map<String, String>> mapFields = currentIdealState.getRecord().getMapFields();
    Set<String> currentHosts = new HashSet<>();
    for (String segment : mapFields.keySet()) {
      currentHosts.addAll(mapFields.get(segment).keySet());
    }
    AutoRebalanceStrategy rebalanceStrategy = new AutoRebalanceStrategy(tableName, partitions, states);

    TableNameBuilder builder = new TableNameBuilder(tableType);
    List<String> instancesInClusterWithTag = helixAdmin.getInstancesInClusterWithTag(clusterName, builder.forTable(tenantName));
    LOGGER.info("Current: Nodes:" + currentHosts);
    LOGGER.info("New Nodes:" + instancesInClusterWithTag);
    Map<String, Map<String, String>> currentMapping = currentIdealState.getRecord().getMapFields();
    ZNRecord newZnRecord = rebalanceStrategy
        .computePartitionAssignment(instancesInClusterWithTag, currentMapping, instancesInClusterWithTag);
    backfillMissingOnlineReplicas(newZnRecord, targetNumReplicas);
    final Map<String, Map<String, String>> newMapping = newZnRecord.getMapFields();
    LOGGER.info("Current segment Assignment:");
    printSegmentAssignment(currentMapping);
    LOGGER.info("Final segment Assignment:");
    printSegmentAssignment(newMapping);
    if (!dryRun) {
      if (EqualityUtils.isEqual(newMapping, currentMapping)) {
        LOGGER.info("Skipping rebalancing for table:" + tableName + " since its already balanced");
      } else {
        HelixHelper.updateIdealState(helixManager, tableName,
            new com.google.common.base.Function<IdealState, IdealState>() {
              @Nullable
              @Override
              public IdealState apply(@Nullable IdealState idealState) {
                for (String segmentId : newMapping.keySet()) {
                  Map<String, String> instanceStateMap = newMapping.get(segmentId);
                  for (String instanceId : instanceStateMap.keySet()) {
                    idealState.setPartitionState(segmentId, instanceId, instanceStateMap.get(instanceId));
                  }
                }
                return idealState;
              }
            }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
        waitForStable(tableName);
        LOGGER.info("Successfully rebalanced table:" + tableName);
      }
    }
  }

  private void backfillMissingOnlineReplicas(ZNRecord znRecord, int desiredOnlineReplicaCount) {
    // Get the current segment -> (server, state) mappings (this is not a copy, at least in Helix 0.6.5)
    Map<String, Map<String, String>> currentMapping = znRecord.getMapFields();

    Map<String, Integer> perServerSegmentCount = new HashMap<>();

    // Count segments per server
    for (Map.Entry<String, Map<String, String>> entry : currentMapping.entrySet()) {
      String segmentName = entry.getKey();

      for (Map.Entry<String, String> serverAndState : entry.getValue().entrySet()) {
        String server = serverAndState.getKey();
        String state = serverAndState.getValue();

        if (state.equals("ONLINE")) {
          Integer serverSegmentCount = perServerSegmentCount.get(server);

          if (serverSegmentCount == null) {
            serverSegmentCount = 0;
          }

          perServerSegmentCount.put(server, serverSegmentCount + 1);
        } else {
          // Shouldn't happen, just ignore it
          LOGGER.warn("State is not ONLINE for server {} and segment {}", server, segmentName);
        }
      }
    }

    // Order the servers in number of replicas
    PriorityQueue<Pair<String, Integer>> serverQueue =
        new PriorityQueue<>(perServerSegmentCount.size(), new Comparator<Pair<String, Integer>>() {
          @Override
          public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
            return Integer.compare(o1.getValue(), o2.getValue());
          }
        });
    for (Map.Entry<String, Integer> entry : perServerSegmentCount.entrySet()) {
      serverQueue.add(Pair.of(entry.getKey(), entry.getValue()));
    }

    // Iterate over the segments and fill missing ONLINE entries
    for (Map.Entry<String, Map<String, String>> entry : currentMapping.entrySet()) {
      // Count number of servers in ONLINE state
      int onlineServerCount = 0;
      for (Map.Entry<String, String> serverAndState : entry.getValue().entrySet()) {
        if (serverAndState.getValue().equals("ONLINE")) {
          onlineServerCount++;
        }
      }

      if (onlineServerCount < desiredOnlineReplicaCount) {
        Set<String> serversInUse = new HashSet<>(entry.getValue().keySet());
        List<Pair<String, Integer>> rejectedServers = new ArrayList<>();

        // Add servers based on the queue
        while(!serverQueue.isEmpty() && onlineServerCount < desiredOnlineReplicaCount) {
          Pair<String, Integer> candidateServer = serverQueue.poll();
          String serverName = candidateServer.getKey();

          // If this server is already in the list of servers used, set it aside to add it later
          if (serversInUse.contains(serverName)) {
            rejectedServers.add(candidateServer);
          } else {
            // Add the server as ONLINE for this segment
            entry.getValue().put(serverName, "ONLINE");

            // Mark the server as in use and put it back into the queue with the incremented segment count
            serversInUse.add(serverName);
            serverQueue.add(Pair.of(serverName, candidateServer.getValue() + 1));

            onlineServerCount++;
          }
        }

        if (serverQueue.isEmpty()) {
          LOGGER.warn("Ran out of replicas while trying to assign servers, this shouldn't happen.");
        } else {
          // Add all the rejected servers back into the queue
          serverQueue.addAll(rejectedServers);
        }
      }
    }

    // This effectively does nothing, but in case Helix changes their API to return a copy, this will not break
    znRecord.setMapFields(currentMapping);
  }

  private static void usage() {
    System.out.println(
        "Usage: PinotRebalancer [" + rebalanceTableCmd + "|"  + rebalanceTenantCmd + "] <zkAddress> <clusterName> <tableName|tenantName>");
    System.out.println("Example: " + rebalanceTableCmd + " localhost:2181 PinotCluster myTable_OFFLINE");
    System.out.println("         " + rebalanceTenantCmd + " localhost:2181 PinotCluster beanCounter");
    System.exit(1);
  }
  public static void main(String[] args) throws Exception {

    final boolean dryRun = true;
    if (args.length != 4) {
      usage();
    }
    final String subCmd = args[0];
    final String zkAddress = args[1];
    final String clusterName = args[2];
    final String tableOrTenant = args[3];
    PinotSegmentRebalancer rebalancer = new PinotSegmentRebalancer(zkAddress, clusterName, dryRun);
    if (subCmd.equals(rebalanceTenantCmd)) {
      rebalancer.rebalanceTenantTables(tableOrTenant);
    } else if (subCmd.equals(rebalanceTableCmd)) {
      rebalancer.rebalanceTable(tableOrTenant);
    } else {
      usage();
    }
    if (dryRun) {
      System.out.println("That was a dryrun");
    }
  }
}

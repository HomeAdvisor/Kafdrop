/*
 * Copyright 2017 HomeAdvisor, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.homeadvisor.kafdrop.model;

import kafka.cluster.Cluster;

import java.util.*;

public class ClusterSummaryVO {
    private int topicCount;
    private int partitionCount;
    private int underReplicatedCount;
    private double preferredReplicaPercent;

    /**
     * Number of partitions each broker is the leader for
     */
    private Map<Integer, Integer> brokerLeaderPartitionCount = new HashMap<>();

    /**
     * Number of partitions each broker should be the leader for
     */
    private Map<Integer, Integer> brokerPreferredLeaderPartitionCount = new HashMap<>();

    private Map<Integer, Integer> brokerUnderReplicationCount = new HashMap<>();

    private Set<Integer> expectedBrokerIds = new HashSet<>();

    public int getTopicCount() {
        return topicCount;
    }

    public void setTopicCount(int topicCount) {
        this.topicCount = topicCount;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public int getUnderReplicatedCount() {
        return underReplicatedCount;
    }

    public void setUnderReplicatedCount(int underReplicatedCount) {
        this.underReplicatedCount = underReplicatedCount;
    }

    public int getLaggingReplicaCount()
    {
        return getExpectedBrokerIds().stream()
           .mapToInt(this::getBrokerUnderReplicationCount)
           .sum();
    }

    public double getPreferredReplicaPercent() {
        return preferredReplicaPercent;
    }

    public void setPreferredReplicaPercent(double preferredReplicaPercent) {
        this.preferredReplicaPercent = preferredReplicaPercent;
    }

    public Map<Integer, Integer> getBrokerLeaderPartitionCount() {
        return brokerLeaderPartitionCount;
    }

    public Integer getBrokerLeaderPartitionCount(int brokerId) {
        return brokerLeaderPartitionCount.get(brokerId);
    }

    public void addBrokerLeaderPartition(int brokerId) {
        addBrokerLeaderPartition(brokerId, 1);
    }

    public void addBrokerLeaderPartition(int brokerId, int partitionCount) {
        brokerLeaderPartitionCount.compute(brokerId, (k, v) -> v == null ? partitionCount : v + partitionCount);
    }

    public Map<Integer, Integer> getBrokerPreferredLeaderPartitionCount() {
        return brokerPreferredLeaderPartitionCount;
    }

    public Integer getBrokerPreferredLeaderPartitionCount(int brokerId) {
        return brokerPreferredLeaderPartitionCount.get(brokerId);
    }

    public void addBrokerPreferredLeaderPartition(int brokerId) {
        addBrokerPreferredLeaderPartition(brokerId, 1);
    }

    public void addBrokerPreferredLeaderPartition(int brokerId, int partitionCount) {
        brokerPreferredLeaderPartitionCount.compute(brokerId, (k, v) -> v == null ? partitionCount : v + partitionCount);
    }

    public Collection<Integer> getExpectedBrokerIds() {
        return expectedBrokerIds;
    }

    public void addExpectedBrokerId(int brokerId)
    {
        expectedBrokerIds.add(brokerId);
    }

    public void addBrokerUnderReplicatedPartition(int brokerId)
    {
        addBrokerUnderReplicatedPartitions(brokerId, 1);
    }

    public void addBrokerUnderReplicatedPartitions(int brokerId, int underReplicatedCount)
    {
        brokerUnderReplicationCount.merge(brokerId, underReplicatedCount, Integer::sum);
    }

    public Map<Integer, Integer> getBrokerUnderReplicationCount()
    {
        return brokerUnderReplicationCount;
    }

    public Integer getBrokerUnderReplicationCount(int brokerId)
    {
        return brokerUnderReplicationCount.get(brokerId);
    }

    public ClusterSummaryVO merge(ClusterSummaryVO that)
    {
        this.setPartitionCount(this.getPartitionCount() + that.getPartitionCount());
        this.setUnderReplicatedCount(this.getUnderReplicatedCount() + that.getUnderReplicatedCount());
        this.setPreferredReplicaPercent(this.getPreferredReplicaPercent() + that.getPreferredReplicaPercent());
        that.getBrokerLeaderPartitionCount().forEach(this::addBrokerLeaderPartition);
        that.getBrokerPreferredLeaderPartitionCount().forEach(this::addBrokerPreferredLeaderPartition);
        that.getBrokerUnderReplicationCount().forEach(this::addBrokerUnderReplicatedPartitions);
        that.getExpectedBrokerIds().forEach(this::addExpectedBrokerId);

        return this;
    }
}

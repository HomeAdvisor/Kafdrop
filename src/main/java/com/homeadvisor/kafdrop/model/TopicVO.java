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

import kafka.cluster.Broker;

import java.util.*;
import java.util.stream.Collectors;

public class TopicVO implements Comparable<TopicVO>
{
   private String name;
   private Map<Integer, TopicPartitionVO> partitions = new TreeMap<>();
   private Map<String, Object> config = new TreeMap<>();
   // description?
   // partition state
   // delete supported?


   public TopicVO(String name)
   {
      this.name = name;
   }

   public String getName()
   {
      return name;
   }

   public void setName(String name)
   {
      this.name = name;
   }

   public Map<String, Object> getConfig()
   {
      return config;
   }

   public void setConfig(Map<String, Object> config)
   {
      this.config = config;
   }

   public Collection<TopicPartitionVO> getPartitions()
   {
      return partitions.values();
   }

   public Optional<TopicPartitionVO> getPartition(int partitionId)
   {
      return Optional.ofNullable(partitions.get(partitionId));
   }

   public Collection<TopicPartitionVO> getLeaderPartitions(int brokerId)
   {
      return partitions.values().stream()
         .filter(tp -> tp.getLeader() != null && tp.getLeader().getId() == brokerId)
         .collect(Collectors.toList());
   }

   public Collection<TopicPartitionVO> getUnderReplicatedPartitions()
   {
      return partitions.values().stream()
         .filter(TopicPartitionVO::isUnderReplicated)
         .collect(Collectors.toList());
   }

   public void setPartitions(Map<Integer, TopicPartitionVO> partitions)
   {
      this.partitions = partitions;
   }

   /**
    * Returns the total number of messages published to the topic, ever
    * @return
    */
   public long getTotalSize()
   {
      return partitions.values().stream()
         .map(TopicPartitionVO::getSize)
         .reduce(0L, Long::sum);
   }

   /**
    * Returns the total number of messages available to consume from the topic.
    * @return
    */
   public long getAvailableSize()
   {
      return partitions.values().stream()
         .map(p -> p.getSize() - p.getFirstOffset())
         .reduce(0L, Long::sum);
   }

   public double getPreferredReplicaPercent()
   {
      long preferredLeaderCount = partitions.values().stream()
         .filter(TopicPartitionVO::isLeaderPreferred)
         .count();
      return ((double) preferredLeaderCount) / ((double)partitions.size());
   }

   public Map<String, List<TopicPartitionVO.PartitionReplica>> getBrokerLeaders()
   {
      return partitions.values().stream()
              .map(TopicPartitionVO::getLeader)
              .filter(Objects::nonNull)
              .collect(Collectors.groupingBy(leader -> leader.getId().toString(), TreeMap::new, Collectors.toList()));
   }

   public Map<String, BrokerReplicas> getBrokerReplicas()
   {
      Map<String, BrokerReplicas> brokerReplicasMap = new TreeMap<>();
      partitions.values().stream()
         .flatMap(tp -> tp.getReplicas().stream())
         .forEach(r -> brokerReplicasMap.computeIfAbsent(r.getId().toString(),
                                                         BrokerReplicas::new)
            .addReplica(r));
      return brokerReplicasMap;
   }

   public static class BrokerReplicas
   {
      /** The ID of the broker owning the replicas */
      private final String brokerId;
      /** List of leader replicas on this broker */
      private final List<TopicPartitionVO.PartitionReplica> leaders = new ArrayList<>();
      /** List of non-leader replicas on this broker */
      private final List<TopicPartitionVO.PartitionReplica> replicas = new ArrayList<>();

      public BrokerReplicas(String brokerId) {
         this.brokerId = brokerId;
      }

      public void addReplica(TopicPartitionVO.PartitionReplica replica)
      {
         if (replica.isLeader()) {
            leaders.add(replica);
         }
         else {
            replicas.add(replica);
         }
      }

      public String getBrokerId()
      {
         return brokerId;
      }

      public List<TopicPartitionVO.PartitionReplica> getLeaders()
      {
         return leaders;
      }

      public List<TopicPartitionVO.PartitionReplica> getReplicas()
      {
         return replicas;
      }
   }

   public void addPartition(TopicPartitionVO partition)
   {
      partitions.put(partition.getId(), partition);
   }

   @Override
   public int compareTo(TopicVO that)
   {
      return this.name.compareTo(that.name);
   }

   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TopicVO that = (TopicVO) o;

      if (!name.equals(that.name)) return false;

      return true;
   }

   @Override
   public int hashCode()
   {
      return name.hashCode();
   }

}

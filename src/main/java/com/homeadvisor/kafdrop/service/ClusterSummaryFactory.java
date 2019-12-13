/*
 * Copyright 2019 HomeAdvisor, Inc.
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

package com.homeadvisor.kafdrop.service;

import com.homeadvisor.kafdrop.model.ClusterSummaryVO;
import com.homeadvisor.kafdrop.model.TopicPartitionVO;
import com.homeadvisor.kafdrop.model.TopicVO;

import java.util.Collection;

public class ClusterSummaryFactory
{
   public ClusterSummaryVO getClusterSummary(Collection<TopicVO> topics)
   {
      final ClusterSummaryVO topicSummary = topics.stream()
         .map(this::createSummaryFromTopic)
         .reduce(ClusterSummaryVO::merge)
         .orElseGet(ClusterSummaryVO::new);
      topicSummary.setTopicCount(topics.size());
      topicSummary.setPreferredReplicaPercent(topicSummary.getPreferredReplicaPercent() / topics.size());
      return topicSummary;
   }

   public ClusterSummaryVO createSummaryFromTopic(TopicVO topic)
   {
      ClusterSummaryVO summary = new ClusterSummaryVO();
      topic.getPartitions().forEach(partition -> populateSummaryWithPartitionData(summary, partition));

      summary.setPartitionCount(topic.getPartitions().size());
      summary.setUnderReplicatedCount(topic.getUnderReplicatedPartitions().size());
      summary.setPreferredReplicaPercent(topic.getPreferredReplicaPercent());
      return summary;
   }

   public void populateSummaryWithPartitionData(ClusterSummaryVO summary, TopicPartitionVO partition)
   {
      if (partition.getLeader() != null)
      {
         summary.addBrokerLeaderPartition(partition.getLeader().getId());
      }
      if (partition.getPreferredLeader() != null)
      {
         summary.addBrokerPreferredLeaderPartition(partition.getPreferredLeader().getId());
      }
      partition.getReplicas()
         .forEach(replica -> {
            summary.addExpectedBrokerId(replica.getId());
            if (replica.isInService())
            {
               summary.addBrokerUnderReplicatedPartitions(replica.getId(), 0);
            }
            else
            {
               summary.addBrokerUnderReplicatedPartition(replica.getId());
            }
         });
   }
}

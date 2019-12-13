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
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class ClusterSummaryFactoryTest
{

   @Test
   public void clusterSummary()
   {
      List<TopicVO> topicList = new ArrayList<>();
      topicList.add(createTopic("test.topic.1",
                                tp -> {
                                   tp.setFirstOffset(10);
                                   tp.setSize(20);
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(10, false, true));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(11, true, false));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, false));
                                },
                                tp -> {
                                   tp.setFirstOffset(12);
                                   tp.setSize(26);
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(10, true, false));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(11, true, false));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, true));
                                },
                                tp -> {
                                   tp.setFirstOffset(13);
                                   tp.setSize(25);
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(10, false, false));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(11, false, true));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, false));
                                }
      ));


      topicList.add(createTopic("test.topic.2",
                                tp -> {
                                   tp.setFirstOffset(1);
                                   tp.setSize(2);
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(10, false, true));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(11, true, false));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, false));
                                },
                                tp -> {
                                   tp.setFirstOffset(1);
                                   tp.setSize(10);
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, true));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(10, true, false));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(11, true, false));
                                },
                                tp -> {
                                   tp.setFirstOffset(5);
                                   tp.setSize(15);
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(11, false, true));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(10, false, false));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, false));
                                }
      ));


      final ClusterSummaryVO summary = new ClusterSummaryFactory().getClusterSummary(topicList);

      assertEquals("Unexpected partition count", 6, summary.getPartitionCount());
      assertEquals("Unexpected preferred replica percent", (4.0/6.0), summary.getPreferredReplicaPercent(), 0.0001);
      assertEquals("Unexpected under-replicated count", 4, summary.getUnderReplicatedCount());
      assertEquals("Unexpected lagging replica count", 6, summary.getLaggingReplicaCount());
      assertEquals("Unexpected topic count", 2, summary.getTopicCount());
      assertEquals("", Integer.valueOf(4), summary.getBrokerUnderReplicationCount(10));
      assertEquals("", Integer.valueOf(2), summary.getBrokerUnderReplicationCount(11));
      assertEquals("", Integer.valueOf(0), summary.getBrokerUnderReplicationCount(12));
   }

   @Test
   public void clusterSummarySingleTopic()
   {
      List<TopicVO> topicList = new ArrayList<>();
      topicList.add(createTopic("test.topic.1",
                                tp -> {
                                   tp.setFirstOffset(10);
                                   tp.setSize(20);
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(10, true, true));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(11, false, false));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, false));
                                },
                                tp -> {
                                   tp.setFirstOffset(12);
                                   tp.setSize(26);
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(11, true, true));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(10, true, false));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, false));
                                },
                                tp -> {
                                   tp.setFirstOffset(13);
                                   tp.setSize(25);
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, true));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(10, false, false));
                                   tp.addReplica(new TopicPartitionVO.PartitionReplica(11, false, false));
                                }
      ));


      final ClusterSummaryVO summary = new ClusterSummaryFactory().getClusterSummary(topicList);

      assertEquals("Unexpected partition count", 3, summary.getPartitionCount());
      assertEquals("Unexpected preferred replica percent", 1.0, summary.getPreferredReplicaPercent(), 0.0001);
      assertEquals("Unexpected under-replicated count", 2, summary.getUnderReplicatedCount());
      assertEquals("Unexpected lagging replica count", 3, summary.getLaggingReplicaCount());
      assertEquals("Unexpected topic count", 1, summary.getTopicCount());
      assertEquals("", Integer.valueOf(1), summary.getBrokerUnderReplicationCount(10));
      assertEquals("", Integer.valueOf(2), summary.getBrokerUnderReplicationCount(11));
   }

   private TopicVO createTopic(String name, Consumer<TopicPartitionVO>... partitionConfigurers)
   {
      TopicVO topic = new TopicVO(name);

      for (int partitionId = 0; partitionId < partitionConfigurers.length; partitionId++)
      {
         Consumer<TopicPartitionVO> configurer = partitionConfigurers[partitionId];

         topic.addPartition(new TopicPartitionVO(partitionId));
         topic.getPartition(partitionId).ifPresent(configurer);
      }

      return topic;
   }
}
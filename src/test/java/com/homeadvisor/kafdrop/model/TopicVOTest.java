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

package com.homeadvisor.kafdrop.model;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TopicVOTest
{

   @Test
   public void outOfSyncReplicas()
   {
      TopicVO topic = new TopicVO("test.topic");
      topic.addPartition(new TopicPartitionVO(1));
      topic.addPartition(new TopicPartitionVO(2));
      topic.addPartition(new TopicPartitionVO(3));

      topic.getPartition(1).ifPresent(tp -> {
         tp.setFirstOffset(10);
         tp.setSize(20);
         tp.addReplica(new TopicPartitionVO.PartitionReplica(10, false, true));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(11, true, false));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, false));
      });

      topic.getPartition(2).ifPresent(tp -> {
         tp.setFirstOffset(12);
         tp.setSize(26);
         tp.addReplica(new TopicPartitionVO.PartitionReplica(10, true, false));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(11, true, false));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, true));
      });

      topic.getPartition(3).ifPresent(tp -> {
         tp.setFirstOffset(13);
         tp.setSize(25);
         tp.addReplica(new TopicPartitionVO.PartitionReplica(10, false, false));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(11, false, true));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, false));
      });

      assertEquals("Unexpected under-replicated partition list",
                   Arrays.asList(1, 3),
                   topic.getUnderReplicatedPartitions().stream()
                      .map(TopicPartitionVO::getId)
                      .collect(Collectors.toList()));

      assertEquals(new HashSet<>(Arrays.asList(10, 11)), topic.getUnderreplicatedBrokerIds());
   }

   @Test
   public void leaderPartitions()
   {
      TopicVO topic = new TopicVO("test.topic");
      topic.addPartition(new TopicPartitionVO(1));
      topic.addPartition(new TopicPartitionVO(2));
      topic.addPartition(new TopicPartitionVO(3));

      topic.getPartition(1).ifPresent(tp -> {
         tp.setFirstOffset(10);
         tp.setSize(20);
         tp.addReplica(new TopicPartitionVO.PartitionReplica(10, false, true));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(11, true, false));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, false));
      });

      topic.getPartition(2).ifPresent(tp -> {
         tp.setFirstOffset(12);
         tp.setSize(26);
         tp.addReplica(new TopicPartitionVO.PartitionReplica(10, true, false));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(11, true, false));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, true));
      });

      topic.getPartition(3).ifPresent(tp -> {
         tp.setFirstOffset(13);
         tp.setSize(25);
         tp.addReplica(new TopicPartitionVO.PartitionReplica(10, false, false));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(11, false, true));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, false));
      });

      assertEquals(Arrays.asList(1),
                   topic.getLeaderPartitions(10).stream()
                      .map(TopicPartitionVO::getId)
                      .collect(Collectors.toList()));

      assertEquals(Arrays.asList(3),
                   topic.getLeaderPartitions(11).stream()
                      .map(TopicPartitionVO::getId)
                      .collect(Collectors.toList()));

      assertEquals(Arrays.asList(2),
                   topic.getLeaderPartitions(12).stream()
                      .map(TopicPartitionVO::getId)
                      .collect(Collectors.toList()));
   }

   @Test
   public void totalSize()
   {
      TopicVO topic = new TopicVO("test.topic");
      topic.addPartition(new TopicPartitionVO(1));
      topic.addPartition(new TopicPartitionVO(2));
      topic.addPartition(new TopicPartitionVO(3));

      topic.getPartition(1).ifPresent(tp -> {
         tp.setFirstOffset(10);
         tp.setSize(20);
         tp.addReplica(new TopicPartitionVO.PartitionReplica(10, false, true));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(11, true, false));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, false));
      });

      topic.getPartition(2).ifPresent(tp -> {
         tp.setFirstOffset(12);
         tp.setSize(26);
         tp.addReplica(new TopicPartitionVO.PartitionReplica(10, true, false));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(11, true, false));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, true));
      });

      topic.getPartition(3).ifPresent(tp -> {
         tp.setFirstOffset(13);
         tp.setSize(25);
         tp.addReplica(new TopicPartitionVO.PartitionReplica(10, false, false));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(11, false, true));
         tp.addReplica(new TopicPartitionVO.PartitionReplica(12, true, false));
      });

      assertEquals(71, topic.getTotalSize());
      assertEquals(36, topic.getAvailableSize());
   }

}
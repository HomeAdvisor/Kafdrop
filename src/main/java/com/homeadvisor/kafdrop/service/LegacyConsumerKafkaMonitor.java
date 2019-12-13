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

import com.google.common.collect.Lists;
import com.homeadvisor.kafdrop.model.BrokerVO;
import com.homeadvisor.kafdrop.model.ClusterSummaryVO;
import com.homeadvisor.kafdrop.model.ConsumerVO;
import com.homeadvisor.kafdrop.model.TopicVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Service
@Qualifier("legacyConsumer")
public class LegacyConsumerKafkaMonitor implements KafkaMonitor
{
   private final KafkaMonitor primaryMonitor;
   private final KafkaMonitor legacyMonitor;

   public LegacyConsumerKafkaMonitor(KafkaMonitor primaryMonitor,
                                     @Autowired(required = false) @Qualifier("curator") KafkaMonitor legacyMonitor)
   {
      this.primaryMonitor = primaryMonitor;
      this.legacyMonitor = legacyMonitor;
   }

   @Override
   public List<BrokerVO> getBrokers()
   {
      return primaryMonitor.getBrokers();
   }

   @Override
   public Optional<BrokerVO> getBroker(int id)
   {
      return primaryMonitor.getBroker(id);
   }

   @Override
   public List<TopicVO> getTopics()
   {
      return primaryMonitor.getTopics();
   }

   @Override
   public Optional<TopicVO> getTopic(String topic)
   {
      return primaryMonitor.getTopic(topic);
   }

   @Override
   public ClusterSummaryVO getClusterSummary(Collection<TopicVO> topics)
   {
      return primaryMonitor.getClusterSummary(topics);
   }

   @Override
   public List<ConsumerVO> getConsumers(TopicVO topic)
   {
      List<ConsumerVO> consumers = new ArrayList<>();
      consumers.addAll(primaryMonitor.getConsumers(topic));
      if (legacyMonitor != null)
      {
         consumers.addAll(legacyMonitor.getConsumers(topic));
      }
      return consumers;
   }

   @Override
   public Optional<ConsumerVO> getConsumer(String groupId)
   {
      return primaryMonitor.getConsumer(groupId);
   }

   @Override
   public Optional<ConsumerVO> getConsumerByTopic(String groupId, TopicVO topic)
   {
      return primaryMonitor.getConsumerByTopic(groupId, topic);
   }
}

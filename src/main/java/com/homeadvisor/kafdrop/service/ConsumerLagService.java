package com.homeadvisor.kafdrop.service;

import com.homeadvisor.kafdrop.model.ConsumerTopicVO;
import com.homeadvisor.kafdrop.model.ConsumerVO;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;


@Service
public class ConsumerLagService
{
   private final KafkaMonitor kafkaMonitor;

   @Autowired
   public ConsumerLagService(CuratorKafkaMonitor kafkaMonitor)
   {
      this.kafkaMonitor = kafkaMonitor;
   }

   public Collection<ConsumerTopicVO> computeConsumerGroupLag(String groupId, ConsumerLagReportingOffsetType offsetType)
   {
      return computeConsumerGroupLag(groupId, offsetType, null);
   }

   public Collection<ConsumerTopicVO> computeConsumerGroupLag(String groupId, ConsumerLagReportingOffsetType offsetType, String topicId)
   {
      final ConsumerVO consumer = kafkaMonitor
            .getConsumer(groupId)
            .orElseThrow(() -> new ConsumerNotFoundException(groupId));

      final Collection<ConsumerTopicVO> topics;
      if (StringUtils.isNotEmpty(topicId))
      {
         topics = Collections.singleton(consumer
               .getTopics()
               .stream()
               .filter(t -> t
                     .getTopic()
                     .equals(topicId))
               .findFirst()
               .orElseThrow(() -> new TopicNotFoundException(topicId)));
      }
      else
      {
         topics = consumer.getTopics();
      }

      return topics;
   }
}

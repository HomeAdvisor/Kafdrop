package com.homeadvisor.kafdrop.model.dto;

import com.homeadvisor.kafdrop.model.ConsumerTopicVO;
import com.homeadvisor.kafdrop.model.ConsumerVO;
import com.homeadvisor.kafdrop.service.ConsumerLagReportingOffsetType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ConsumerDTO extends BaseLagDTO
{
   private Map<String, ConsumerTopicDTO> topics = new HashMap<>();


   ConsumerDTO(ConsumerVO consumerVO, ConsumerLagReportingOffsetType offsetType)
   {
      this(consumerVO.getTopics(), offsetType);
   }

   public ConsumerDTO(Collection<ConsumerTopicVO> topicVOs, ConsumerLagReportingOffsetType offsetType)
   {
      super(offsetType);

      for (ConsumerTopicVO topicVO : topicVOs)
      {
         topics.put(topicVO.getTopic(), new ConsumerTopicDTO(topicVO, offsetType));
      }
   }


   public Map<String, ConsumerTopicDTO> getTopics()
   {
      return topics;
   }


   @Override
   protected long getLagSum(Function<ConsumerPartitionDTO, Long> getLagFunction)
   {
      return getTopics()
            .entrySet()
            .stream()
            .map(Map.Entry::getValue)
            .flatMap(t -> t
                  .getPartitions()
                  .entrySet()
                  .stream())
            .map(Map.Entry::getValue)
            .map(getLagFunction)
            .collect(Collectors.summarizingLong(i -> i))
            .getSum();
   }
}

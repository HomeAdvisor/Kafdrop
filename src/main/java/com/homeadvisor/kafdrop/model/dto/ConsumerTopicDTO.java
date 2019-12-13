package com.homeadvisor.kafdrop.model.dto;

import com.homeadvisor.kafdrop.model.ConsumerPartitionVO;
import com.homeadvisor.kafdrop.model.ConsumerTopicVO;
import com.homeadvisor.kafdrop.service.ConsumerLagReportingOffsetType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ConsumerTopicDTO extends BaseLagDTO
{
   private Map<Integer, ConsumerPartitionDTO> partitions = new HashMap<>();


   ConsumerTopicDTO(ConsumerTopicVO topic, ConsumerLagReportingOffsetType offsetType)
   {
      super(offsetType);

      for (ConsumerPartitionVO partition : topic.getPartitions())
      {
         partitions.put(partition.getPartitionId(), new ConsumerPartitionDTO(partition, offsetType));
      }
   }

   public Map<Integer, ConsumerPartitionDTO> getPartitions()
   {
      return partitions;
   }


   @Override
   protected long getLagSum(Function<ConsumerPartitionDTO, Long> getLagFunction)
   {
      return getPartitions()
            .entrySet()
            .stream()
            .map(Map.Entry::getValue)
            .map(getLagFunction)
            .collect(Collectors.summarizingLong(i -> i))
            .getSum();
   }
}

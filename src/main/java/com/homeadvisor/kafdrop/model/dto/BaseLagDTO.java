package com.homeadvisor.kafdrop.model.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.homeadvisor.kafdrop.service.ConsumerLagReportingOffsetType;

import java.util.function.Function;

public abstract class BaseLagDTO
{
   protected ConsumerLagReportingOffsetType offsetType;

   public BaseLagDTO(ConsumerLagReportingOffsetType offsetType)
   {
      this.offsetType = offsetType;
   }

   @JsonInclude(JsonInclude.Include.NON_NULL)
   public Long getTotalKafkaLag()
   {
      switch (offsetType)
      {
         case Both:
         case Kafka:
            return getLagSum(ConsumerPartitionDTO::getKafkaLag);
      }
      return null;
   }

   @JsonInclude(JsonInclude.Include.NON_NULL)
   public Long getTotalZookeeperLag()
   {
      switch (offsetType)
      {
         case Both:
         case Zookeeper:
            return getLagSum(ConsumerPartitionDTO::getZookeeperLag);

      }
      return null;
   }

   @JsonInclude(JsonInclude.Include.NON_NULL)
   public Long getTotalLag()
   {
      switch (offsetType)
      {
         case Minimum:
            return getLagSum(ConsumerPartitionDTO::getLag);
      }
      return null;
   }

   protected abstract long getLagSum(Function<ConsumerPartitionDTO, Long> getLagFunction);
}

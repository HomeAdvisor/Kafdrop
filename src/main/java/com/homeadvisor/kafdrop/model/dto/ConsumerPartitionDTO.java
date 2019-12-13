package com.homeadvisor.kafdrop.model.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.homeadvisor.kafdrop.model.ConsumerPartitionVO;
import com.homeadvisor.kafdrop.service.ConsumerLagReportingOffsetType;

public class ConsumerPartitionDTO
{
   private ConsumerLagReportingOffsetType offsetType;
   private Long kafkaLag;
   private Long zookeeperLag;


   ConsumerPartitionDTO(ConsumerPartitionVO partition, ConsumerLagReportingOffsetType offsetType)
   {
      this.offsetType = offsetType;
      kafkaLag = partition.getKafkaLag();
      zookeeperLag = partition.getZookeeperLag();
   }

   @JsonInclude(JsonInclude.Include.NON_NULL)
   public Long getKafkaLag()
   {
      switch (offsetType)
      {
         case Both:
         case Kafka:
            return kafkaLag;
      }
      return null;
   }

   @JsonInclude(JsonInclude.Include.NON_NULL)
   public Long getZookeeperLag()
   {
      switch (offsetType)
      {
         case Both:
         case Zookeeper:
            return zookeeperLag;
      }
      return null;
   }

   @JsonInclude(JsonInclude.Include.NON_NULL)
   public Long getLag()
   {
      switch (offsetType)
      {
         case Minimum:
            return computeMinimum();
      }
      return null;
   }

   private Long computeMinimum()
   {
      if (null != kafkaLag && null != zookeeperLag)
      {
         if (kafkaLag < zookeeperLag)
         {
            return kafkaLag;
         }
         return zookeeperLag;
      }
      // the remaining scenarios should never occur.
      return 0L;
   }
}

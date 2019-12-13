package com.homeadvisor.kafdrop.service;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

public enum ConsumerLagReportingOffsetType
{
   Kafka("kafka"), Zookeeper("zookeeper"), Both("both"), Minimum("minimum");

   private String type;
   private static final Map<String, ConsumerLagReportingOffsetType> typeLookup = new HashMap<>();


   ConsumerLagReportingOffsetType(String type)
   {
      this.type = type;
   }


   // Can't override valueOf(), so...use this instead.
   public static ConsumerLagReportingOffsetType enumValue(String type)
   {
      ConsumerLagReportingOffsetType value = typeLookup.get(StringUtils
            .trimToEmpty(type).toLowerCase());
      if (null == value)
      {
         return Minimum;
      }
      return value;
   }

   public String getValue()
   {
      return type;
   }

   static
   {
      for(ConsumerLagReportingOffsetType env : values())
      {
         typeLookup.put(env.getValue(), env);
      }
   }
}

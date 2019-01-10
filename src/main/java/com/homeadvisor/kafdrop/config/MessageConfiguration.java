package com.homeadvisor.kafdrop.config;

import javax.annotation.PostConstruct;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import com.homeadvisor.kafdrop.util.SearchType;;


@Configuration
public class MessageConfiguration {

   @Component
   @ConfigurationProperties(prefix = "message")
   public static class MessageProperties
   {

      private SearchType type;
      private static String consumerName = "kafka-test-ARRTP";

      @PostConstruct
      public void init() {
         // Set a default message format if not configured.
         if (type == null) {
            type = SearchType.Offset;
         }
      }

      public SearchType getType()
      {
         return type;
      }

      public void setType(SearchType type)
      {
         this.type = type;
      }

      public String getConsumerName() 
      {
         return this.consumerName;
      }

   }

}

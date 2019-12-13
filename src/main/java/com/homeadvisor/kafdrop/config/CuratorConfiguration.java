/*
 * Copyright 2017 HomeAdvisor, Inc.
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

package com.homeadvisor.kafdrop.config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Configuration
public class CuratorConfiguration
{
   @Primary
   @Bean(initMethod = "start", destroyMethod = "close")
   public CuratorFramework curatorFramework(ZookeeperProperties props)
   {
      return CuratorFrameworkFactory.builder()
         .connectString(props.getConnect())
         .connectionTimeoutMs(props.getConnectTimeoutMillis())
         .sessionTimeoutMs(props.getSessionTimeoutMillis())
         .retryPolicy(new RetryNTimes(props.getMaxRetries(), props.getRetryMillis()))
         .build();
   }

   @Primary
   @Bean
   @ConfigurationProperties(prefix = "kafka.zookeeper")
   public ZookeeperProperties zookeeperProperties()
   {
      return new ZookeeperProperties();
   }

   @Component(value = "curatorConnection")
   private static class CuratorHealthIndicator extends AbstractHealthIndicator
   {
      private final CuratorFramework framework;

      @Autowired
      public CuratorHealthIndicator(CuratorFramework framework)
      {
         this.framework = framework;
      }

      @Override
      protected void doHealthCheck(Health.Builder builder) throws Exception
      {
         if (framework.getZookeeperClient().isConnected())
         {
            builder.up();
         }
         else
         {
            builder.down();
         }
      }
   }
}

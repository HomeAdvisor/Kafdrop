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

import com.google.common.base.Throwables;
import com.homeadvisor.kafdrop.util.JmxUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.x.discovery.*;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.info.InfoEndpoint;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.boot.web.server.WebServer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

import java.beans.Introspector;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Configuration
@ConditionalOnProperty(value = "curator.discovery.enabled", havingValue = "true")
public class ServiceDiscoveryConfiguration
{
   @Value("${spring.jmx.default_domain}")
   private String jmxDomain;

   @Bean(initMethod = "start", destroyMethod = "close")
   @Qualifier("serviceDiscovery")
   public CuratorFramework serviceDiscoveryCuratorFramework(@Qualifier("serviceDiscovery") ZookeeperProperties props)
   {
      return CuratorFrameworkFactory.builder()
         .connectString(props.getConnect())
         .connectionTimeoutMs(props.getConnectTimeoutMillis())
         .sessionTimeoutMs(props.getSessionTimeoutMillis())
         .retryPolicy(new RetryNTimes(props.getMaxRetries(), props.getRetryMillis()))
         .build();
   }

   @Bean
   @ConfigurationProperties(prefix = "zookeeper")
   @Qualifier("serviceDiscovery")
   public ZookeeperProperties serviceDiscoveryZookeeperProperties()
   {
      return new ZookeeperProperties();
   }

   @Component(value = "serviceDiscoveryCuratorConnection")
   private static class CuratorHealthIndicator extends AbstractHealthIndicator
   {
      private final CuratorFramework framework;

      @Autowired
      public CuratorHealthIndicator(@Qualifier("serviceDiscovery") CuratorFramework framework)
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

   @Bean(initMethod = "start", destroyMethod = "close")
   public ServiceDiscovery curatorServiceDiscovery(
      @Qualifier("serviceDiscovery") CuratorFramework curatorFramework,
      @Value("${curator.discovery.basePath:/homeadvisor/services}") String basePath) throws Exception
   {
      final Class payloadClass = Object.class;
      curatorFramework.createContainers(basePath);
      return ServiceDiscoveryBuilder.builder(payloadClass)
         .client(curatorFramework)
         .basePath(basePath)
         .serializer(new JsonInstanceSerializer(payloadClass))
         .build();
   }

   @Bean
   public ServiceDiscoveryApplicationListener serviceDiscoveryStartupListener(WebServerApplicationContext webContext,
                                                                              ServiceDiscovery serviceDiscovery,
                                                                              Environment environment,
                                                                              InfoEndpoint infoEndpoint)
   {
      return new ServiceDiscoveryApplicationListener(webContext, serviceDiscovery, environment, infoEndpoint);
   }

   public class ServiceDiscoveryApplicationListener implements ApplicationListener<ApplicationReadyEvent>
   {
      private final WebServerApplicationContext webContext;
      private final ServiceDiscovery serviceDiscovery;
      private final Environment environment;
      private final InfoEndpoint infoEndpoint;

      public ServiceDiscoveryApplicationListener(WebServerApplicationContext webContext,
                                                 ServiceDiscovery serviceDiscovery,
                                                 Environment environment,
                                                 InfoEndpoint infoEndpoint)
      {
         this.webContext = webContext;
         this.serviceDiscovery = serviceDiscovery;
         this.environment = environment;
         this.infoEndpoint = infoEndpoint;
      }

      @Override
      public void onApplicationEvent(ApplicationReadyEvent event)
      {
         try
         {
            serviceDiscovery.registerService(createServiceInstance());
         }
         catch (Exception e)
         {
            throw Throwables.propagate(e);
         }
      }

      public ServiceInstance createServiceInstance() throws Exception
      {
         final Map<String, Object> details = serviceDetails(getServicePort());

         final ServiceInstanceBuilder<Map<String, Object>> builder = ServiceInstance.builder();
         Optional.ofNullable(details.get("port")).ifPresent(port -> builder.port((Integer) port));

         return builder
            .id((String) details.get("id"))
            .name((String) details.get("name"))
            .payload(details)
            .uriSpec(new UriSpec("http://{address}:{port}"))
            .build();
      }

      public Map<String, Object> serviceDetails(Integer serverPort)
      {
         Map<String, Object> details = new LinkedHashMap<>();

         Optional.ofNullable(infoEndpoint.info())
            .ifPresent(infoMap -> Optional.ofNullable((Map<String, Object>) infoMap.get("build"))
               .ifPresent(buildInfo -> {
                  details.put("serviceName", buildInfo.get("artifact"));
                  details.put("serviceDescription", buildInfo.get("description"));
                  details.put("serviceVersion", buildInfo.get("version"));
               }));

         final String name = (String) details.getOrDefault("serviceName", "kafdrop");

         String host = null;
         try
         {
            host = InetAddress.getLocalHost().getHostName();
         }
         catch (UnknownHostException e)
         {
            host = "<unknown>";
         }

         details.put("id", Stream.of(name, host, UUID.randomUUID().toString()).collect(Collectors.joining("_")));
         details.put("name", name);
         details.put("host", host);
         details.put("jmxPort", JmxUtils.getJmxPort(environment));
         details.put("jmxHealthMBean", jmxDomain + ":name=" + healthCheckBeanName() + ",type=" + ClassUtils.getShortName(HealthCheckConfiguration.HealthCheck.class));
         details.put("port", serverPort);

         return details;
      }

      private String healthCheckBeanName()
      {
         String shortClassName = ClassUtils.getShortName(HealthCheckConfiguration.HealthCheck.class);
         return Introspector.decapitalize(shortClassName);
      }

      private Integer getServicePort()
      {
         return Optional.ofNullable(webContext.getWebServer())
            .map(WebServer::getPort)
            .filter(i -> i != -1)
            .orElse(null);
      }
   }
}

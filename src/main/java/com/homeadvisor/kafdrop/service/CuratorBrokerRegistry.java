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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.homeadvisor.kafdrop.model.BrokerVO;
import kafka.utils.ZkUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class CuratorBrokerRegistry implements BrokerRegistry
{
   private final Logger LOG = LoggerFactory.getLogger(getClass());

   private PathChildrenCache brokerPathCache;
   private ConcurrentMap<Integer, BrokerVO> brokerCache = new ConcurrentHashMap<>();

   public CuratorBrokerRegistry(CuratorFramework curatorFramework) throws Exception
   {
      brokerPathCache = new PathChildrenCache(curatorFramework, ZkUtils.BrokerIdsPath(), true);
      brokerPathCache.getListenable().addListener(new BrokerListener());
      brokerPathCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
   }

   @Override
   public Map<Integer, BrokerVO> getBrokers()
   {
      return Collections.unmodifiableMap(brokerCache);
   }

   private BrokerVO addBroker(BrokerVO broker)
   {
      final BrokerVO oldBroker = brokerCache.put(broker.getId(), broker);
      LOG.info("Kafka broker {} was {}", broker.getId(), oldBroker == null ? "added" : "updated");
      return oldBroker;
   }

   private BrokerVO removeBroker(int brokerId)
   {
      final BrokerVO broker = brokerCache.remove(brokerId);
      LOG.info("Kafka broker {} was removed", broker.getId());
      return broker;
   }


   private class BrokerListener implements PathChildrenCacheListener
   {
      private final ObjectMapper objectMapper;

      public BrokerListener()
      {
         objectMapper = new Jackson2ObjectMapperBuilder().build();
      }

      @Override
      public void childEvent(CuratorFramework framework, PathChildrenCacheEvent event) throws Exception
      {
         switch (event.getType())
         {
            case CHILD_REMOVED:
            {
               BrokerVO broker = removeBroker(brokerId(event.getData()));
               break;
            }

            case CHILD_ADDED:
            case CHILD_UPDATED:
            {
               addBroker(parseBroker(event.getData()));
               break;
            }

            case INITIALIZED:
            {
               brokerPathCache.getCurrentData().stream()
                  .map(CuratorBrokerRegistry.BrokerListener.this::parseBroker)
                  .forEach(CuratorBrokerRegistry.this::addBroker);
               break;
            }
         }
      }

      private int brokerId(ChildData input)
      {
         return Integer.parseInt(StringUtils.substringAfter(input.getPath(), ZkUtils.BrokerIdsPath() + "/"));
      }


      private BrokerVO parseBroker(ChildData input)
      {
         try
         {
            final BrokerVO broker = objectMapper.reader(BrokerVO.class).readValue(input.getData());
            broker.setId(brokerId(input));
            return broker;
         }
         catch (IOException e)
         {
            throw Throwables.propagate(e);
         }
      }
   }
}

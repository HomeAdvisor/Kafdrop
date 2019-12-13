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

package com.homeadvisor.kafdrop;

import com.homeadvisor.kafdrop.config.ini.IniFilePropertySource;
import com.homeadvisor.kafdrop.config.ini.IniFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringApplicationRunListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;

import java.io.*;
import java.util.Objects;
import java.util.stream.Stream;

@Order(Ordered.HIGHEST_PRECEDENCE)
public class EnvironmentSetupListener implements SpringApplicationRunListener, Ordered
{
   private final Logger LOG = LoggerFactory.getLogger(getClass());

   private static final String SM_CONFIG_DIR = "sm.config.dir";
   private static final String CONFIG_SUFFIX = "-config.ini";

   public EnvironmentSetupListener(SpringApplication application, String[] args)
   {
   }

   @Override
   public int getOrder()
   {
      return Ordered.HIGHEST_PRECEDENCE;
   }

   @Override
   public void starting()
   {

   }

   @Override
   public void environmentPrepared(ConfigurableEnvironment environment)
   {
      if (environment.containsProperty(SM_CONFIG_DIR))
      {
         Stream.of("kafdrop", "global")
            .map(name -> readProperties(environment, name))
            .filter(Objects::nonNull)
            .forEach(iniPropSource -> environment.getPropertySources()
               .addLast(iniPropSource));
      }
   }

   @Override
   public void contextPrepared(ConfigurableApplicationContext context)
   {

   }

   @Override
   public void contextLoaded(ConfigurableApplicationContext context)
   {

   }

   @Override
   public void started(ConfigurableApplicationContext context)
   {

   }

   @Override
   public void running(ConfigurableApplicationContext context)
   {

   }

   @Override
   public void failed(ConfigurableApplicationContext context, Throwable exception)
   {

   }


   private IniFilePropertySource readProperties(Environment environment, String name)
   {
      final File file = new File(environment.getProperty(SM_CONFIG_DIR), name + CONFIG_SUFFIX);
      if (file.exists() && file.canRead())
      {
         try (InputStream in = new FileInputStream(file);
              Reader reader = new InputStreamReader(in, "UTF-8"))
         {
            return new IniFilePropertySource(name, new IniFileReader().read(reader), environment.getActiveProfiles());
         }
         catch (IOException ex)
         {
            LOG.error("Unable to read configuration file {}", file, ex);
         }
      }
      return null;
   }
}

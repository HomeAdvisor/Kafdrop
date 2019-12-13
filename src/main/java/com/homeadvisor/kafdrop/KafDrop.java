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

package com.homeadvisor.kafdrop;

import com.google.common.base.Throwables;
import org.apache.logging.log4j.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.io.File;
import java.lang.management.ManagementFactory;

@SpringBootApplication
public class KafDrop
{
   private final static Logger LOG = LoggerFactory.getLogger(KafDrop.class);

   public static void main(String[] args)
   {
      new SpringApplicationBuilder(KafDrop.class)
         .bannerMode(Banner.Mode.OFF)
         .listeners(new LoggingConfigurationListener())
         .run(args);
   }

   private static class LoggingConfigurationListener implements ApplicationListener<ApplicationEnvironmentPreparedEvent>, Ordered
   {
      private static final String PROP_LOGGING_CONFIG = "logging.config";
      private static final String PROP_LOGGING_FILE = "logging.file";
      private static final String PROP_LOGGER = "LOGGER";
      private static final String PROP_SPRING_BOOT_LOG_LEVEL = "logging.level.org.springframework.boot";

      @Override
      public int getOrder()
      {
         // LoggingApplicationListener runs at HIGHEST_PRECEDENCE + 11.  This needs to run before that.
         return Ordered.HIGHEST_PRECEDENCE;
      }

      @Override
      public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event)
      {
         Environment environment = event.getEnvironment();
         final String loggingFile = environment.getProperty(PROP_LOGGING_FILE);
         if (loggingFile != null)
         {
            System.setProperty(PROP_LOGGER, "FILE");
            try
            {
               System.setProperty("logging.dir", new File(loggingFile).getParent());
            }
            catch (Exception ex)
            {
               System.err.println("Unable to set up logging.dir from logging.file " + loggingFile + ": " +
                                  Throwables.getStackTraceAsString(ex));
            }
         }
         if (environment.containsProperty("debug") &&
             !"false".equalsIgnoreCase(environment.getProperty("debug", String.class)))
         {
            System.setProperty(PROP_SPRING_BOOT_LOG_LEVEL, "DEBUG");
         }
         setProccessId();

      }

      private void setProccessId()
      {
         try
         {
            ThreadContext.put("PID", ManagementFactory.getRuntimeMXBean().getName().replaceAll("@.*", ""));
         }
         catch(Exception e)
         {
            LOG.warn("Unable to determine process ID, setting to -1");
            ThreadContext.put("PID", "-1");
         }
      }


   }

   @Bean
   public WebMvcConfigurerAdapter webConfig()
   {
      return new WebMvcConfigurerAdapter()
      {
         @Override
         public void configureContentNegotiation(ContentNegotiationConfigurer configurer)
         {
            super.configureContentNegotiation(configurer);
            configurer.favorPathExtension(false);
         }
      };
   }
}

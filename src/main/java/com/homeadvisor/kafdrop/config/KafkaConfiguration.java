package com.homeadvisor.kafdrop.config;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kafka.clients.*;
import org.apache.kafka.common.config.*;
import org.slf4j.*;
import org.springframework.boot.context.properties.*;
import org.springframework.stereotype.*;

import java.io.*;
import java.util.*;

@Component
@ConfigurationProperties(prefix = "kafka")
public final class KafkaConfiguration
{
   private static final Logger LOG = LoggerFactory.getLogger(KafkaConfiguration.class);

   public  static final int DEFAULT_REQUEST_TIMEOUT_MS = 10_000;

   private String brokers;
   private boolean secured;
   private String saslMechanism;
   private String securityProtocol;
   private String truststoreLocation;
   private String keystoreLocation;

   private PoolProperties adminPool = new PoolProperties();
   private PoolProperties consumerPool = new PoolProperties();
   private Map<String, String> additionalProperties = new HashMap<>();

   private int requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS;

   public int getRequestTimeoutMs()
   {
      return requestTimeoutMs;
   }

   public void setRequestTimeoutMs(int requestTimeoutMs)
   {
      this.requestTimeoutMs = requestTimeoutMs;
   }

   public String getBrokers()
   {
      return brokers;
   }

   public void setBrokers(String brokers)
   {
      this.brokers = brokers;
   }

   public boolean isSecured()
   {
      return secured;
   }

   public void setSecured(boolean secured)
   {
      this.secured = secured;
   }

   public String getSaslMechanism()
   {
      return saslMechanism;
   }

   public void setSaslMechanism(String saslMechanism)
   {
      this.saslMechanism = saslMechanism;
   }

   public String getSecurityProtocol()
   {
      return securityProtocol;
   }

   public void setSecurityProtocol(String securityProtocol)
   {
      this.securityProtocol = securityProtocol;
   }

   public String getTruststoreLocation()
   {
      return truststoreLocation;
   }

   public void setTruststoreLocation(String truststoreLocation)
   {
      this.truststoreLocation = truststoreLocation;
   }

   public String getKeystoreLocation()
   {
      return keystoreLocation;
   }

   public void setKeystoreLocation(String keystoreLocation)
   {
      this.keystoreLocation = keystoreLocation;
   }

   public Map<String, String> getAdditionalProperties()
   {
      return additionalProperties;
   }

   public void setAdditionalProperties(Map<String, String> additionalProperties)
   {
      this.additionalProperties.putAll(additionalProperties);
   }

   public PoolProperties getAdminPool()
   {
      return adminPool;
   }

   public PoolProperties getConsumerPool()
   {
      return consumerPool;
   }

   /**
    * Builds a Properties object that contains the common configuration needed to connect to kafka, regardless
    * of which client (admin vs. consumer vs. producer) that is used.
    *
    * @param properties
    */
   public void applyCommon(Properties properties)
   {
      properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
      if (secured)
      {
         LOG.warn(
            "The 'isSecured' property is deprecated; consult README.md on the preferred way to configure security");
         properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
         properties.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
      }

      if (StringUtils.isNotBlank(truststoreLocation))
      {
         File truststoreFile = new File(truststoreLocation);
         if (truststoreFile.isFile() && truststoreFile.canRead())
         {
            LOG.info("Assigning truststore location to {}", truststoreLocation);
            properties.put("ssl.truststore.location", truststoreLocation);
         }
         else
         {
            throw new IllegalArgumentException("Truststore cannot be read: " + truststoreLocation);
         }
      }

      if (StringUtils.isNotBlank(keystoreLocation))
      {
         File keystoreFile = new File(keystoreLocation);
         if (keystoreFile.isFile() && keystoreFile.canRead())
         {
            LOG.info("Assigning keystore location to {}", keystoreLocation);
            properties.put("ssl.keystore.location", keystoreLocation);
         }
         else
         {
            throw new IllegalArgumentException("Keystore cannot be read: " + keystoreLocation);
         }
      }

      additionalProperties.forEach((k, v) -> properties.setProperty(k, v));
   }

   public static class PoolProperties
   {
      private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
      private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
      private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
      private long maxWaitMillis = GenericKeyedObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS;

      public int getMinIdle()
      {
         return minIdle;
      }

      public void setMinIdle(int minIdle)
      {
         this.minIdle = minIdle;
      }

      public int getMaxIdle()
      {
         return maxIdle;
      }

      public void setMaxIdle(int maxIdle)
      {
         this.maxIdle = maxIdle;
      }

      public int getMaxTotal()
      {
         return maxTotal;
      }

      public void setMaxTotal(int maxTotal)
      {
         this.maxTotal = maxTotal;
      }

      public long getMaxWaitMillis()
      {
         return maxWaitMillis;
      }

      public void setMaxWaitMillis(long maxWaitMillis)
      {
         this.maxWaitMillis = maxWaitMillis;
      }
   }

}

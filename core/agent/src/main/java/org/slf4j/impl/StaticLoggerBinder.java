/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.slf4j.impl;

import org.apache.spark.raydp.Agent;
import org.apache.spark.raydp.SparkOnRayConfigs;
import org.slf4j.ILoggerFactory;
import org.slf4j.spi.LoggerFactoryBinder;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

/**
 * A delegation class to bind to slf4j so that we can have a chance to choose
 * which underlying log4j framework to use.
 */
public class StaticLoggerBinder implements LoggerFactoryBinder {

  // for compatibility check
  public static final String REQUESTED_API_VERSION;

  private static final Class<?> LOGFACTORY_CLASS;

  private static final ILoggerFactory FACTORY;

  private static final StaticLoggerBinder _INSTANCE = new StaticLoggerBinder();

  private static final Map<String, String> LOG_FACTORY_CLASSES
      = new HashMap<>();

  private static PrintStream subSystemErr;

  private static PrintStream subSystemOut;

  static {
    subSystemErr = System.err;
    subSystemOut = System.out;

    LOG_FACTORY_CLASSES.put("log4j",
        "org.slf4j.impl.Log4jLoggerFactory"); // log4j 1
    LOG_FACTORY_CLASSES.put("log4j2",
        "org.apache.logging.slf4j.Log4jLoggerFactory"); // log4j 2

    String factoryClzStr = System
        .getProperty(SparkOnRayConfigs.LOG4J_FACTORY_CLASS_KEY, "");
    if (factoryClzStr.length() == 0) {
      System.err.println("ERROR: system property '"
          + SparkOnRayConfigs.LOG4J_FACTORY_CLASS_KEY
          + "' needs to be specified for slf4j binding");
      LOGFACTORY_CLASS = null;
      FACTORY = null;
    } else {
      String mappedClsStr = LOG_FACTORY_CLASSES.get(factoryClzStr);
      if (mappedClsStr == null) {
        mappedClsStr = factoryClzStr;
      }
      // restore to system default stream so that log4j console appender
      // can be correctly set
      System.setErr(Agent.DEFAULT_ERR_PS);
      System.setOut(Agent.DEFAULT_OUT_PS);
      Class<?> tempClass = null;
      try {
        tempClass = Class.forName(mappedClsStr);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        LOGFACTORY_CLASS = tempClass;
      }
      StringBuilder sb = new StringBuilder();
      sb.append("mapped factory class: ").append(mappedClsStr)
          .append(". load ");
      if (LOGFACTORY_CLASS != null) {
        sb.append(LOGFACTORY_CLASS.getName());
        try {
          String loc = LOGFACTORY_CLASS.getProtectionDomain().getCodeSource()
              .getLocation().toURI().toString();
          sb.append(" from ").append(loc);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        sb.append("failed");
      }

      ILoggerFactory tmpFactory = null;
      try {
        tmpFactory = (ILoggerFactory) tempClass.newInstance();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        FACTORY = tmpFactory;
      }
      // set to substitute stream for capturing remaining logs
      System.setErr(subSystemErr);
      System.setOut(subSystemOut);
      System.out.println(sb);
    }
    REQUESTED_API_VERSION = "1.6.66";
  }

  public static final StaticLoggerBinder getSingleton() {
    return _INSTANCE;
  }

  @Override
  public ILoggerFactory getLoggerFactory() {
    // restore to system default stream so that log4j console appender
    // can be correctly set
    if (System.out != Agent.DEFAULT_OUT_PS) {
      System.setOut(Agent.DEFAULT_OUT_PS);
    }
    if (System.err != Agent.DEFAULT_ERR_PS) {
      System.setErr(Agent.DEFAULT_ERR_PS);
    }
    return FACTORY;
  }

  @Override
  public String getLoggerFactoryClassStr() {
    return LOGFACTORY_CLASS.getName();
  }
}

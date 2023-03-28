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

package org.apache.spark.raydp;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;


public class Agent {

  public static final PrintStream DEFAULT_ERR_PS = System.err;

  public static final PrintStream DEFAULT_OUT_PS = System.out;

  public static void premain(String agentArgs, Instrumentation inst)
      throws IOException {
    // redirect system output/error stream so that annoying SLF4J warnings
    // and other logs during binding
    // SLF4J factory don't show in spark-shell
    // Instead, the warnings and logs are kept in
    // <ray session>/logs/slf4j-<worker pid>.log

    String pid = ManagementFactory.getRuntimeMXBean().getName()
        .split("@")[0];
    String logDir = System.getProperty("ray.logging.dir");
    if (logDir == null) {
      logDir = "/tmp/ray/session_latest/logs";
      System.getProperties().put("ray.logging.dir", logDir);
    }

    File parentDir = new File(logDir);
    if (!parentDir.exists()) {
      parentDir.mkdirs();
    }

    File logFile = new File(parentDir, "/slf4j-" + pid + ".log");
    try (PrintStream ps = new PrintStream(logFile)) {
      System.setOut(ps);
      System.setErr(ps);
      // slf4j binding
      LoggerFactory.getLogger(Agent.class);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      System.out.flush();
      System.err.flush();
      // restore system output/error stream
      System.setErr(DEFAULT_ERR_PS);
      System.setOut(DEFAULT_OUT_PS);
    }
    String jobId = System.getenv("RAY_JOB_ID");
    String rayAddress = System.getProperty("ray.address");
    if (jobId != null && rayAddress != null) {
      try (FileWriter writer = new FileWriter(logDir + "/java-worker-" + jobId + "-" + pid + ".log")) {
        writer.write(":job_id:" + jobId + "\n");
      }
    }
  }
}

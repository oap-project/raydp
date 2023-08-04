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

import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;


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
      boolean flag = parentDir.mkdirs();
      if (!flag) {
        throw new RuntimeException("Error create log dir.");
      }
    }

    File logFile = new File(parentDir, "/slf4j-" + pid + ".log");
    try (PrintStream ps = new PrintStream(logFile, "UTF-8")) {
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
    // below is to write ':job_id:<jobid>' to first line of log file prefixed with 'java-worker' as required by
    // PR, https://github.com/ray-project/ray/pull/31772.
    // It's a workaround of the ray 2.3.[0-1] issue going to be fixed by https://github.com/ray-project/ray/pull/33665.
    String jobId = System.getenv("RAY_JOB_ID");
    String rayAddress = System.getProperty("ray.address");
    if (jobId != null && rayAddress != null) {
      String prefix = "java-worker";
      // TODO: uncomment after the ray PR #33665 released
      // String prefix = System.getProperty("ray.logging.file-prefix", "java-worker");
      // if ("java-worker".equals(prefix)) {
      File file = new File(new String((logDir + "/" + prefix + "-" + jobId + "-" + pid + ".log")
        .getBytes(Charset.forName("UTF-8")), "UTF-8"));
      try (OutputStream out = new FileOutputStream(file);
          Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
        writer.write(":job_id:" + jobId + "\n");
      }
      // }
    }
  }
}

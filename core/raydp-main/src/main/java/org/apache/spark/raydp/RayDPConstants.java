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

public class RayDPConstants {

    public final static String SPARK_JAVAAGENT = "spark.javaagent";

    public static final String LOG4J_FACTORY_CLASS_KEY = "spark.ray.log4j.factory.class";

    public static final String RAY_LOG4J_CONFIG_FILE_NAME = "log4j2.configurationFile";

    public static final String RAY_LOG4J_CONFIG_FILE_NAME_KEY = "spark.ray.log4j.config.file.name";

    public static final String SPARK_DRIVER_EXTRA_JAVA_OPTIONS = "spark.driver.extraJavaOptions";
}
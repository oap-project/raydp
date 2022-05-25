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

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.ObjectId;
import io.ray.runtime.AbstractRayRuntime;
import io.ray.runtime.object.ObjectRefImpl;

public class RayDPUtils {

  /**
   * Convert ObjectRef to subclass ObjectRefImpl. Throw RuntimeException if it is not instance
   * of ObjectRefImpl. We can't import the ObjectRefImpl in scala code, so we do the
   * conversion at here.
   */
  public static <T> ObjectRefImpl<T> convert(ObjectRef<T> obj) {
    if (obj instanceof ObjectRefImpl) {
      return (ObjectRefImpl<T>)obj;
    } else {
      throw new RuntimeException(obj.getClass() + " is not ObjectRefImpl");
    }
  }

  /**
   * Create ObjectRef from Array[Byte] and register ownership.
   * We can't import the ObjectRefImpl in scala code, so we do the conversion at here.
   */
  public static <T> ObjectRef<T> readBinary(byte[] obj, Class<T> clazz, byte[] ownerAddress) {
    ObjectId id = new ObjectId(obj);
    ObjectRefImpl<T> ref = new ObjectRefImpl<>(id, clazz);
    ((AbstractRayRuntime) Ray.internal()).getObjectStore()
        .registerOwnershipInfoAndResolveFuture(id, null, ownerAddress);
    return ref;
  }
}

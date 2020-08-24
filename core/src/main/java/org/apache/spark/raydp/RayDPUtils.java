package org.apache.spark.raydp;
import io.ray.api.ObjectRef;
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
}

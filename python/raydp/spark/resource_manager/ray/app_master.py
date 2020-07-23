from jnius import JavaClass, JavaMethod, MetaJavaClass


class AppMaster(JavaClass, metaclass=MetaJavaClass):
    __javaclass__ = 'org/apache/spark/raydp/PyjniusBridge'

    createAppMaster = JavaMethod("(Ljava/lang/String)V")
    getMasterUrl = JavaMethod("()Ljava/lang/String")
    stop = JavaMethod("()V")


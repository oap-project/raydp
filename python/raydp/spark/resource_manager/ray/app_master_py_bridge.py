from jnius import JavaClass, JavaMethod, MetaJavaClass


class AppMasterPyBridge(JavaClass, metaclass=MetaJavaClass):
    __javaclass__ = 'org/apache/spark/raydp/AppMasterJavaBridge'

    createAppMaster = JavaMethod("(Ljava/lang/String;)V")
    getMasterUrl = JavaMethod("()Ljava/lang/String;")
    stop = JavaMethod("()V")


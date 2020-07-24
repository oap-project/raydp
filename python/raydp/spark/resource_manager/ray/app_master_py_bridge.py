from jnius import autoclass


class AppMasterPyBridge:
    def __init__(self, jvm_properties):
        app_master_cls = autoclass("org.apache.spark.raydp.AppMasterJavaBridge")
        self._app_master = app_master_cls(jvm_properties)

    def create_app_master(self, extra_cp):
        self._app_master.createAppMaster(extra_cp)

    def get_master_url(self):
        return self._app_master.getMasterUrl()

    def stop(self):
        self._app_master.stop()



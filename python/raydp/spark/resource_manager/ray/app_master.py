import ray


class AppMaster:
    def __init__(self):
        self._java_app_master_actor = None

    def startup_java_app_master(self):
        if self._java_app_master_actor is not None:
            self._java_app_master_actor = ray.java_actor_class(
                "org.apache.spark.deploy.raydp.RayAppMaster")
            self._java_app_master_actor.remote()

    def get_master_url(self):
        assert self._java_app_master_actor is not None, "AppMaster has not started up"
        return ray.get(self._java_app_master_actor.getMasterUrl.remote())

    def stop(self):
        if self._java_app_master_actor is not None:
            ray.kill(self._java_app_master_actor)
            self._java_app_master_actor = None

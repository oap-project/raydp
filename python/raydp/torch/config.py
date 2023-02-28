from ray.train.torch.config import _TorchBackend
from ray.train.torch.config import TorchConfig as RayTorchConfig
from ray.train._internal.worker_group import WorkerGroup
from dataclasses import dataclass

@dataclass
class TorchConfig(RayTorchConfig):

    @property
    def backend_cls(self):
        return EnableCCLBackend

def ccl_import():
    try:
        # pylint: disable=import-outside-toplevel
        import oneccl_bindings_for_pytorch
    except ImportError as ccl_not_exist:
        raise ImportError(
            "Please install torch-ccl"
        ) from ccl_not_exist

class EnableCCLBackend(_TorchBackend):

    def on_start(self, worker_group: WorkerGroup, backend_config: RayTorchConfig):
        for i in range(len(worker_group)):
            worker_group.execute_single_async(i, ccl_import)
        super().on_start(worker_group, backend_config)

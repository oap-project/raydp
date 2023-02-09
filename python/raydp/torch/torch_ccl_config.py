from ray.train.torch.config import _TorchBackend
from ray.train.torch.config import TorchConfig
from ray.train._internal.worker_group import WorkerGroup
from dataclasses import dataclass


@dataclass
class CCLConfig(TorchConfig):

    @property
    def backend_cls(self):
        return EnableCCLBackend

def ccl_import():
    # pylint: disable=import-outside-toplevel
    import oneccl_bindings_for_pytorch

class EnableCCLBackend(_TorchBackend):

    def on_start(self, worker_group: WorkerGroup, backend_config: TorchConfig):
        for i in range(len(worker_group)):
            worker_group.execute_single_async(i, ccl_import)
        super().on_start(worker_group, backend_config)

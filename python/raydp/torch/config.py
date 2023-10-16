from ray.train.torch.config import _TorchBackend
from ray.train.torch.config import TorchConfig as RayTorchConfig
from ray.train._internal.worker_group import WorkerGroup
from dataclasses import dataclass
import sys
# The package importlib_metadata is in a different place, depending on the Python version.
if sys.version_info < (3, 8):
    import importlib_metadata
else:
    import importlib.metadata as importlib_metadata

@dataclass
class TorchConfig(RayTorchConfig):

    @property
    def backend_cls(self):
        return EnableCCLBackend

def libs_import():
    """try to import IPEX and oneCCL.
    """
    try:
        import intel_extension_for_pytorch
    except ImportError:
        raise ImportError(
            "Please install intel_extension_for_pytorch"
        )
    try:
        ccl_version = importlib_metadata.version("oneccl_bind_pt")
        if ccl_version >= "1.12":
            # pylint: disable-all
            import oneccl_bindings_for_pytorch
        else:
            import torch_ccl
    except ImportError as ccl_not_exist:
        raise ImportError(
            "Please install torch-ccl"
        ) from ccl_not_exist

class EnableCCLBackend(_TorchBackend):

    def on_start(self, worker_group: WorkerGroup, backend_config: RayTorchConfig):
        for i in range(len(worker_group)):
            worker_group.execute_single_async(i, libs_import)
        super().on_start(worker_group, backend_config)

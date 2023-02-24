from ray.train.torch.config import _TorchBackend
from ray.train.torch.config import TorchConfig as RayTorchConfig
from ray.train._internal.worker_group import WorkerGroup
from dataclasses import dataclass
from packaging import version
import importlib_metadata

@dataclass
class TorchConfig(RayTorchConfig):

    @property
    def backend_cls(self):
        return EnableCCLBackend

def ccl_import():
    # pylint: disable=import-outside-toplevel
    import oneccl_bindings_for_pytorch

class EnableCCLBackend(_TorchBackend):

    def on_start(self, worker_group: WorkerGroup, backend_config: RayTorchConfig):
        for i in range(len(worker_group)):
            worker_group.execute_single_async(i, ccl_import)
        super().on_start(worker_group, backend_config)

def check_ipex():
    def get_major_and_minor_from_version(full_version):
        return str(version.parse(full_version).major) + "." + str(version.parse(full_version).minor)

    try:
        _torch_version = importlib_metadata.version("torch")
    except importlib_metadata.PackageNotFoundError:
        return None, None

    try:
        _ipex_version = importlib_metadata.version("intel_extension_for_pytorch")
    except importlib_metadata.PackageNotFoundError:
        return _torch_version, None

    torch_major_and_minor = get_major_and_minor_from_version(_torch_version)
    ipex_major_and_minor = get_major_and_minor_from_version(_ipex_version)
    return torch_major_and_minor , ipex_major_and_minor

import logging

import ray
from ray.util.sgd.torch.distributed_torch_runner import (
    DistributedTorchRunner, LocalDistributedRunner)
from ray.util.sgd.torch.torch_runner import TorchRunner
from ray.util.sgd.torch.torch_trainer import TorchTrainer
from ray.util.sgd.utils import BATCH_SIZE

logger = logging.getLogger(__name__)


class TorchTrainerWithResourceSpecification(TorchTrainer):
    def __init__(self,
                 num_cpus: int = 1,
                 **kwargs):
        self._num_cpus_for_worker = num_cpus

        super(TorchTrainerWithResourceSpecification, self).__init__(**kwargs)

    def _start_workers(self, num_workers):
        logger.debug(f"start_workers: Setting %d workers." % num_workers)
        worker_config = self.config.copy()
        batch_size_per_worker = self._configure_and_split_batch(num_workers)
        if batch_size_per_worker:
            worker_config[BATCH_SIZE] = batch_size_per_worker

        params = dict(
            model_creator=self.model_creator,
            data_creator=self.data_creator,
            optimizer_creator=self.optimizer_creator,
            loss_creator=self.loss_creator,
            scheduler_creator=self.scheduler_creator,
            training_operator_cls=self.training_operator_cls,
            config=worker_config,
            serialize_data_creation=self.serialize_data_creation,
            use_fp16=self.use_fp16,
            use_gpu=self.use_gpu,
            use_tqdm=self.use_tqdm,
            apex_args=self.apex_args,
            scheduler_step_freq=self.scheduler_step_freq)

        if num_workers == 1:
            # Start local worker
            self.local_worker = TorchRunner(**params)
            if self.initialization_hook:
                self.apply_all_workers(self.initialization_hook)
            self.local_worker.setup()
        else:
            params.update(
                backend=self.backend,
                add_dist_sampler=self.add_dist_sampler,
                wrap_ddp=self.wrap_ddp)

            # Start local worker
            self.local_worker = LocalDistributedRunner(
                num_cpus=self._num_cpus_for_worker, num_gpus=int(self.use_gpu), **params)

            # Generate actor class
            RemoteRunner = ray.remote(
                num_cpus=self._num_cpus_for_worker, num_gpus=int(self.use_gpu))(
                DistributedTorchRunner)
            # Start workers
            self.remote_workers = [
                RemoteRunner.remote(**params) for i in range(num_workers - 1)
            ]
            if self.initialization_hook:
                self.apply_all_workers(self.initialization_hook)

            # Compute URL for initializing distributed PyTorch
            ip = ray.services.get_node_ip_address()
            port = self.local_worker.find_free_port()

            address = "tcp://{ip}:{port}".format(ip=ip, port=port)

            # Runs the creator functions.
            remote_component_setup = [
                worker.setup_components.remote()
                for i, worker in enumerate(self.remote_workers)
            ]
            self.local_worker.setup_components()
            # Get setup tasks in order to throw errors on failure
            ray.get(remote_component_setup)

            # Setup the process group among all workers.
            remote_pgroup_setups = [
                worker.setup_process_group.remote(address, i + 1, num_workers)
                for i, worker in enumerate(self.remote_workers)
            ]
            self.local_worker.setup_process_group(address, 0, num_workers)
            # Get setup tasks in order to throw errors on failure
            ray.get(remote_pgroup_setups)

            # Runs code that requires all creator functions to have run.
            remote_operator_setups = [
                worker.setup_ddp_and_operator.remote()
                for worker in self.remote_workers
            ]
            self.local_worker.setup_ddp_and_operator()
            # Get setup tasks in order to throw errors on failure
            ray.get(remote_operator_setups)

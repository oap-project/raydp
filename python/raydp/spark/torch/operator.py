from ray.util.sgd.torch.training_operator import TrainingOperator
import ray


class TrainingOperatorWithWarmUp(TrainingOperator):
    def __init__(self,
                 config,
                 models,
                 optimizers,
                 train_loader,
                 validation_loader,
                 world_rank,
                 criterion=None,
                 schedulers=None,
                 device_ids=None,
                 use_gpu=False,
                 use_fp16=False,
                 use_tqdm=False):
        super(TrainingOperatorWithWarmUp, self).__init__(
            config=config, models=models, optimizers=optimizers, train_loader=train_loader,
            validation_loader=validation_loader, world_rank=world_rank, criterion=criterion,
            schedulers=schedulers, device_ids=device_ids, use_gpu=use_gpu, use_fp16=use_fp16,
            use_tqdm=use_tqdm)

    def setup(self, config):
        """We trigger the underlying object transfer before training startup"""
        train_loader = self.train_loader
        plasma_store_path = ray.worker.global_worker.node.plasma_store_socket_name
        train_loader.sampler.resolve(plasma_store_path)

    def train_epoch(self, iterator, info):
        if self.timers is not None:
            iterator = self._profile_data_loader(iterator)
        return super(TrainingOperatorWithWarmUp, self).train_epoch(iterator, info)

    def _profile_data_loader(self, iterator):
        outer = self

        class DataLoaderWithProfile:
            def __init__(self, iterator):
                self._iterator = iterator
                self._underlying_iterator = None

            def __iter__(self):
                if self._underlying_iterator is None:
                    self._underlying_iterator = iter(self._iterator)
                return self

            def __next__(self):
                with outer.timers.record("data_load"):
                    return next(self._underlying_iterator)

            def __len__(self):
                return len(iterator)

        return DataLoaderWithProfile(iterator)

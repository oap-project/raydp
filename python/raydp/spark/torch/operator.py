from ray.util.sgd.torch.training_operator import TrainingOperator


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
        train_loader.sampler.resolve()

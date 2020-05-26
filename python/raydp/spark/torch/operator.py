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
        super(TrainingOperator, self).__init__(config, models, optimizers, train_loader,
                                               validation_loader, world_rank, criterion,
                                               schedulers, device_ids, use_gpu, use_fp16,
                                               use_tqdm)

    def setup(self, config):
        """We trigger the underlying object transfer before training startup"""
        train_loader = self.train_loader
        train_loader.sampler.resolve()

import torchmetrics
import sys
module = sys.modules[__name__]

class Torch_Metric():
    def __init__(self, metrics_name):
        self._metrics_name = metrics_name
        self._preprocess_fun = {}
        self._metrics_fun = {}
        if self._metrics_name is not None:
            for metric in self._metrics_name:
                if callable(metric):
                    self._preprocess_fun[metric.__name__] = None
                    self._metrics_fun[metric.__name__] = metric()
                else:
                    self._preprocess_fun[metric] = getattr(module, 'pre'+metric, None)
                    self._metrics_fun[metric] = getattr(torchmetrics, metric)()

    def update(self, preds, targets):
        for metric in self._metrics_fun:
            pre_func = self._preprocess_fun[metric]
            if pre_func is not None:
                preds, targets = pre_func(preds, targets)
            self._metrics_fun[metric].update(preds, targets)

    def compute(self):
        epoch_res = {}
        for metric in self._metrics_fun:
            epoch_res[metric] = self._metrics_fun[metric].compute().item()

        return epoch_res

    def reset(self):
        for metric in self._metrics_fun:
            self._metrics_fun[metric].reset()

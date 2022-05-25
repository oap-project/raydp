import torchmetrics
import sys
module = sys.modules[__name__]

class Torch_Metric():
    def __init__(self, metrics_name, metrics_config):
        self._metrics_name = metrics_name
        self._preprocess_fun = {}
        self._metrics_fun = {}
        if self._metrics_name is not None:
            assert isinstance(metrics_name, list), "metrics_name must be a list"
            for metric in self._metrics_name:
                if isinstance(metric, torchmetrics.Metric):
                    self._preprocess_fun[metric.__class__.__name__] = None
                    self._metrics_fun[metric.__class__.__name__] = metric
                elif isinstance(metric, str) and hasattr(torchmetrics, metric):
                    self._preprocess_fun[metric] = getattr(module, "pre"+metric, None)
                    if metrics_config is not None and metrics_config[metric] is not None:
                        self._metrics_fun[metric] = getattr(torchmetrics, metric)(
                                                            **metrics_config[metric])
                    else:
                        self._metrics_fun[metric] = getattr(torchmetrics, metric)()
                else:
                    raise Exception(
                        "Unsupported parameter, we only support list of "
                        "torchmetrics.Metric instances or arr of torchmetrics.")

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

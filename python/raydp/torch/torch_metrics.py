import sys
module = sys.modules[__name__]

def try_import_torchmetrics():
    """Tries importing torchmetrics and returns the module (or None).
    Returns:
        torchmetrics modules.
    """
    try:
        # pylint: disable=import-outside-toplevel
        import torchmetrics

        return torchmetrics
    except ImportError as torchmetrics_not_exist:
        raise ImportError(
            "Could not import torchmetrics! Raydp TorchEstimator requires "
            "you to install torchmetrics: "
            "`pip install torchmetrics`."
        ) from torchmetrics_not_exist

class TorchMetric():
    def __init__(self, metrics_name, metrics_config):
        torchmetrics = try_import_torchmetrics()
        self._metrics_name = metrics_name
        self._metrics_func = {}
        if self._metrics_name is not None:
            assert isinstance(metrics_name, list), "metrics_name must be a list"
            for metric in self._metrics_name:
                if isinstance(metric, torchmetrics.Metric):
                    self._metrics_func[metric.__class__.__name__] = metric
                elif isinstance(metric, str) and hasattr(torchmetrics, metric):
                    if metrics_config is not None and metrics_config[metric] is not None:
                        self._metrics_func[metric] = getattr(torchmetrics, metric)(
                                                            **metrics_config[metric])
                    else:
                        self._metrics_func[metric] = getattr(torchmetrics, metric)()
                else:
                    raise Exception(
                        "Unsupported parameter, we only support list of "
                        "torchmetrics.Metric instances or arr of torchmetrics.")

    def update(self, preds, targets):
        for metric in self._metrics_func:
            self._metrics_func[metric].update(preds, targets)

    def compute(self):
        epoch_res = {}
        for metric in self._metrics_func:
            epoch_res[metric] = self._metrics_func[metric].compute().item()

        return epoch_res

    def reset(self):
        for metric in self._metrics_func:
            self._metrics_func[metric].reset()

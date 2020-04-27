from raydp.spark.dataholder import ObjectIdList

import torch

from typing import List


class RayDataset(torch.utils.data.IterableDataset):
    def __init__(self,
                 objs: ObjectIdList,
                 features_columns: List[str],
                 label_column: str):
        super(RayDataset, self).__init__()
        self._objs = objs
        self._feature_columns = features_columns
        self._label_column = label_column
        self._df_index = 0
        self._index = 0
        self._feature_df = None
        self._label_df = None

    def __iter__(self):
        worker_info = torch.utils.data.get_worker_info()
        if worker_info:
            # TODO: add support
            raise Exception("Multiple processes loading is not supported")

        self._objs.resolve(True)

        # TODO: should this too slowly?
        if self._feature_df is None or self._index >= len(self._feature_df):
            # the first call or reach the end of the current df
            self._df_index = 0 if self._feature_df is None else self._df_index + 1
            if self._df_index >= len(self._objs):
                self._df_index = 0

            df = self._objs[self._df_index]
            self._feature_df = df[self._feature_columns].values
            self._label_df = df[self._label_column].values
            self._index = 0

        result = torch.from_numpy(self._feature_df[self._index], dtype=torch.float), \
                 torch.tensor(self._label_df[self._index], dtype=torch.float)
        self._index += 1
        return result

    def __len__(self):
        return self._objs.total_size


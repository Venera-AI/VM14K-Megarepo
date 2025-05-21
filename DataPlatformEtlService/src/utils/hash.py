import hashlib
from typing import Any, List
import numpy as np
import pandas as pd
from src.custom_types.common import BaseDict


class HashUtils:
    @classmethod
    def hash_data(
        cls,
        data: List[BaseDict] | pd.DataFrame | Any,
        include_fields,
        order_bys=[],
        ascending=(),
        drop_duplicate=False,
        hash_column_name="__hashID",
    ) -> pd.DataFrame:
        df = pd.DataFrame(data, dtype=np.dtype("O")).sort_values(by=order_bys, ascending=ascending)
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        selected_df = df.copy()
        selected_columns = include_fields.copy()
        selected_columns.sort()
        if hash_column_name in selected_columns:
            raise ValueError("include_fields contain hash column")
        if hash_column_name in df.columns:
            raise ValueError("input data contain hash column")

        for column in selected_columns:
            if column not in selected_df.columns:
                selected_df[column] = None
        selected_df = selected_df[selected_columns].copy()
        selected_df[hash_column_name] = selected_df.apply(
            lambda row: hashlib.md5("_".join(row.fillna("").values.astype(str)).encode()).hexdigest(), axis=1
        )
        df[hash_column_name] = selected_df[hash_column_name]
        if drop_duplicate:
            df = df.drop_duplicates(subset=[hash_column_name], keep="last")
        return df

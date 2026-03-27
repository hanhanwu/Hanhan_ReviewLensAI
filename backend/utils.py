from __future__ import annotations

from io import BytesIO
from typing import Tuple

import pandas as pd


def count_csv_dimensions(raw_bytes: bytes) -> Tuple[int, int]:
    """Return the number of rows and columns in the provided CSV data."""
    if not raw_bytes:
        return 0, 0

    dataframe = pd.read_csv(BytesIO(raw_bytes))
    return dataframe.shape

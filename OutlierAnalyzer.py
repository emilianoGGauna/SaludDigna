import logging
from typing import Dict, Optional, List, Tuple, Union

import pandas as pd
import numpy as np
from scipy.stats import zscore
import plotly.express as px
from plotly.graph_objs import Figure
from DatabaseEDA import DatabaseEDA

# Configure logger once at module level
def get_logger(name: str = __name__) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

class OutlierAnalyzer:
    """
    Utilities for detecting, summarizing, and removing outliers in DataFrames.
    Supports IQR and Z-score methods, full-table summaries, and violin visualizations.
    """
    def __init__(
        self,
        env_path: Optional[str] = None,
        default_method: str = 'iqr',
        default_k: float = 1.5,
        default_threshold: float = 3.0,
    ):
        self.db = DatabaseEDA(env_path)
        self.engine = self.db.engine
        self.method = default_method
        self.k = default_k
        self.threshold = default_threshold
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized OutlierAnalyzer with %s", default_method)

    def detect_outliers(
        self,
        series: pd.Series,
        method: Optional[str] = None,
        k: Optional[float] = None,
        threshold: Optional[float] = None,
    ) -> pd.Series:
        """
        Return a boolean mask where True indicates an outlier in `series`.
        Numeric series is required; datetime series should be converted to int64 before passing.

        :param series: numeric pandas Series
        :param method: 'iqr' or 'zscore'
        :param k: multiplier for IQR
        :param threshold: z-score cutoff
        """
        method = method or self.method
        k = k or self.k
        threshold = threshold or self.threshold

        clean = series.dropna()
        if clean.empty:
            return pd.Series(False, index=series.index)

        if method == 'iqr':
            q1, q3 = clean.quantile([0.25, 0.75])
            iqr = q3 - q1
            lower, upper = q1 - k * iqr, q3 + k * iqr
            mask = series.lt(lower) | series.gt(upper)
        elif method == 'zscore':
            zs = pd.Series(zscore(clean, nan_policy='omit'), index=clean.index).abs()
            mask = pd.Series(False, index=series.index)
            mask.loc[zs.index] = zs.gt(threshold)
        else:
            raise ValueError(f"Unsupported method: {method}")

        self.logger.info(
            "%d outliers detected in '%s' using %s", mask.sum(), series.name, method
        )
        return mask

    def detect_all(
        self,
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
        **kwargs,
    ) -> Dict[str, pd.Series]:
        """
        Detect outliers across multiple numeric columns.

        :param df: DataFrame to analyze
        :param columns: list of numeric columns to include (defaults to all numeric cols)
        :return: dict mapping column -> boolean mask
        """
        cols = columns or df.select_dtypes(include=[np.number]).columns.tolist()
        masks = {col: self.detect_outliers(df[col], **kwargs) for col in cols}
        return masks

    def remove_outliers(
        self,
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Return a cleaned DataFrame with all rows containing outliers dropped.
        """
        masks = self.detect_all(df, columns, **kwargs)
        if not masks:
            self.logger.info("No numeric columns found for outlier removal.")
            return df.copy()

        combined = pd.concat(masks.values(), axis=1).any(axis=1)
        cleaned = df.loc[~combined].reset_index(drop=True)
        self.logger.info("Removed %d rows out of %d", combined.sum(), len(df))
        return cleaned

    def summary(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Load a database table and summarize outlier counts by column for both IQR and Z-score.
        """
        df = self.db.load_table(table_name)
        if df.empty:
            self.logger.warning("Table '%s' is empty.", table_name)
            return pd.DataFrame()

        cols = columns or df.select_dtypes(include=[np.number]).columns.tolist()
        summary = []
        for col in cols:
            iqr_count = self.detect_outliers(df[col], method='iqr', **kwargs).sum()
            zs_count = self.detect_outliers(df[col], method='zscore', **kwargs).sum()
            summary.append({
                'table': table_name,
                'column': col,
                'iqr_outliers': int(iqr_count),
                'zscore_outliers': int(zs_count),
            })
        result = pd.DataFrame(summary)
        self.logger.info("Summary generated for '%s': %d columns", table_name, len(cols))
        return result

    def plot_violin(
        self,
        df: pd.DataFrame,
        column: str,
        method: Optional[str] = None,
        **kwargs,
    ) -> Figure:
        """
        Create a violin plot highlighting outliers for specified column.
        """
        mask = self.detect_outliers(df[column], method=method, **kwargs)
        df_plot = df[[column]].copy()
        df_plot['state'] = np.where(mask, 'Outlier', 'Normal')
        fig = px.violin(
            df_plot,
            y=column,
            color='state',
            box=True,
            points='outliers',
            color_discrete_map={'Normal': 'royalblue', 'Outlier': 'firebrick'},
            title=f"{column} Outliers ({method or self.method})"
        )
        fig.update_layout(yaxis_title=column)
        return fig

    def generate_report(
        self,
        df: pd.DataFrame,
        fill_nulls: bool = True,
        subset: Optional[List[str]] = None,
        **kwargs,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Produce before/after DataFrame summary and cleaned DataFrame.
        """
        before = df.describe(include='all').T.assign(nulls=df.isna().sum())
        df_clean = df.copy()
        if fill_nulls:
            for col in df_clean.columns:
                if df_clean[col].isna().any():
                    if pd.api.types.is_numeric_dtype(df_clean[col]):
                        df_clean[col].fillna(df_clean[col].mean(), inplace=True)
                    else:
                        df_clean[col].fillna(df_clean[col].mode().iloc[0], inplace=True)
        df_cleaned = self.remove_outliers(df_clean, subset, **kwargs)
        after = df_cleaned.describe(include='all').T.assign(nulls=df_cleaned.isna().sum())
        report = pd.concat({'before': before, 'after': after}, axis=1)
        self.logger.info("Report: %d -> %d rows", len(df), len(df_cleaned))
        return report, df_cleaned

# TemporalCleaner.py
import pandas as pd
import numpy as np
from OutlierAnalyzer import OutlierAnalyzer
from pandas.api.types import is_datetime64_any_dtype
from typing import List, Tuple

class TemporalCleaner:
    def __init__(self):
        self.analyzer = OutlierAnalyzer()

    def clean_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Imputa nulos y elimina outliers, ignorando columnas que contengan 'id' en el nombre.
        """
        log: List[str] = []
        df_clean = df.copy()

        # Imputación de nulos
        for col in df.columns:
            col_lower = col.lower()
            # Saltar columnas con 'id'
            if 'id' in col_lower:
                continue

            n_null = df[col].isna().sum()
            if n_null > 0:
                if is_datetime64_any_dtype(df[col]):
                    median = pd.to_datetime(df[col]).median()
                    df_clean[col].fillna(median, inplace=True)
                    log.append(f"{col}: fecha imputada con mediana {median}")
                elif np.issubdtype(df[col].dtype, np.number):
                    mean_val = df[col].mean()
                    df_clean[col].fillna(mean_val, inplace=True)
                    log.append(f"{col}: numérico imputado con media {mean_val:.2f}")
                else:
                    mode_val = df[col].mode().iloc[0]
                    df_clean[col].fillna(mode_val, inplace=True)
                    log.append(f"{col}: categórico imputado con moda '{mode_val}'")

        # Remover outliers en columnas numéricas (excluyendo 'id')
        cols_to_clean = [c for c in df_clean.select_dtypes(include=[np.number]).columns if 'id' not in c.lower()]
        cleaned_df = self.analyzer.remove_outliers(
            df_clean,
            columns=cols_to_clean,
            method='iqr',
            k=1.5
        )
        removed_rows = len(df_clean) - len(cleaned_df)
        log.append(f"Outliers eliminados: {removed_rows} filas (método IQR)")

        return cleaned_df, log

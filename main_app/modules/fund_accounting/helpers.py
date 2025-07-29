from shiny import reactive
import pandas as pd
from ...s3_utils import load_tb_file

@reactive.calc
def trial_balance():
    df = load_tb_file()
    date_col_map = {}
    for col in df.columns:
        try:
            parsed = pd.to_datetime(col, errors="raise", dayfirst=False)
            date_col_map[col] = parsed
        except Exception:
            continue
    df = df.rename(columns=date_col_map)
    return df

@reactive.calc
def melted_tb():
    df = trial_balance()
    date_cols = [col for col in df.columns if isinstance(col, pd.Timestamp)]
    id_vars = [col for col in df.columns if col not in date_cols]
    df_long = pd.melt(
        df,
        id_vars=id_vars,
        value_vars=date_cols,
        var_name="Report Date",
        value_name="Balance"
    )
    df_long = df_long.dropna(subset=["Balance"])
    df_long = df_long[df_long["Balance"] != 0]
    return df_long

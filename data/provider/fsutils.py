import re
from datetime import date, timedelta
from fractions import Fraction

import pandas as pd

from data.provider.config import Config


def store_bhavcopy(data: bytes, for_date: date):
  bhavcopy_file = f"bhavdata_{for_date.strftime('%Y%m%d')}.csv"
  Config.BHAVCOPY_DIR.mkdir(parents=True, exist_ok=True)
  with open(f"{Config.BHAVCOPY_DIR / bhavcopy_file}", mode="wb") as f:
    f.write(data)


def store_corporate_action(data: pd.DataFrame, type: str):
  file = f"{type}_raw.csv"
  Config.CORP_ACTIONS_DIR.mkdir(parents=True, exist_ok=True)
  data.to_csv(Config.CORP_ACTIONS_DIR / file, index=False)


def bhavcopy_available(for_date: date) -> bool:
  bhavcopy_file = f"bhavdata_{for_date.strftime('%Y%m%d')}.csv"
  return (Config.BHAVCOPY_DIR / bhavcopy_file).exists()


def _bhavdata_to_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
  """
  Convert bhavdata DataFrame to OHLCV format.
  """
  bhav_column_map = {
    "DATE1": "Date",
    "SYMBOL": "Symbol",
    "SERIES": "Series",
    "OPEN_PRICE": "Open",
    "HIGH_PRICE": "High",
    "LOW_PRICE": "Low",
    "CLOSE_PRICE": "Close",
    "LAST_PRICE": "Last",
    "TTL_TRD_QNTY": "Volume",
  }
  df.columns = df.columns.str.strip()
  filt = df["SERIES"].isin([" EQ", " BE"])
  df = df[filt]
  df = df[bhav_column_map.keys()]
  df = df.rename(columns=bhav_column_map)
  df["Date"] = pd.to_datetime(df["Date"].str.lstrip(), format="%d-%b-%Y").dt.date
  df["Series"] = df["Series"].str.lstrip()
  return df


def _sanitize(df: pd.DataFrame):
  df.loc[:, "Date"] = pd.to_datetime(df["Date"]).dt.date.astype(str)
  df = df[df["Date"].notna()]
  df = df.drop_duplicates(subset="Date", ignore_index=True)
  return df


def build_ticker_data(*, from_date: date, to_date: date):
  current_date = from_date
  df = pd.DataFrame()

  while current_date <= to_date:
    file = Config.BHAVCOPY_DIR / f"bhavdata_{current_date.strftime('%Y%m%d')}.csv"
    if file.exists():
      try:
        file_df = pd.read_csv(file)
      except UnicodeDecodeError:
        file_df = pd.read_excel(file)
      df = pd.concat([df, file_df], ignore_index=True)
    current_date += timedelta(1)

  if not df.empty:
    df = _bhavdata_to_ohlcv(df)
  return df


def save_symbol_changes(df: pd.DataFrame):
  file_manual = Config.CORP_ACTIONS_DIR / "symbol_change_manual.csv"
  if file_manual.exists():
    df_manual = pd.read_csv(file_manual)
    df = pd.concat([df, df_manual], ignore_index=True)
  df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%Y").dt.date
  df.sort_values(by="Date", inplace=True, ignore_index=True)
  df.drop_duplicates(inplace=True, ignore_index=True)
  Config.CORP_ACTIONS_DIR.mkdir(exist_ok=True, parents=True)
  df.to_csv(Config.CORP_ACTIONS_DIR / "symbol_change.csv", index=False)


def symbol_change_info(*, from_date: date, to_date: date) -> dict[str, list[str]]:
  df = pd.read_csv(Config.CORP_ACTIONS_DIR / "symbol_change.csv")
  df["Date"] = pd.to_datetime(df["Date"]).dt.date
  df.set_index("Date", inplace=True)
  df = df[from_date:to_date]
  info = {}
  for _, row in df.iterrows():
    old = row["Symbol-Old"]
    new = row["Symbol"]
    info[new] = [old]
    if old in info and info[old]:
      info[new] = info[old] + info[new]
      del info[old]
  return info


def save_ticker(symbol: str, df: pd.DataFrame):
  df = _sanitize(df)
  Config.TICKER_DIR.mkdir(exist_ok=True, parents=True)
  df.to_csv(Config.TICKER_DIR / f"{symbol}.csv", index=False)


def load_ticker(symbol: str):
  file = Config.TICKER_DIR / f"{symbol}.csv"
  return pd.read_csv(file) if file.exists() else None


def delete_ticker(symbol: str):
  (Config.TICKER_DIR / f"{symbol}.csv").unlink(missing_ok=True)


def get_ca_info(
  type: str, *, from_date: date | None = None, to_date: date | None = None
) -> dict[str, dict[date, Fraction]]:
  file = f"{type}_raw.csv"
  ca = pd.read_csv(Config.CORP_ACTIONS_DIR / file)
  if from_date is not None:
    ca = ca[ca["EX-DATE"] >= from_date.isoformat()]
  if to_date is not None:
    ca = ca[ca["EX-DATE"] <= to_date.isoformat()]
  ca_info = {}
  for _, row in ca.iterrows():
    m = re.findall(r"\d+", row["PURPOSE"])
    ca_info.setdefault(row["SYMBOL"], {})[row["EX-DATE"]] = Fraction(f"{m[0]}/{m[1]}")
  return ca_info

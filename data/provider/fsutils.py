import re
from datetime import date, timedelta
from fractions import Fraction

import polars as pl

from data.provider.config import Config


def store_bhavcopy(data: bytes, for_date: date):
  bhavcopy_file = f"bhavdata_{for_date.strftime('%Y%m%d')}.csv"
  Config.BHAVCOPY_DIR.mkdir(parents=True, exist_ok=True)
  with open(f"{Config.BHAVCOPY_DIR / bhavcopy_file}", mode="wb") as f:
    f.write(data)


def store_corporate_action(data: pl.DataFrame, type: str):
  file = f"{type}_raw.csv"
  Config.CORP_ACTIONS_DIR.mkdir(parents=True, exist_ok=True)
  data.write_csv(Config.CORP_ACTIONS_DIR / file)


def bhavcopy_available(for_date: date) -> bool:
  bhavcopy_file = f"bhavdata_{for_date.strftime('%Y%m%d')}.csv"
  return (Config.BHAVCOPY_DIR / bhavcopy_file).exists()


def _bhavdata_to_ohlcv(df: pl.DataFrame) -> pl.DataFrame:
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

  return (
    df.rename({c: c.strip() for c in df.columns})
    .filter(pl.col("SERIES").str.strip_chars().is_in(["EQ", "BE"]))
    .select(bhav_column_map.keys())
    .rename(bhav_column_map)
    .with_columns(
      [
        pl.col("Date").str.strip_chars().str.to_date(format="%d-%b-%Y"),
        pl.col("Series").str.strip_chars(),
      ]
    )
  )


def _sanitize(df: pl.DataFrame):
  date_col = pl.col("Date")
  if df.schema["Date"] == pl.String:
    df = df.with_columns(date_col.str.to_date())
  return df.filter(date_col.is_not_null()).unique(subset="Date", maintain_order=True)


def build_ticker_data(*, from_date: date, to_date: date):
  frames: list[pl.DataFrame] = []

  current_date = from_date
  while current_date <= to_date:
    file = Config.BHAVCOPY_DIR / f"bhavdata_{current_date.strftime('%Y%m%d')}.csv"
    if file.exists():
      try:
        schema = {
          " OPEN_PRICE": pl.Float64,
          " HIGH_PRICE": pl.Float64,
          " LOW_PRICE": pl.Float64,
          " LAST_PRICE": pl.Float64,
          " CLOSE_PRICE": pl.Float64,
          " TTL_TRD_QNTY": pl.Int64,
        }
        file_df = pl.read_csv(file, schema_overrides=schema)
      except pl.exceptions.ComputeError:
        file_df = pl.read_excel(file)

      if not file_df.is_empty():
        frames.append(_bhavdata_to_ohlcv(file_df))
    current_date += timedelta(1)

  return pl.concat(frames) if frames else pl.DataFrame()


def save_symbol_changes(df: pl.DataFrame):
  file_manual = Config.CORP_ACTIONS_DIR / "symbol_change_manual.csv"
  if file_manual.exists():
    df_manual = pl.read_csv(file_manual)
    df = pl.concat([df, df_manual])
  df = df.with_columns(pl.col("Date").str.to_date(format="%d-%b-%Y")).sort(by="Date").unique()
  Config.CORP_ACTIONS_DIR.mkdir(exist_ok=True, parents=True)
  df.write_csv(Config.CORP_ACTIONS_DIR / "symbol_change.csv")


def symbol_change_info(*, from_date: date, to_date: date) -> dict[str, list[str]]:
  df = pl.read_csv(Config.CORP_ACTIONS_DIR / "symbol_change.csv")
  date_col = pl.col("Date")
  if df.schema["Date"] == pl.String:
    df = df.with_columns(date_col.str.to_date())
  df = df.filter((date_col >= from_date) & (date_col <= to_date))
  info = {}
  for row in df.iter_rows(named=True):
    old = row["Symbol-Old"]
    new = row["Symbol"]
    info[new] = [old]
    if old in info and info[old]:
      info[new] = info[old] + info[new]
      del info[old]
  return info


def save_ticker(symbol: str, df: pl.DataFrame):
  df = _sanitize(df)
  Config.TICKER_DIR.mkdir(exist_ok=True, parents=True)
  df.write_csv(Config.TICKER_DIR / f"{symbol}.csv")


def load_ticker(symbol: str):
  file = Config.TICKER_DIR / f"{symbol}.csv"
  if not file.exists():
    return None

  return pl.read_csv(file, schema_overrides={"Date": pl.Date})


def delete_ticker(symbol: str):
  (Config.TICKER_DIR / f"{symbol}.csv").unlink(missing_ok=True)


def get_ca_info(
  type: str, *, from_date: date | None = None, to_date: date | None = None
) -> dict[str, dict[date, Fraction]]:
  file = f"{type}_raw.csv"
  ca = pl.read_csv(Config.CORP_ACTIONS_DIR / file)
  if from_date is not None:
    ca = ca.filter(pl.col("EX-DATE") >= from_date.isoformat())
  if to_date is not None:
    ca = ca.filter(pl.col("EX-DATE") <= to_date.isoformat())
  ca_info = {}
  for row in ca.iter_rows(named=True):
    m = re.findall(r"\d+", row["PURPOSE"])
    dt = date.fromisoformat(row["EX-DATE"])
    ca_info.setdefault(row["SYMBOL"], {})[dt] = Fraction(f"{m[0]}/{m[1]}")
  return ca_info

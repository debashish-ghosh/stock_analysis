from fractions import Fraction
import json
from datetime import date, timedelta
from pathlib import Path
from time import perf_counter

import polars as pl
from requests import HTTPError

from data.client.nse import CorporateAction, nse_client
from data.provider import fsutils

BASE_DIR = Path(__file__).parent
TEMPLATE_FILE = BASE_DIR / "templates" / "app.json"
CONFIG_FILE = BASE_DIR / "config" / "app.json"


def load_config():
  app_config = {}
  with open(CONFIG_FILE if CONFIG_FILE.exists() else TEMPLATE_FILE) as f:
    app_config = json.load(f)
  return app_config


def save_config(appconfig):
  CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
  with open(CONFIG_FILE, "w") as f:
    json.dump(appconfig, f, indent=2)


def get_date(appconfig, *path):
  config = appconfig
  for step in path:
    config = config[step]
  if config is None:
    return date.fromisoformat(appconfig["epoch"]) - timedelta(days=1)
  elif isinstance(config, str):
    return date.fromisoformat(config)
  else:
    raise ValueError(f"Config path ({'.'.join(path)}) does not correspond to a date field")


def sync_bhavdata(appconfig, client: nse_client):
  perf_start = perf_counter()
  last_synced = get_date(appconfig, "bhavcopy", "last_synced")

  current_date = last_synced
  today = date.today()
  if last_synced >= today:
    print("No new bhavcopy to sync")
    return

  while current_date < today:
    current_date += timedelta(days=1)
    if fsutils.bhavcopy_available(current_date):
      print(f"Bhavcopy for {current_date} is available. Skipping download")
      last_synced = current_date
      continue
    try:
      data = client.fetch_bhavcopy(current_date)
      fsutils.store_bhavcopy(data, current_date)
      print(f"Bhavcopy downloaded for {current_date}")
      last_synced = current_date
    except HTTPError as e:
      if e.response is not None and e.response.status_code == 404:
        print(f"Bhavcopy not available for {current_date}")
        pass
    except Exception as e:
      print(f"Error fetching bhavcopy for {current_date}: {e}")

  appconfig["bhavcopy"]["last_synced"] = last_synced.isoformat()
  perf_end = perf_counter()
  print(f"Bhavcopy synced to {last_synced} ({perf_end - perf_start:.2f}s)")


def sync_corp_actions(appconfig, client: nse_client):
  ca_last_synced = get_date(appconfig, "corp_actions", "last_synced")
  today = date.today()
  if ca_last_synced >= today:
    return

  from_date = date.fromisoformat(appconfig["epoch"])
  try:
    bonus_data = client.fetch_corporate_action(CorporateAction.BONUS, from_date, today)
    fsutils.store_corporate_action(bonus_data, "bonus")

    split_data = client.fetch_corporate_action(CorporateAction.SPLIT, from_date, today)
    fsutils.store_corporate_action(split_data, "split")

    symbol_changes = client.fetch_symbol_changes()
    fsutils.save_symbol_changes(symbol_changes)
  except HTTPError as e:
    if e.response is not None and e.response.status_code == 404:
      print(f"Corporate action data not available from {from_date} to {today}")
      pass
  appconfig["corp_actions"]["last_synced"] = today.isoformat()


def update_tickers(appconfig):
  perf_start = perf_counter()
  last_modified = get_date(appconfig, "ticker", "last_modified")
  last_synced = get_date(appconfig, "bhavcopy", "last_synced")

  if last_synced <= last_modified:
    print("No symbols to update")
    return

  df = fsutils.build_ticker_data(from_date=last_modified + timedelta(days=1), to_date=last_synced)
  symbols = df["Symbol"].unique()
  print(f"{len(symbols)} symbols found")
  perf_end = perf_counter()
  print(f"Ticker data built ({perf_end - perf_start:.2f}s)")
  perf_start = perf_end
  for symbol in symbols:
    df_symbol = df.filter(pl.col("Symbol") == symbol)
    df_file = fsutils.load_ticker(symbol)
    if not (df_file is None or df_file.is_empty()):
      df_symbol = pl.concat([df_file, df_symbol])
    fsutils.save_ticker(symbol, df_symbol)

  appconfig["ticker"]["last_modified"] = last_synced.isoformat()
  perf_end = perf_counter()
  print(f"Ticker data updated ({perf_end - perf_start:.2f}s)")


def combined_corporate_action():
  ca_info = fsutils.get_ca_info("split")
  for s, i in fsutils.get_ca_info("bonus").items():
    if s not in ca_info:
      ca_info[s] = {d: 1 + f for d, f in i.items()}
    else:
      for d, f in i.items():
        if d not in ca_info[s]:
          ca_info[s][d] = 1 + f
        else:
          ca_info[s][d] *= 1 + f
  return ca_info


def adjust_ticker_price(df: pl.DataFrame, symbol: str, info: dict[date, Fraction]) -> tuple[pl.DataFrame, bool]:
  adjusted = False
  ADJ_SUFFIX = " (adj)"
  for ca_date, ratio in info.items():
    before_ca = pl.col("Date") < ca_date
    post_df = df.filter(~before_ca)

    if post_df.is_empty():
      print(f"No data for {symbol} since {ca_date}")
      break

    # check if not asjusted already
    if post_df["Symbol"][0].endswith(ADJ_SUFFIX):
      continue

    ratio_f = float(ratio)
    price_cols = ["Open", "High", "Low", "Close", "Last"]
    df = df.with_columns(
      [
        *[pl.when(before_ca).then((pl.col(c) / ratio_f).round(2)).otherwise(pl.col(c)).alias(c) for c in price_cols],
        pl.when(before_ca)
        .then((pl.col("Volume") * ratio_f).cast(pl.Int64))
        .otherwise(pl.col("Volume"))
        .alias("Volume"),
        pl.when(pl.col("Date") == post_df["Date"][0])
        .then(pl.col("Symbol") + ADJ_SUFFIX)
        .otherwise(pl.col("Symbol"))
        .alias("Symbol"),
      ]
    )

    # mark as adjusted
    adjusted = True
  return df, adjusted


def adjust_ticker_symbol(latest_symbol: str, old_symbols: list[str]):
  olds = [fsutils.load_ticker(s) for s in old_symbols]
  new = fsutils.load_ticker(latest_symbol)
  result = [x for x in olds + [new] if x is not None]
  if result:
    # combine all previous symbols with the latest symbol and save
    fsutils.save_ticker(latest_symbol, pl.concat(result))
    # delete previous symbol files
    for symbol in old_symbols:
      fsutils.delete_ticker(symbol)


def adjust_tickers(appconfig):
  perf_start = perf_counter()
  last_adjusted = get_date(appconfig, "ticker", "last_adjusted")
  last_modified = get_date(appconfig, "ticker", "last_modified")
  if last_adjusted >= last_modified:
    print("No symbols to adjust")
    return

  print("Adjusting for symbol change ...")
  sc_info = fsutils.symbol_change_info(from_date=last_adjusted + timedelta(days=1), to_date=last_modified)
  for latest_symbol, old_symbols in sc_info.items():
    adjust_ticker_symbol(latest_symbol, old_symbols)
  perf_end = perf_counter()
  print(f"Symbol changes done ({perf_end - perf_start:.2f}s)")
  perf_start = perf_end

  print("Adjusting for price due to bonus/split ...")
  ca_info = combined_corporate_action()
  for symbol, info in ca_info.items():
    df = fsutils.load_ticker(symbol)

    if df is None:
      continue

    df, adjusted = adjust_ticker_price(df, symbol, info)
    if adjusted:
      fsutils.save_ticker(symbol, df)

  perf_end = perf_counter()
  print(f"Bonus and split adjustments done ({perf_end - perf_start:.2}s)")

  appconfig["ticker"]["last_adjusted"] = last_modified.isoformat()


def main():
  print("Starting algo trading ...")
  appconfig = load_config()
  client = nse_client()

  sync_bhavdata(appconfig, client)
  sync_corp_actions(appconfig, client)
  save_config(appconfig)

  update_tickers(appconfig)
  save_config(appconfig)

  adjust_tickers(appconfig)
  save_config(appconfig)


if __name__ == "__main__":
  perf_start = perf_counter()
  try:
    main()
  except KeyboardInterrupt:
    print("Application interrupted by user")
  perf_end = perf_counter()
  print(f"Finished in {perf_end - perf_start:.2f} seconds")

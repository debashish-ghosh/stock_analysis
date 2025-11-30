from fractions import Fraction
import json
from datetime import date, timedelta
from functools import partial
from operator import is_not
from pathlib import Path
from time import perf_counter

import pandas as pd
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
      if e.response.status_code == 404:
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
    if e.response.status_code == 404:
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
    df_symbol = df[df["Symbol"] == symbol]
    df_file = fsutils.load_ticker(symbol)
    if not (df_file is None or df_file.empty):
      df_symbol = pd.concat([df_file, df_symbol], ignore_index=True)
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


def adjust_ticker_price(df: pd.DataFrame, symbol: str, info: dict[date, Fraction]):
  adjusted = False
  ADJ_SUFFIX = " (adj)"
  for ca_date, ratio in info.items():
    row_indexer = df["Date"] < ca_date
    if df[~row_indexer].empty:
      print(f"No data for {symbol} since {ca_date}")
      break
    marker_idx = df[~row_indexer].iloc[0].name
    # check if not asjusted already
    if not df.loc[marker_idx, "Symbol"].endswith(ADJ_SUFFIX):
      col_indexer = ["Open", "High", "Low", "Close", "Last"]
      df.loc[row_indexer, col_indexer] = round(df.loc[row_indexer, col_indexer] / float(ratio), 2)
      df.loc[row_indexer, "Volume"] = (df.loc[row_indexer, "Volume"] * float(ratio)).astype(int)
      # mark as adjusted
      df.loc[marker_idx, "Symbol"] = df.loc[marker_idx, "Symbol"] + ADJ_SUFFIX
      adjusted = True
  return adjusted


def adjust_ticker_symbol(latest_symbol: str, old_symbols: list[str]):
  olds = [fsutils.load_ticker(s) for s in old_symbols]
  new = fsutils.load_ticker(latest_symbol)
  result = list(filter(partial(is_not, None), olds + [new]))
  if result:
    # combine all previous symbols with the latest symbol and save
    fsutils.save_ticker(latest_symbol, pd.concat(result, ignore_index=True))
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
    if adjust_ticker_price(df, symbol, info):
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

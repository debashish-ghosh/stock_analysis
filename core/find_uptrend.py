import math
from pathlib import Path

import pandas as pd


def pertTimeAbovEma(df, emaLevel, days):
  emaList = df.Close.ewm(span=emaLevel, adjust=False).mean().tail(days)
  priceList = df.Close.tail(days)
  diff = priceList - emaList
  return sum(diff >= 0) * 100 / days


def pertTimeBelowEma(df, emaLevel, days):
  emaList = df.Close.ewm(span=emaLevel, adjust=False).mean().tail(days)
  priceList = df.Close.tail(days)
  diff = priceList - emaList
  return sum(diff <= 0) * 100 / days


# ------Function-----------
# Main function to find stocks in uptrend
def uptrendStockFind(
  stockfile: Path, uptrend: list, downtrend: list, notrend: list, insufficient: list, backTestQuery: bool = False
):
  df = pd.read_csv(stockfile)
  length = df.shape[0]
  if not backTestQuery and length < 200:
    insufficient.append(stockfile.stem)
    return

  try:
    present = df.Close.iloc[-1].item()
  except IndexError:
    print(stockfile, ":", "Check Stock File")
    return

  ema10 = df.Close.ewm(span=10, adjust=False).mean().tail(1).iloc[0]
  ema21 = df.Close.ewm(span=21, adjust=False).mean().tail(1).iloc[0]
  ema50 = df.Close.ewm(span=50, adjust=False).mean().tail(1).iloc[0]
  ema200 = df.Close.ewm(span=200, adjust=False).mean().tail(1).iloc[0]

  ema200n = df.Close.ewm(span=200, adjust=False).mean()[-200:]
  # xrange = range(200)

  dma200std = df.Close.rolling(window=200).std().tail(1).iloc[0]

  # 200 ema gain in last 200 days
  pertgain = 100 * (ema200n.values[-1] - ema200n.values[0]) / ema200n.values[0]

  # Slope for 200 day ema
  # slopeD = np.rad2deg(np.arctan2(pertgain, xrange[-1] - xrange[0]))

  # Filter for stocks which are above certain % over  200dma when
  # signal is generated
  # pertGainAbove200Ema = (present - ema200) * 100 / ema200

  # averageVol = statistics.mean(df.Volume)
  # Filter for stocks which are at least 60% time above 200 dma in
  # no of days suppilied.
  pertTimeAbove200ema = pertTimeAbovEma(df, 200, 60)
  pertTimeBelow200ema = pertTimeBelowEma(df, 200, 60)

  # if( present >= ema21 and present >= ema50 and present >=ema200 and
  # ema21 >= ema50  and ema50 >= ema200 and pertAbove200ema >=60):
  # if(math.isnan(dma200std) == False):
  if (
    present >= ema21
    and present >= ema50
    and present >= ema200
    and present >= ema10
    and ema10 >= ema200
    and pertTimeAbove200ema >= 60
    and pertgain >= 5
  ):
    if not math.isnan(dma200std):
      uptrend.append(stockfile.stem)
  elif (
    present <= ema21
    and present <= ema50
    and present <= ema200
    and present <= ema10
    and ema10 <= ema200
    and pertTimeBelow200ema >= 60
    and pertgain <= -5
  ):
    if not math.isnan(dma200std):
      downtrend.append(stockfile.stem)
  else:
    notrend.append(stockfile.stem)


# ------Function-----------


def uptrendBackTestLogic(df: pd.DataFrame):
  dma200 = df.Close.rolling(window=200).mean()
  dma50 = df.Close.rolling(window=50).mean()
  ema21 = df.Close.ewm(span=21).mean()
  ema10 = df.Close.ewm(span=10).mean()

  pertpresent = (df.Close - dma200) * 100 / dma200
  pert21 = (ema21 - dma200) * 100 / dma200
  pert50 = (dma50 - dma200) * 100 / dma200

  c0 = (df.Close <= ema10) & (df.Close >= dma50) & (df.Close >= dma200) & (ema21 >= dma50) & (dma50 >= dma200)
  c1 = (pertpresent <= 10) & (abs(pert21) <= 5) & (abs(pert50 <= 5))

  combined = c0 & c1
  combined = combined.dropna()
  return combined


# ------Function-----------


def backTestDetect(stockfile: Path):
  print("filename: ", stockfile)
  df = pd.read_csv(stockfile)
  combined = uptrendBackTestLogic(df)
  dateindex = combined[combined].index
  for i in dateindex:
    print(df.Date[i])
    print(i)


def main():
  BASE_DIR = Path(__file__).parent.parent
  DATASTORE_DIR = BASE_DIR / "data" / "store"
  STOCK_FILTER_FILE = DATASTORE_DIR / "refdata" / "NIFTY_500.csv"
  df = pd.read_csv(STOCK_FILTER_FILE)

  # Dataset on which uptrend alogo is run
  TICKER_DIR = DATASTORE_DIR / "ticker"
  tickers = filter(lambda f: f.exists(), map(lambda file: TICKER_DIR / f"{file}.csv", df.Symbol))

  uptrend = []
  downtrend = []
  notrend = []
  insufficient = []
  for ticker in tickers:
    uptrendStockFind(ticker, uptrend, downtrend, notrend, insufficient)

  ANALYSIS_DIR = DATASTORE_DIR / "analysis"
  ANALYSIS_DIR.mkdir(exist_ok=True, parents=True)

  df.loc[df["Symbol"].isin(uptrend), "Trend"] = "uptrend"
  df.loc[df["Symbol"].isin(downtrend), "Trend"] = "downtrend"
  df.loc[df["Symbol"].isin(notrend), "Trend"] = "notrend"
  df.loc[df["Symbol"].isin(insufficient), "Trend"] = "insufficient data"

  df.dropna(inplace=True)
  df.to_csv(ANALYSIS_DIR / STOCK_FILTER_FILE.name, index=False)

  print(f"Stocks in uptrend: {len(uptrend)}")
  print(f"Stocks in downtrend: {len(downtrend)}")
  print(f"Stocks in notrend: {len(notrend)}")
  print(f"Stocks having insufficient data: {len(insufficient)}")
  print(f"Total stocks processed: {len(uptrend) + len(downtrend) + len(notrend) + len(insufficient)}")

import os
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

dfout = pd.DataFrame(
  columns=[
    "SYMBOL",
    "DATE",
    "DECISION",
    "PRICE",
    "GAIN",
    "STOPLOSS",
    "VOLATLITY",
    "PROBABLITY",
    "OUTLIERS",
    "SIGNALTYPE",
    "RANK",
  ]
)

stocksParametersdata = pd.DataFrame(
  columns=[
    "SYMBOL",
    "PRESENT",
    "PRESENTHIGH",
    "ONEDAYOLDPRICE",
    "TWODAYOLDPRICE",
    "UPPERBAND",
    "LOWERBAND",
    "SELLPROBABLITY",
    "OUTLIERS",
    "SIGNALTYPE",
    "RANK",
  ]
)


def calculateRank(df, stockfile, spread):
  # 90 day price * volume

  timeframe = 120
  priceVol = (df.Close - df.Open).tail(timeframe) * df.Volume.tail(timeframe)
  # tweak to handel zero values
  priceVol[priceVol == 0] = 1
  absPriceVol = abs(priceVol)
  normalizedPriceVol = np.log(absPriceVol)
  normalizedMean = np.mean(normalizedPriceVol)
  normalizedSD = np.std(normalizedPriceVol)

  FILTER = normalizedPriceVol >= (normalizedMean + 2 * normalizedSD)
  values = priceVol[FILTER]
  totalCounts = sum(FILTER)
  positiveCounts = sum(values > 0)
  ratio = 0
  if totalCounts != 0:
    ratio = int(100 * (positiveCounts / totalCounts))

  # averagePertDeliverablesRank = sum(df['%Deliverble'].tail(10))
  outlierRank = 2 * totalCounts * ratio / 100
  spreadRank = spread / 10

  totalRank = outlierRank + spreadRank

  # print(stockfile[36:-4],":" ,"Total Rank :" , totalRank)
  # plt.hist(normalizedPriceVol,bins=timeframe)
  # plt.show()
  return [totalCounts, ratio, totalRank]


# ------Function-----------
# Main function to find stocks in uptrend
def uptrendStockDecision(stockfile: Path):
  # print(stockfile)
  df = pd.read_csv(stockfile)
  today = date.today().strftime("%d/%m/%Y")

  length = df.shape[0]

  # ema21 = df.Close.ewm(span = 21,adjust=False).mean().tail(1).iloc[0]
  ema21 = df.Close.rolling(window=21).mean().tail(1).iloc[0]

  # ema21std = df.Close.ewm(span = 21,adjust=False).std().tail(1).iloc[0]
  ema21std = df.Close.rolling(window=21).std().tail(1).iloc[0]

  # Volatlity ..Currenty not used in program

  present = df.Close.iloc[length - 1]
  oneDayOldPrice = [df.Close.iloc[length - 2]]
  twoDayOldPrice = [df.Close.iloc[length - 3]]

  # Logic below is buy sell decision using bollinger bands
  # Indicate buy or sell for today : 0 = sell, 1 = buy

  upperBand = ema21 + 2 * ema21std
  lowerBand = ema21 - 2 * ema21std

  # Lower Band buy logic
  presentHigh = df.High.iloc[length - 1]

  highLowRatio = round(100 * upperBand / lowerBand, 2)

  lowBandArray = df.Close.rolling(window=21).mean().tail(5) - 2 * df.Close.rolling(window=21).std().tail(5)
  highBandArray = df.Close.rolling(window=21).mean().tail(5) + 2 * df.Close.rolling(window=21).std().tail(5)
  midBandArray = df.Close.rolling(window=21).mean().tail(5)

  lowPriceArray = df.Low.tail(5)
  highPriceArray = df.High.tail(5)

  numLowBelowBand = sum(lowPriceArray <= lowBandArray)
  numHighAoveBand = sum(highPriceArray >= highBandArray)
  numlowBelow21Day = sum(lowPriceArray <= midBandArray)

  buyProbablity = numLowBelowBand * 100 / 5
  sellProbablity = numHighAoveBand * 100 / 5
  midBandBuyProbablity = numlowBelow21Day * 100 / 5

  # Risk Reward based stoploss calulations :
  riskRewardRatio = 1 / 3
  stopLossPrice = round(present - (upperBand - present) * riskRewardRatio, 2)

  # Min expected gain value
  spreadAvailable = 100 * (upperBand - present) / present

  # outlier contains rank
  rankdata = calculateRank(df, stockfile, spreadAvailable)
  rank = round(rankdata[2], 1)
  outliers = str(rankdata[0]) + "::" + str(rankdata[1])

  # print(stockfile[36:-4],outliers, rank)
  # Store Data for later use in program

  # Constants
  RANK_DATA_LIMIT = 75

  # print(stockfile[36:-4])
  if (
    buyProbablity > 0
    and present >= oneDayOldPrice
    and present > lowerBand
    and spreadAvailable >= 7
    and (rankdata[1] >= RANK_DATA_LIMIT)
    and (highLowRatio >= 114)
  ):
    decision = "NEWBUY"
    stocksParametersdata.loc[len(stocksParametersdata)] = [
      stockfile.stem,
      present,
      presentHigh,
      oneDayOldPrice,
      twoDayOldPrice,
      upperBand,
      lowerBand,
      sellProbablity,
      outliers,
      "LOW",
      rank,
    ]

    dfout.loc[len(dfout)] = [
      stockfile.stem,
      today,
      decision,
      present,
      0,
      stopLossPrice,
      highLowRatio,
      buyProbablity,
      outliers,
      "LOW",
      rank,
    ]
    print("buy", stockfile.stem, rankdata[1])
  elif (
    midBandBuyProbablity > 0
    and present >= oneDayOldPrice
    and present > ema21
    and spreadAvailable >= 5
    and (rankdata[1] >= RANK_DATA_LIMIT)
    and (highLowRatio >= 110)
  ):
    decision = "NEWBUY"
    stocksParametersdata.loc[len(stocksParametersdata)] = [
      stockfile.stem,
      present,
      presentHigh,
      oneDayOldPrice,
      twoDayOldPrice,
      upperBand,
      lowerBand,
      sellProbablity,
      outliers,
      "MID",
      rank,
    ]

    dfout.loc[len(dfout)] = [
      stockfile.stem,
      today,
      decision,
      present,
      0,
      stopLossPrice,
      highLowRatio,
      midBandBuyProbablity,
      outliers,
      "MID",
      rank,
    ]
    print("midbuy", stockfile.stem, rankdata[1])
  elif buyProbablity > 0 and present >= oneDayOldPrice:
    stocksParametersdata.loc[len(stocksParametersdata)] = [
      stockfile.stem,
      present,
      presentHigh,
      oneDayOldPrice,
      twoDayOldPrice,
      upperBand,
      lowerBand,
      sellProbablity,
      outliers,
      "NA",
      rank,
    ]
    # print("Potential Buy", stockfile.stem,buyProbablity ,present,oneDayOldPrice, upperBand )
  else:
    stocksParametersdata.loc[len(stocksParametersdata)] = [
      stockfile.stem,
      present,
      presentHigh,
      oneDayOldPrice,
      twoDayOldPrice,
      upperBand,
      lowerBand,
      sellProbablity,
      outliers,
      "NA",
      rank,
    ]

  return


# ------Function-----------
def updateDecisionFile(DECISION_FILE: Path, TICKER_DIR: Path):
  if DECISION_FILE.exists():
    df_decision = pd.read_csv(DECISION_FILE)
    newStocks = dfout[~dfout["SYMBOL"].isin(df_decision["SYMBOL"])]
    df_decision = pd.concat([df_decision, newStocks], ignore_index=True)
  else:
    DECISION_FILE.parent.mkdir(exist_ok=True, parents=True)
    df_decision = dfout
  df_decision.reset_index(drop=True, inplace=True)

  # Recalculate all parameters for stocks in decision file. Use hardware file to calulcate it

  # Loop to fill FINAL column
  for index, row in df_decision.iterrows():
    file = TICKER_DIR / f"{row['SYMBOL']}.csv"
    if not file.exists():
      # print("File:", file , "Do Not Exist: Dropping file")
      df_decision.drop(index, inplace=True)
      continue

    parameters = stocksParametersdata[stocksParametersdata["SYMBOL"] == row["SYMBOL"]]

    df_file = pd.read_csv(file)
    # ema21 = df_file.Close.ewm(span = 21,adjust=False).mean().tail(1).iloc[0]
    # ema21std = df_file.Close.ewm(span = 21,adjust=False).std().tail(1).iloc[0]
    dma200 = df_file.Close.rolling(window=200).mean().tail(1).iloc[0]
    ema200 = df_file.Close.ewm(span=200, adjust=False).mean().tail(1).iloc[0]

    gain = 100 * (parameters.PRESENT.iloc[0] - row["PRICE"]) / parameters.PRESENT.iloc[0]
    df_decision.loc[index, "GAIN"] = round(gain, 1)
    df_decision.loc[index, "OUTLIERS"] = parameters.OUTLIERS.iloc[0]

    if parameters.SIGNALTYPE.iloc[0] == "MID":
      df_decision.loc[index, "RANK"] = round(parameters.RANK.iloc[0] * 0.75, 1)
    else:
      df_decision.loc[index, "RANK"] = parameters.RANK.iloc[0]

    # State Machine
    if row["DECISION"] == "NEWBUY":
      df_decision.loc[index, "DECISION"] = "BUY"
    elif row["DECISION"] == "BUY" or row["DECISION"] == "HOLD":
      if (
        parameters.SELLPROBABLITY.iloc[0] > 0
        and parameters.PRESENT.iloc[0] < parameters.ONEDAYOLDPRICE.iloc[0]
        and parameters.PRESENT.iloc[0] > row["PRICE"]
      ):
        df_decision.loc[index, "DECISION"] = "BOOK PROFIT"
      elif parameters.PRESENT.iloc[0] <= row["STOPLOSS"]:
        df_decision.loc[index, "DECISION"] = "EXIT"
      else:
        df_decision.loc[index, "DECISION"] = "HOLD"
    elif row["DECISION"] == "BOOK PROFIT":
      df_decision.drop(index, inplace=True)
    elif row["DECISION"] == "EXIT":
      # Remove file from trend directory of it is below 200 ema and dma
      if parameters.PRESENT.iloc[0] < ema200 and parameters.PRESENT.iloc[0] < dma200:
        print("removing", file)
        os.remove(file)
      df_decision.drop(index, inplace=True)

  # Write to file
  df_decision.to_csv(DECISION_FILE, sep=",", mode="w", index=False)
  # df_decision.to_csv(forDistribution, sep=',', mode='w',index = False)

  # df_sorted = df_decision.sort_values(by = ["RANK","SIGNALTYPE"], ascending= False)
  # df_sorted.to_csv(decisionFileRankSorted, sep=',', mode='w',index = False)
  # df_sorted.to_csv(forDistributionRankSorted, sep=',', mode='w',index = False)


# -------Execuation start from here -------------
def main():
  BASE_DIR = Path(__file__).parent.parent
  DATASTORE_DIR = BASE_DIR / "data" / "store"
  ANALYSIS_DIR = DATASTORE_DIR / "analysis"
  TICKER_DIR = DATASTORE_DIR / "ticker"
  TREND_FILE = ANALYSIS_DIR / "NIFTY_500.csv"
  DECISION_FILE = ANALYSIS_DIR / "upTrendNiftyIncome.csv"

  df = pd.read_csv(TREND_FILE)
  uptrend_symbols = df.loc[df["Trend"] == "uptrend", "Symbol"]
  uptrend_stock_files = filter(lambda f: f.exists(), map(lambda file: TICKER_DIR / f"{file}.csv", uptrend_symbols))

  for stockfile in uptrend_stock_files:
    uptrendStockDecision(stockfile)

  updateDecisionFile(DECISION_FILE, TICKER_DIR)

from datetime import date
from enum import Enum
from io import BytesIO
from time import perf_counter

import pandas as pd
import requests

from data.client.config import Config


class CorporateAction(Enum):
  BONUS = "BONUS"
  SPLIT = "Split"


class nse_client:
  def __init__(self):
    self.headers = {
      "User-Agent": Config.USER_AGENT,
      "Accept-Encoding": "gzip, deflate, br, zstd",
      "Sec-Fetch-Mode": "navigate",
      "Sec-Fetch-Dest": "document",
    }
    self.session = None

  def _init_session(self):
    # First visit the main page to get cookies
    headers = {
      **self.headers,
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Sec-Fetch-Site": "none",
    }
    sess = requests.Session()
    response = sess.get(Config.NSE_BASE_URL, headers=headers, timeout=30)
    response.raise_for_status()

    # Then visit the corporate actions page
    headers = {
      **self.headers,
      "Accept": "text/html,application/json,text/csv,*/*",
      "Sec-Fetch-Site": "same-origin",
      "Referer": Config.NSE_BASE_URL,
    }
    response = sess.get(Config.NSE_CORP_ACTIONS_LANDING_URL, headers=headers, timeout=30)
    response.raise_for_status()
    return sess

  def fetch_corporate_action(self, subject: CorporateAction, from_date: date, to_date: date) -> pd.DataFrame:
    if self.session is None:
      perf_start = perf_counter()
      self.session = self._init_session()
      perf_end = perf_counter()
      print(f"Session initialized ({perf_end - perf_start:.2f}s)")

    perf_start = perf_counter()
    params = {
      "index": "equities",
      "from_date": from_date.strftime("%d-%m-%Y"),
      "to_date": to_date.strftime("%d-%m-%Y"),
      "subject": subject.value,
      "csv": "true",
    }

    headers = {
      **self.headers,
      "Accept": "text/csv,application/x-www-form-urlencoded",
      "Sec-Fetch-Site": "same-origin",
      "Referer": Config.NSE_CORP_ACTIONS_LANDING_URL,
    }

    response = self.session.get(Config.NSE_CORP_ACTIONS_URL, params=params, headers=headers)
    response.raise_for_status()
    df = pd.read_csv(BytesIO(response.content))
    df["EX-DATE"] = pd.to_datetime(df["EX-DATE"], format="%d-%b-%Y").dt.date
    perf_end = perf_counter()
    print(f"{subject.value} data downloaded from {from_date} to {to_date} ({perf_end - perf_start:.2}s)")
    return df

  def fetch_symbol_changes(self):
    perf_start = perf_counter()
    response = requests.get(Config.SYMBOL_CHANGE_URL, headers=self.headers)
    response.raise_for_status()
    cols = ["Name", "Symbol-Old", "Symbol", "Date"]

    df = pd.read_csv(BytesIO(response.content), header=None, names=cols)
    df.drop(columns="Name", inplace=True)
    perf_end = perf_counter()
    print(f"Symbol changes downloaded ({perf_end - perf_start:.2f}s)")
    return df

  def fetch_bhavcopy(self, for_date: date = date.today()) -> bytes:
    bhavcopy_file = f"sec_bhavdata_full_{for_date.strftime('%d%m%Y')}.csv"
    URL = f"{Config.BHAVCOPY_URL_PREFIX}/{bhavcopy_file}"

    response = requests.get(URL, headers={"User-Agent": Config.USER_AGENT})
    response.raise_for_status()  # Raise an error for bad responses
    return response.content

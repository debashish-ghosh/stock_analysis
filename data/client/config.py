from dataclasses import dataclass


@dataclass
class Config:
  NSE_BASE_URL: str = "https://www.nseindia.com"
  NSE_CORP_ACTIONS_LANDING_URL = f"{NSE_BASE_URL}/companies-listing/corporate-filings-actions"
  NSE_CORP_ACTIONS_URL = f"{NSE_BASE_URL}/api/corporates-corporateActions"
  BHAVCOPY_URL_PREFIX: str = "https://nsearchives.nseindia.com/products/content"
  SYMBOL_CHANGE_URL: str = "https://nsearchives.nseindia.com/content/equities/symbolchange.csv"
  USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0"

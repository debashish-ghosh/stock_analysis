from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
  __PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
  BHAVCOPY_DIR = __PROJECT_DIR / "data" / "store" / "bhavcopy"
  CORP_ACTIONS_DIR = __PROJECT_DIR / "data" / "store" / "corporate_actions"
  TICKER_DIR = __PROJECT_DIR / "data" / "store" / "ticker"
  TICKER_ADJ_DIR = TICKER_DIR / "adjusted"

from pathlib import Path
from time import perf_counter

import requests


def download_reference_file(filepath: Path, url: str):
  headers = {
    "User-Agent": (
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
  }
  try:
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
      # Save the content of the response to a local CSV file
      with open(filepath, "wb") as f:
        f.write(response.content)
        print(f"{filepath.name} file downloaded successfully")
    else:
      print(f"Failed to download {filepath.name} file. Status code: {response.status_code}")
  except requests.RequestException as e:
    print(f"Error: {e}")


def fetch():
  DATA_DIR = Path(__file__).parent.parent
  REFDATA_DIR = DATA_DIR / "store" / "refdata"
  REFDATA_DIR.mkdir(parents=True, exist_ok=True)

  tasks = {
    "NIFTY_50.csv": "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv",
    "NIFTY_MICROCAP_250.csv": "https://nsearchives.nseindia.com/content/indices/ind_niftymicrocap250_list.csv",
    "NIFTY_500.csv": "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
    "NIFTY_TOTAL_MARKET.csv": "https://nsearchives.nseindia.com/content/indices/ind_niftytotalmarket_list.csv",
    # "symbolchange.csv": "https://nsearchives.nseindia.com/content/equities/symbolchange.csv",
    "EQUITY_L.csv": "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    "SME_EQUITY_L.csv": "https://nsearchives.nseindia.com/emerge/corporates/content/SME_EQUITY_L.csv",
  }

  for file, url in tasks.items():
    download_reference_file(REFDATA_DIR / file, url)


if __name__ == "__main__":
  perf_start = perf_counter()
  fetch()
  perf_end = perf_counter()
  print(f"Finished in {perf_end - perf_start:.2f}s")

import pandas as pd
from pytrends.request import TrendReq

KEYWORDS = ["affitto", "mutuo"]  # rental & mortgage in Italian
GEO = "IT"  # Italy

def fetch_trends(days: int = 90) -> pd.DataFrame:
    # Fetch daily Google Trends for the last `days` days.
    pytrends = TrendReq(hl="it-IT", tz=360)
    timeframe = f"today {days}-d"
    pytrends.build_payload(KEYWORDS, timeframe=timeframe, geo=GEO)
    df = pytrends.interest_over_time()
    if df is None or df.empty:
        raise RuntimeError("Google Trends returned empty dataframe. Try again later.")
    df = df.reset_index().rename(columns={"date": "date"})
    if "isPartial" in df.columns:
        df = df.drop(columns=["isPartial"])
    df = df.melt(id_vars=["date"], var_name="keyword", value_name="value")
    df["geo"] = GEO
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["value"] = df["value"].fillna(0).astype(int)
    return df

if __name__ == "__main__":
    print(fetch_trends().head())

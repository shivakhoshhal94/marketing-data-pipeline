# src/extract_trends.py
import pandas as pd
import numpy as np

try:
    from pytrends.request import TrendReq
except Exception:
    TrendReq = None

KEYWORDS = ["affitto", "mutuo"]  # rental & mortgage in Italian
GEO = "IT"

def _synthetic_trends(days: int = 90, seed: int = 7) -> pd.DataFrame:
    """Generate offline trends with weekly seasonality + noise (values 0..100)."""
    rng = np.random.default_rng(seed)
    end = pd.Timestamp.today().normalize()
    dates = pd.date_range(end=end, periods=days, freq="D")
    rows = []
    for kw in KEYWORDS:
        season = 10 * np.sin(np.arange(days) * 2 * np.pi / 7)
        noise = rng.normal(0, 1.5, size=days).cumsum() / 6
        level = rng.uniform(40, 70) + season + noise
        level = np.clip(np.round(level), 0, 100)
        for d, v in zip(dates, level):
            rows.append({"date": d.date(), "keyword": kw, "geo": GEO, "value": int(v)})
    return pd.DataFrame(rows)

def fetch_trends(days: int = 90, retries: int = 3, offline_ok: bool = True) -> pd.DataFrame:
    """
    Try to fetch Google Trends. If it fails (400/429/etc) and offline_ok=True,
    fall back to synthetic data with the same shape.
    """
    # If pytrends isn't available at all, go synthetic.
    if TrendReq is None:
        if offline_ok:
            return _synthetic_trends(days)
        raise RuntimeError("pytrends not available and offline_ok=False")

    try:
        # Use retries + fetch keywords one-by-one (more reliable)
        pytrends = TrendReq(hl="it-IT", tz=0, retries=retries, backoff_factor=0.4)
        timeframe = f"today {days}-d" if days <= 269 else "today 12-m"

        frames = []
        for kw in KEYWORDS:
            pytrends.build_payload([kw], timeframe=timeframe, geo=GEO)
            df = pytrends.interest_over_time()
            if df is None or df.empty:
                raise ValueError("Empty trends response")
            df = df.reset_index().rename(columns={"date": "date", kw: "value"})
            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])
            df["keyword"] = kw
            df["geo"] = GEO
            df["date"] = pd.to_datetime(df["date"]).dt.date
            frames.append(df[["date", "keyword", "geo", "value"]])

        return pd.concat(frames, ignore_index=True)

    except Exception as e:
        if not offline_ok:
            raise
        print(f"[WARN] Trends fetch failed ({e}); generating synthetic trends instead.")
        return _synthetic_trends(days)

if __name__ == "__main__":
    print(fetch_trends().head())

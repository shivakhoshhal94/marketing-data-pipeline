import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple

SOURCES = ["google_ads", "facebook_ads", "organic"]
REGIONS = ["IT-NW", "IT-NE", "IT-C", "IT-S", "IT-IS"]  # simple macro areas

def gen_synthetic(days: int = 90, seed: int = 42) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    today = pd.Timestamp.today().normalize()
    dates = pd.date_range(end=today, periods=days, freq="D")
    events = []
    adspend = []

    # base rates per source (sessions per day), conv rates
    base_sessions = {
        "google_ads": 600,
        "facebook_ads": 300,
        "organic": 400,
    }
    signup_rate = {"google_ads": 0.10, "facebook_ads": 0.07, "organic": 0.05}
    purchase_rate = {"google_ads": 0.06, "facebook_ads": 0.05, "organic": 0.04}
    cpc = {"google_ads": 0.45, "facebook_ads": 0.30}  # organic has no spend

    user_counter = 1

    for d in dates:
        for src in SOURCES:
            lam = base_sessions[src] * rng.uniform(0.8, 1.2)
            sessions = rng.poisson(lam=lam)
            sessions = int(max(sessions, 0))

            session_ids = [f"{src[:2]}_{int(d.strftime('%Y%m%d'))}_{i}" for i in range(sessions)]
            regions = rng.choice(REGIONS, size=sessions, replace=True, p=[0.22,0.18,0.24,0.24,0.12])

            for sid, geo in zip(session_ids, regions):
                minute_of_day = rng.integers(9*60, 22*60)
                t = (pd.Timestamp(d) + pd.Timedelta(minutes=int(minute_of_day))).to_pydatetime()
                events.append({
                    "event_time": t,
                    "user_id": user_counter,
                    "session_id": sid,
                    "source": src,
                    "campaign": f"{src}_brand" if rng.random() < 0.5 else f"{src}_generic",
                    "geo": geo,
                    "action": "session_start",
                })
                user_counter += 1

            signups_n = int(np.round(sessions * signup_rate[src]))
            purchases_n = int(np.round(signups_n * purchase_rate[src]))

            if sessions > 0:
                signup_sids = list(rng.choice(session_ids, size=min(signups_n, sessions), replace=False))
            else:
                signup_sids = []

            if signups_n > 0:
                purchase_sids = list(rng.choice(signup_sids, size=min(purchases_n, len(signup_sids)), replace=False))
            else:
                purchase_sids = []

            for sid in signup_sids:
                minute_of_day =  rng.integers(9*60, 22*60)
                t = (pd.Timestamp(d) + pd.Timedelta(minutes=int(minute_of_day))).to_pydatetime()
                events.append({
                    "event_time": t,
                    "user_id": rng.integers(1e6),
                    "session_id": sid,
                    "source": src,
                    "campaign": f"{src}_brand",
                    "geo": rng.choice(REGIONS),
                    "action": "signup",
                })

            for sid in purchase_sids:
                minute_of_day =  rng.integers(9*60, 22*60)
                t = (pd.Timestamp(d) + pd.Timedelta(minutes=int(minute_of_day))).to_pydatetime()
                events.append({
                    "event_time": t,
                    "user_id": rng.integers(1e6),
                    "session_id": sid,
                    "source": src,
                    "campaign": f"{src}_brand",
                    "geo": rng.choice(REGIONS),
                    "action": "purchase",
                })

            if src in cpc:
                clicks = sessions  # proxy clicks ~= sessions
                spend = float(clicks * cpc[src] * rng.uniform(0.9, 1.1))
                adspend.append({"date": pd.Timestamp(d).date(), "source": src, "spend": round(spend, 2)})

    df_events = pd.DataFrame(events)
    df_adspend = pd.DataFrame(adspend)
    return df_events, df_adspend

if __name__ == "__main__":
    e,a = gen_synthetic()
    print(e.head(), a.head())

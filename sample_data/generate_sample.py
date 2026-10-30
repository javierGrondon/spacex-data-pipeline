"""
Genera datos de MUESTRA sintéticos con el mismo formato que la API v4 de SpaceX.
Se usan para tests y para correr el pipeline sin internet (USE_SAMPLE_DATA=true).
No son datos reales: el pipeline real lee https://api.spacexdata.com/v4.
"""
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
HERE = Path(__file__).parent

ROCKETS = [
    {"id": "5e9d0d95eda69955f709d1eb", "name": "Falcon 1", "type": "rocket", "active": False,
     "stages": 2, "cost_per_launch": 6700000, "success_rate_pct": 40, "first_flight": "2006-03-24",
     "country": "Republic of the Marshall Islands", "company": "SpaceX"},
    {"id": "5e9d0d95eda69973a809d1ec", "name": "Falcon 9", "type": "rocket", "active": True,
     "stages": 2, "cost_per_launch": 50000000, "success_rate_pct": 98, "first_flight": "2010-06-04",
     "country": "United States", "company": "SpaceX"},
    {"id": "5e9d0d95eda69974db09d1ed", "name": "Falcon Heavy", "type": "rocket", "active": True,
     "stages": 2, "cost_per_launch": 90000000, "success_rate_pct": 100, "first_flight": "2018-02-06",
     "country": "United States", "company": "SpaceX"},
    {"id": "5e9d0d96eda699382d09d1ee", "name": "Starship", "type": "rocket", "active": False,
     "stages": 2, "cost_per_launch": 7000000, "success_rate_pct": 0, "first_flight": "2021-12-01",
     "country": "United States", "company": "SpaceX"},
]

# lanzamientos por año (aprox.) y cohete
PLAN = {2006: 1, 2007: 1, 2008: 2, 2009: 1, 2010: 2, 2012: 2, 2013: 3, 2014: 6, 2015: 7,
        2016: 9, 2017: 18, 2018: 21, 2019: 13, 2020: 26, 2021: 31, 2022: 40}
MISSIONS = ["Starlink", "CRS", "Crew", "GPS III", "Transporter", "SES", "Iridium NEXT", "NROL"]


def make_launches():
    launches, n = [], 0
    for year, count in PLAN.items():
        for _ in range(count):
            n += 1
            date = datetime(year, 1, 1) + timedelta(days=random.randint(0, 364), hours=random.randint(0, 23))
            if year <= 2009:
                rocket = ROCKETS[0]
                success = n not in (1, 2, 3)
            else:
                rocket = ROCKETS[2] if year >= 2018 and random.random() < 0.05 else ROCKETS[1]
                success = random.random() > 0.02
            reused = year >= 2017 and random.random() < min(0.15 * (year - 2016), 0.85)
            mission = random.choice(MISSIONS) if year > 2009 else "DemoSat"
            launches.append(_launch(n, f"{mission} {n}", date, rocket["id"], success, False, reused))

    # próximos lanzamientos (sin resultado todavía)
    for i in range(5):
        n += 1
        date = datetime(2023, 1, 10) + timedelta(days=20 * i)
        launches.append(_launch(n, f"Starlink {n}", date, ROCKETS[1]["id"], None, True, True))
    return launches


def _launch(n, name, date, rocket_id, success, upcoming, reused):
    return {
        "id": f"sample{n:016d}",
        "flight_number": n,
        "name": name,
        "date_utc": date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "rocket": rocket_id,
        "success": success,
        "upcoming": upcoming,
        "details": None,
        "failures": [] if success is not False else [{"time": 140, "reason": "sample failure"}],
        "links": {"patch": {"small": None, "large": None}, "webcast": None},
        "cores": [{"core": None, "reused": reused,
                   "landing_attempt": not upcoming and rocket_id != ROCKETS[0]["id"],
                   "landing_success": (random.random() > 0.1) if not upcoming else None}],
    }


if __name__ == "__main__":
    (HERE / "rockets.json").write_text(json.dumps(ROCKETS, indent=1))
    (HERE / "launches.json").write_text(json.dumps(make_launches(), indent=1))
    print("OK")

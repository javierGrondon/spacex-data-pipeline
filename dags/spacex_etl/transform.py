import pandas as pd


def transform_rockets(raw: list) -> pd.DataFrame:
    df = pd.DataFrame(raw)
    cols = ["id", "name", "type", "active", "stages", "cost_per_launch",
            "success_rate_pct", "first_flight", "country", "company"]
    df = df.reindex(columns=cols).rename(columns={"id": "rocket_id", "name": "rocket_name"})
    df["first_flight"] = pd.to_datetime(df["first_flight"], errors="coerce").dt.date
    return df


def transform_launches(raw: list, rockets: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame(raw)
    df = df.reindex(columns=["id", "flight_number", "name", "date_utc", "rocket",
                             "success", "upcoming", "details", "links", "cores"])

    df["date_utc"] = pd.to_datetime(df["date_utc"], utc=True, errors="coerce")
    df["year"] = df["date_utc"].dt.year
    df["patch_url"] = df["links"].apply(
        lambda l: ((l or {}).get("patch") or {}).get("small") if isinstance(l, dict) else None
    )
    df["webcast_url"] = df["links"].apply(lambda l: l.get("webcast") if isinstance(l, dict) else None)
    df["reused_core"] = df["cores"].apply(
        lambda cs: any(bool(c.get("reused")) for c in cs) if isinstance(cs, list) and cs else False
    )
    df["landing_success"] = df["cores"].apply(
        lambda cs: cs[0].get("landing_success") if isinstance(cs, list) and cs else None
    )

    # Estado legible: éxito / falla / próximo / sin dato
    def status(row):
        if row["upcoming"]:
            return "upcoming"
        if row["success"] is True:
            return "success"
        if row["success"] is False:
            return "failure"
        return "unknown"

    df["status"] = df.apply(status, axis=1)

    df = df.rename(columns={"id": "launch_id", "rocket": "rocket_id"}).merge(
        rockets[["rocket_id", "rocket_name"]], on="rocket_id", how="left"
    )
    df["date_utc"] = df["date_utc"].dt.tz_localize(None)  # compatible con cualquier base

    return df[["launch_id", "flight_number", "name", "date_utc", "year", "rocket_id",
               "rocket_name", "status", "success", "upcoming", "reused_core",
               "landing_success", "details", "patch_url", "webcast_url"]]

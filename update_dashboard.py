#!/usr/bin/env python3
"""
xwOBA Dashboard Daily Updater — GitHub Actions version
Fetches live MLB data from Baseball Savant + MLB Stats API + FanGraphs
and updates index.html in place. Git commit/push is handled by the workflow.
"""

import re
import json
import time
import logging
import sys
import requests
from pathlib import Path
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
DASHBOARD_PATH = Path("index.html")
YEAR    = 2026
MIN_PA  = 1

MLB_STATS_URL = "https://statsapi.mlb.com/api/v1"
SAVANT_URL = (
    "https://baseballsavant.mlb.com/leaderboard/expected_statistics"
    f"?type=batter&year={YEAR}&position=&team=&min={MIN_PA}&csv=true"
)
FANGRAPHS_URL = (
    "https://www.fangraphs.com/api/leaders/major-league/data"
    f"?age=&pos=all&stats=bat&lg=all&qual=0&season={YEAR}&season1={YEAR}"
    "&ind=0&team=0%2Cts&rost=&month=0&hand=&startdate=&enddate="
    "&pageitems=2000000&pagenum=1&type=8&postseason=&sortdir=default&sortstat=WAR"
)
HEADERS = {"User-Agent": "xwOBA-dashboard-updater/2.0 (github-actions)"}

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ── Data fetching ─────────────────────────────────────────────────────────────

def fetch_savant_rows():
    log.info(f"Fetching Savant leaderboard for {YEAR}…")
    resp = requests.get(SAVANT_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    import csv, io
    text = resp.text.lstrip("\ufeff")
    rows = list(csv.DictReader(io.StringIO(text)))
    log.info(f"  → {len(rows)} rows returned")
    return rows


def fetch_fangraphs_wrc_plus():
    log.info("Fetching wRC+ from FanGraphs…")
    try:
        resp = requests.get(FANGRAPHS_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        results = {}
        for p in resp.json().get("data", []):
            mlbam_id = p.get("xMLBAMID")
            wrc = p.get("wRC+")
            if mlbam_id and wrc is not None:
                try:
                    results[int(mlbam_id)] = round(float(wrc))
                except (ValueError, TypeError):
                    pass
        log.info(f"  → wRC+ fetched for {len(results)} players")
        return results
    except Exception as e:
        log.warning(f"  FanGraphs failed: {e} — wRC+ will be approximated")
        return {}


def fetch_team_abbr_map():
    try:
        resp = requests.get(f"{MLB_STATS_URL}/teams?sportId=1", headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return {t["id"]: t.get("abbreviation", "") for t in resp.json().get("teams", [])}
    except Exception as e:
        log.warning(f"  Could not fetch team abbreviations: {e}")
        return {}


def fetch_player_meta_bulk(player_ids, team_abbr_map):
    log.info(f"Fetching metadata for {len(player_ids)} players…")
    results = {}
    CHUNK = 250
    for start in range(0, len(player_ids), CHUNK):
        chunk = player_ids[start:start + CHUNK]
        ids_str = ",".join(str(i) for i in chunk)
        url = f"{MLB_STATS_URL}/people?personIds={ids_str}&hydrate=currentTeam"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            for p in resp.json().get("people", []):
                pid = p["id"]
                current_team = p.get("currentTeam", {})
                team_id = current_team.get("id", "")
                team_abbr = team_abbr_map.get(team_id, "")
                team_name = current_team.get("name", "Free Agent")
                if not team_abbr:
                    parent_id = current_team.get("parentOrgId")
                    if parent_id and parent_id in team_abbr_map:
                        team_abbr = team_abbr_map[parent_id]
                        team_name = f"{team_name} ({team_abbr})"
                results[pid] = {
                    "position":  p.get("primaryPosition", {}).get("abbreviation", ""),
                    "bats":      p.get("batSide",  {}).get("code", ""),
                    "throws":    p.get("pitchHand", {}).get("code", ""),
                    "height":    p.get("height", ""),
                    "weight":    str(p.get("weight", "")),
                    "team_name": team_name,
                    "team_abbr": team_abbr,
                    "team_id":   team_id,
                }
        except Exception as e:
            log.warning(f"  Metadata chunk failed ({start}–{start+CHUNK}): {e}")
        time.sleep(0.3)
    log.info(f"  → Metadata fetched for {len(results)} players")
    return results


# ── Data processing ───────────────────────────────────────────────────────────

def normalize_row(row):
    if "player_name" in row:
        row["_name"] = row["player_name"]
    elif "last_name, first_name" in row:
        row["_name"] = row["last_name, first_name"]
    else:
        name_key = next((k for k in row if "last_name" in k.lower()), None)
        row["_name"] = row.get(name_key, "") if name_key else ""
    row["_xwoba"] = float(row.get("est_woba") or row.get("xwoba") or 0)
    row["_woba"]  = float(row.get("woba") or 0)
    row["_pa"]    = int(row.get("pa") or 0)
    xslg_raw = row.get("est_slg") or row.get("xslg") or None
    row["_xslg"] = float(xslg_raw) if xslg_raw else None
    return row


def percentile_rank(value, sorted_values):
    n = len(sorted_values)
    if n <= 1:
        return 100.0
    return round((sum(1 for v in sorted_values if v <= value) / n) * 100, 1)


def build_players_data(savant_rows, wrc_map):
    for row in savant_rows:
        try:
            normalize_row(row)
        except Exception:
            row["_xwoba"] = 0.0
            row["_woba"]  = 0.0
            row["_pa"]    = 0
            row["_xslg"]  = None

    valid = [r for r in savant_rows if r["_pa"] >= MIN_PA and r["_xwoba"] > 0]
    if not valid:
        return None, None, None, None

    n = len(valid)
    log.info(f"Processing {n} qualified players (≥{MIN_PA} PA)")

    total_pa     = sum(r["_pa"]    for r in valid)
    league_xwoba = round(sum(r["_xwoba"] * r["_pa"] for r in valid) / total_pa, 3)
    league_woba  = round(sum(r["_woba"]  * r["_pa"] for r in valid) / total_pa, 3)

    xslg_valid = [r for r in valid if r["_xslg"] is not None]
    if xslg_valid:
        xslg_pa_total = sum(r["_pa"] for r in xslg_valid)
        league_xslg = round(sum(r["_xslg"] * r["_pa"] for r in xslg_valid) / xslg_pa_total, 3)
    else:
        league_xslg = 0.410

    log.info(f"  League xwOBA: {league_xwoba}  |  League wOBA: {league_woba}  |  League xSLG: {league_xslg}")

    valid.sort(key=lambda r: r["_xwoba"], reverse=True)
    woba_sorted = sorted(r["_woba"]  for r in valid)
    pa_sorted   = sorted(r["_pa"]    for r in valid)

    player_ids = []
    for r in valid:
        try:
            player_ids.append(int(r["player_id"]))
        except (ValueError, TypeError):
            pass

    team_abbr_map = fetch_team_abbr_map()
    meta_map = fetch_player_meta_bulk(player_ids, team_abbr_map)

    players = []
    for rank, row in enumerate(valid, start=1):
        try:
            pid = int(row["player_id"])
        except (ValueError, TypeError):
            continue

        meta  = meta_map.get(pid, {})
        xwoba = row["_xwoba"]
        woba  = row["_woba"]

        xwoba_plus = round((xwoba / league_xwoba) * 100) if league_xwoba else 100

        wrc_plus = wrc_map.get(pid)
        if wrc_plus is None and league_woba:
            wrc_plus = round((woba / league_woba) * 100)

        xwoba_pct = round(((n - rank) / (n - 1)) * 100, 1) if n > 1 else 100.0
        woba_pct  = percentile_rank(woba, woba_sorted)
        pa_pct    = percentile_rank(row["_pa"], pa_sorted)

        headshot  = (
            "https://img.mlbstatic.com/mlb-photos/image/upload/"
            "d_people:generic:headshot:67:current.png/w_180/"
            f"v1/people/{pid}/headshot/67/current"
        )
        team_id   = meta.get("team_id", "")
        team_logo = (
            f"www.mlbstatic.com/team-logos/team-cap-on-light/{team_id}.svg"
            if team_id else ""
        )

        raw_name  = row["_name"]
        parts     = raw_name.split(", ", 1)
        full_name = f"{parts[1]} {parts[0]}" if len(parts) == 2 else raw_name

        player_obj = {
            "player_id":        pid,
            "name":             raw_name,
            "full_name":        full_name,
            "pa":               row["_pa"],
            "woba":             round(woba, 3),
            "xwoba":            round(xwoba, 3),
            "xwoba_plus":       xwoba_plus,
            "wrc_plus":         wrc_plus,
            "xwoba_percentile": xwoba_pct,
            "woba_percentile":  woba_pct,
            "pa_percentile":    pa_pct,
            "xwoba_rank":       rank,
            "position":         meta.get("position", ""),
            "bats":             meta.get("bats", ""),
            "throws":           meta.get("throws", ""),
            "height":           meta.get("height", ""),
            "weight":           meta.get("weight", ""),
            "headshot_url":     headshot,
            "team_name":        meta.get("team_name", "Free Agent"),
            "team_abbr":        meta.get("team_abbr", ""),
            "team_logo":        team_logo,
        }

        if row["_xslg"] is not None:
            player_obj["xslg"] = round(row["_xslg"], 3)

        players.append(player_obj)

    return players, league_xwoba, league_woba, league_xslg


# ── HTML update ───────────────────────────────────────────────────────────────

def update_dashboard_html(players, league_xwoba, league_woba, league_xslg):
    if not DASHBOARD_PATH.exists():
        log.error(f"Dashboard file not found: {DASHBOARD_PATH}")
        return False

    html = DASHBOARD_PATH.read_text(encoding="utf-8")

    html = re.sub(r"(2026:\s*\{[^}]*xwoba:\s*)[0-9.]+",  lambda m: m.group(1) + str(league_xwoba), html)
    html = re.sub(r"(2026:\s*\{[^}]*woba:\s*)[0-9.]+",   lambda m: m.group(1) + str(league_woba),  html)
    html = re.sub(r"(2026:\s*\{[^}]*xslg:\s*)[0-9.]+",   lambda m: m.group(1) + str(league_xslg),  html)

    players_json = json.dumps(players, indent=2, ensure_ascii=False)
    new_block = f"const playersData2026 = {players_json};"

    new_html, count = re.subn(
        r"const playersData2026\s*=\s*\[.*?\];",
        new_block,
        html,
        flags=re.DOTALL,
    )
    if count == 0:
        log.error("Could not find 'const playersData2026' in index.html!")
        return False

    today = datetime.now().strftime("%B %d, %Y")
    new_html = re.sub(r"Last updated:.*?(?=<|$)", f"Last updated: {today}", new_html)

    DASHBOARD_PATH.write_text(new_html, encoding="utf-8")
    log.info(f"  ✓ Wrote {len(players)} players to {DASHBOARD_PATH}")
    log.info(f"  ✓ League xwOBA: {league_xwoba} | wOBA: {league_woba} | xSLG: {league_xslg}")
    return True


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info(f"xwOBA Dashboard Updater (GitHub Actions) — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    try:
        rows = fetch_savant_rows()
    except Exception as e:
        log.error(f"Failed to fetch Savant data: {e}")
        sys.exit(1)

    wrc_map = fetch_fangraphs_wrc_plus()
    players, lg_xwoba, lg_woba, lg_xslg = build_players_data(rows, wrc_map)

    if players is None:
        log.warning(f"No qualifying data for {YEAR} yet — skipping update.")
        return

    if not update_dashboard_html(players, lg_xwoba, lg_woba, lg_xslg):
        log.error("Dashboard update failed")
        sys.exit(1)

    log.info("✓ Done!")


if __name__ == "__main__":
    main()

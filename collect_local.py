#!/usr/bin/env python3
"""
collect_local.py — Collecte locale DLP
Tourne en arrière-plan quand l'appli est lancée via DLP.command.
- Collecte toutes les 5 min pendant les horaires d'ouverture
- Vérifie si GitHub Actions tourne déjà (pour ne pas doubler)
- Pousse les nouvelles lignes vers GitHub via l'API

Prérequis : définir GITHUB_TOKEN dans DLP.command
"""
import subprocess, time, sys, os, json, base64, requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────
PARIS        = timezone(timedelta(hours=2))   # UTC+2 été
OPEN_T       = (8,  0)                        # 8h00 Paris
CLOSE_T      = (23, 30)                       # 23h30 Paris
INTERVAL     = 5 * 60                         # 5 min entre collectes
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO         = "maellepub3-dumbo/dlp-wait-times"
WORKFLOW     = "collect.yml"
SCRIPT_DIR   = Path(__file__).parent.resolve()
CSV_PATH     = SCRIPT_DIR / "data" / "wait_times.csv"
COLLECT_PY   = SCRIPT_DIR / "collect.py"
# ──────────────────────────────────────────────────────────────

def now_paris():
    return datetime.now(PARIS)

def in_hours():
    n = now_paris()
    t = n.hour * 60 + n.minute
    return OPEN_T[0]*60+OPEN_T[1] <= t < CLOSE_T[0]*60+CLOSE_T[1]

def github_action_running():
    """Retourne True si un run Actions est in_progress ou queued."""
    if not GITHUB_TOKEN:
        return False
    try:
        url = f"https://api.github.com/repos/{REPO}/actions/workflows/{WORKFLOW}/runs"
        r = requests.get(url, headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json"
        }, params={"per_page": 5}, timeout=10)
        if r.status_code != 200:
            return False
        runs = r.json().get("workflow_runs", [])
        return any(run["status"] in ("in_progress", "queued") for run in runs)
    except Exception as e:
        print(f"  [GitHub] Erreur check : {e}")
        return False

def push_to_github():
    """Pousse les nouvelles lignes du CSV local vers GitHub via l'API."""
    if not GITHUB_TOKEN:
        print("  [Push] Pas de GITHUB_TOKEN — push ignoré.")
        return
    if not CSV_PATH.exists():
        print("  [Push] CSV local introuvable.")
        return
    try:
        url     = f"https://api.github.com/repos/{REPO}/contents/data/wait_times.csv"
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json"
        }
        local_text = CSV_PATH.read_text(encoding="utf-8")

        # Récupérer le fichier distant
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            remote_data = resp.json()
            remote_text = base64.b64decode(
                remote_data["content"].replace("\n", "")
            ).decode("utf-8")

            # Fusionner : garder uniquement les lignes nouvelles
            remote_set  = set(remote_text.strip().splitlines())
            local_lines = local_text.strip().splitlines()
            new_lines   = [l for l in local_lines[1:] if l and l not in remote_set]

            if not new_lines:
                print("  [Push] Aucune nouvelle ligne.")
                return

            merged     = remote_text.rstrip() + "\n" + "\n".join(new_lines) + "\n"
            merged_b64 = base64.b64encode(merged.encode()).decode()
            payload    = {
                "message": f"Data local: {now_paris().strftime('%Y-%m-%d %H:%M Paris')}",
                "content": merged_b64,
                "sha":     remote_data["sha"],
            }
        else:
            # Fichier distant inexistant — créer
            payload = {
                "message": f"Data local: {now_paris().strftime('%Y-%m-%d %H:%M Paris')}",
                "content": base64.b64encode(local_text.encode()).decode(),
            }
            new_lines = local_text.strip().splitlines()[1:]

        put = requests.put(url, headers=headers, json=payload, timeout=15)
        if put.status_code in (200, 201):
            print(f"  [Push] {len(new_lines)} ligne(s) poussée(s) ✓")
        else:
            print(f"  [Push] Erreur {put.status_code} : {put.text[:150]}")
    except Exception as e:
        print(f"  [Push] Exception : {e}")

def collect_once():
    """Lance collect.py une fois."""
    if not COLLECT_PY.exists():
        print(f"  [Collect] {COLLECT_PY} introuvable !")
        return False
    r = subprocess.run(
        [sys.executable, str(COLLECT_PY)],
        capture_output=True, text=True, cwd=str(SCRIPT_DIR)
    )
    if r.stdout:
        print(r.stdout.strip()[-300:])
    if r.returncode != 0:
        print(f"  [Collect] Erreur : {r.stderr[-200:]}")
    return r.returncode == 0

def main():
    print("=" * 50)
    print("  Collecte locale DLP démarrée")
    print(f"  Horaires : {OPEN_T[0]}h{OPEN_T[1]:02d} – {CLOSE_T[0]}h{CLOSE_T[1]:02d} Paris")
    print(f"  Intervalle : {INTERVAL//60} min")
    print(f"  GitHub token : {'✓ push activé' if GITHUB_TOKEN else '✗ push désactivé'}")
    print("=" * 50)
    print()

    while True:
        n = now_paris()

        if not in_hours():
            print(f"[{n.strftime('%H:%M')}] Hors horaires — attente 1 min…")
            time.sleep(60)
            continue

        # Vérifier si GitHub Actions tourne
        if github_action_running():
            print(f"[{n.strftime('%H:%M')}] GitHub Actions en cours — collecte locale sautée.")
        else:
            print(f"[{n.strftime('%H:%M')}] Collecte locale…")
            ok = collect_once()
            if ok:
                push_to_github()

        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()

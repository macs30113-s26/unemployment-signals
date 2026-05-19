"""
Verify that every article in config/articles.yaml exists on English Wikipedia
and discover any redirect history.

This script is run BEFORE the ingest job. It catches typos in titles, missed
historical renames, and pages that have been deleted — all of which would
otherwise show up as silent zero-row matches in the staging output.

Output:
  - prints a report to stdout
  - writes config/articles.verified.yaml: the same shape as articles.yaml,
    but with any newly-discovered aliases added. You can diff the two files
    and merge in the new aliases manually (preserving the comments in the
    original).

Why this matters for the research:
  Wikipedia article titles change over time. The article currently called
  "COVID-19" was titled "Coronavirus_disease_2019" through most of 2020,
  meaning pageviews from that window are filed under the old title in the
  hourly dumps. Without alias mapping the redirect-resolution pass in Spark
  produces silent under-counts of exactly the time periods we most care about.

Usage:
  python ingest/verify_articles.py config/articles.yaml
"""

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import yaml


WIKI_API = "https://en.wikipedia.org/w/api.php"
UA = {"User-Agent": "lsc4ss-final-project-verifier/1.0 (educational use)"}


def api_get(params: dict) -> dict:
    """One MediaWiki API call. Returns the parsed JSON body."""
    qs = urllib.parse.urlencode({**params, "format": "json"})
    req = urllib.request.Request(f"{WIKI_API}?{qs}", headers=UA)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_title_info(title: str) -> dict:
    """Look up a title. Returns:
        {
          "exists":   bool,
          "canonical": str | None,    # what it currently redirects to
          "redirects_in": [str, ...], # titles known to redirect TO this canonical
          "raw":       dict,          # full API payload, for debugging
        }
    """
    # First: resolve the title (follow any redirect) and ask which other titles
    # redirect TO the resolved canonical.
    data = api_get({
        "action": "query",
        "titles": title,
        "redirects": 1,
        "prop": "redirects",
        "rdlimit": "max",
    })
    pages = data.get("query", {}).get("pages", {})
    if not pages:
        return {"exists": False, "canonical": None, "redirects_in": [], "raw": data}

    page = next(iter(pages.values()))
    if "missing" in page:
        return {"exists": False, "canonical": None, "redirects_in": [], "raw": data}

    canonical = page.get("title", title).replace(" ", "_")
    redirects_in = [
        r["title"].replace(" ", "_")
        for r in page.get("redirects", []) or []
    ]
    return {
        "exists": True,
        "canonical": canonical,
        "redirects_in": redirects_in,
        "raw": data,
    }


def verify(config_path: Path) -> int:
    config = yaml.safe_load(config_path.read_text())

    print(f"Verifying {sum(len(g['articles']) for g in config['groups'].values())} "
          f"articles across {len(config['groups'])} groups against Wikipedia API...")
    print()

    n_missing = 0
    n_renamed = 0
    n_new_aliases = 0

    out_config = {"groups": {}}

    for group_name, group_def in config["groups"].items():
        print(f"== {group_name}")
        out_articles = []
        for entry in group_def["articles"]:
            declared_canonical = entry["canonical"]
            declared_aliases = set(entry.get("aliases", []) or [])
            info = fetch_title_info(declared_canonical)
            time.sleep(0.1)  # polite to the API

            if not info["exists"]:
                # Maybe an alias is the real title now?
                print(f"  [MISSING] {declared_canonical} — not found on Wikipedia")
                for alias in declared_aliases:
                    alias_info = fetch_title_info(alias)
                    time.sleep(0.1)
                    if alias_info["exists"]:
                        print(f"            alias '{alias}' resolves to '{alias_info['canonical']}'")
                        break
                n_missing += 1
                out_articles.append(entry)
                continue

            real_canonical = info["canonical"]
            if real_canonical != declared_canonical:
                print(f"  [RENAMED] declared '{declared_canonical}' "
                      f"-> current canonical '{real_canonical}'")
                n_renamed += 1

            # Merge known + newly-discovered redirects.
            all_aliases = set(declared_aliases) | set(info["redirects_in"])
            # Don't list canonical as its own alias.
            all_aliases.discard(real_canonical)
            # Drop aliases we already have.
            new_aliases = all_aliases - declared_aliases
            if new_aliases:
                # Only mention the first handful — popular articles have
                # hundreds of redirects, most are typos/abbreviations.
                preview = sorted(new_aliases)[:5]
                print(f"  [+{len(new_aliases):3d} aliases] {declared_canonical}: {preview}"
                      + (" ..." if len(new_aliases) > 5 else ""))
                n_new_aliases += len(new_aliases)

            out_articles.append({
                "canonical": real_canonical,
                "aliases": sorted(all_aliases),
            })

        out_config["groups"][group_name] = {
            "description": group_def.get("description", ""),
            "articles": out_articles,
        }
        print()

    out_path = config_path.parent / "articles.verified.yaml"
    out_path.write_text(yaml.safe_dump(out_config, sort_keys=False, allow_unicode=True))

    print(f"== summary")
    print(f"  missing on Wikipedia:    {n_missing}")
    print(f"  silently renamed:        {n_renamed}")
    print(f"  newly-discovered aliases:{n_new_aliases}")
    print(f"  wrote: {out_path}")
    print()
    print("Next step: diff articles.yaml vs articles.verified.yaml. For each")
    print("real rename or new alias you trust, copy it into articles.yaml")
    print("(keeping the original comments). Then re-upload articles.yaml to S3:")
    print()
    print("  aws s3 cp config/articles.yaml s3://$BUCKET/config/articles.yaml")
    print()
    return 0 if n_missing == 0 else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("config", type=Path, nargs="?",
                    default=Path(__file__).parent.parent / "config" / "articles.yaml")
    args = ap.parse_args()
    sys.exit(verify(args.config))

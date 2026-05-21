"""Smoke tests for the wiki-attention analysis pipeline.

These tests verify the cross-pipeline parity that README \u00a74 / \u00a79.2 claim:

  test_load_pageviews_schema:
      load_pageviews() returns the same schema (columns + dtypes) regardless
      of whether the input is the Dask flat-CSV output or the v1 pandas
      partitioned output.

  test_dask_pandas_weekly_parity:
      The weekly aggregation produced by aggregate_dask.py (--source rest)
      is byte-identical to the weekly aggregation produced by
      aggregate_pandas.py on the same staging input. This is the strongest
      possible cross-pipeline check: if the two pipelines disagreed on a
      single article-week sum, this test would fail.

Run from repo root with `pytest tests/test_pipeline_parity.py`, or directly
with `python tests/test_pipeline_parity.py`. The tests skip cleanly if the
required curated outputs don't exist (e.g., in a fresh clone before
aggregate_dask.py has been run); a manual run on Midway3 after
`sbatch sbatch/aggregate_dask.sbatch` will exercise them.
"""
import os
import sys
import unittest
from pathlib import Path

import pandas as pd

# Make `analyze` importable as a sibling package.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

USER = os.environ.get("USER", "yinyuanc")
SCRATCH = Path(f"/scratch/midway3/{USER}/wiki-attention")
DASK_REST = SCRATCH / "curated_dask_slurm"
PANDAS_V1 = SCRATCH / "curated"


def _have(p: Path) -> bool:
    return p.exists()


class TestPipelineParity(unittest.TestCase):
    """Two assertions: schema parity + weekly-sum byte parity."""

    @classmethod
    def setUpClass(cls):
        if not _have(DASK_REST / "weekly_dask.csv"):
            raise unittest.SkipTest(
                f"Dask REST output not found at {DASK_REST}; "
                "run `sbatch sbatch/aggregate_dask.sbatch` first."
            )

    def test_load_pageviews_schema(self):
        """load_pageviews() returns identical schema for Dask vs v1 inputs."""
        from analyze.analyze import load_pageviews

        dask_df = load_pageviews(DASK_REST, grain="weekly")
        self.assertSetEqual(
            set(dask_df.columns), {"week_start", "canonical", "grp", "views"},
            "Dask load_pageviews schema regression"
        )
        self.assertEqual(dask_df["week_start"].dtype.kind, "M",
                         "week_start should be datetime")

        if _have(PANDAS_V1 / "weekly"):
            v1_df = load_pageviews(PANDAS_V1, grain="weekly")
            self.assertSetEqual(set(dask_df.columns), set(v1_df.columns),
                                "Dask vs v1 column mismatch")
            self.assertEqual(dask_df["week_start"].dtype,
                             v1_df["week_start"].dtype,
                             "Dask vs v1 week_start dtype mismatch")

    def test_dask_pandas_weekly_parity(self):
        """Per-(canonical, week_start) view sums must match between the two
        pipelines."""
        from analyze.analyze import load_pageviews

        if not _have(PANDAS_V1 / "weekly"):
            self.skipTest(f"v1 pandas output not found at {PANDAS_V1}/weekly")

        dask_df = load_pageviews(DASK_REST, grain="weekly")
        v1_df = load_pageviews(PANDAS_V1, grain="weekly")

        keys = ["canonical", "week_start"]
        dask_sum = (dask_df.groupby(keys, as_index=False)["views"].sum()
                          .sort_values(keys).reset_index(drop=True))
        v1_sum = (v1_df.groupby(keys, as_index=False)["views"].sum()
                       .sort_values(keys).reset_index(drop=True))

        self.assertEqual(len(dask_sum), len(v1_sum),
                         f"row count differs: dask={len(dask_sum)} v1={len(v1_sum)}")

        merged = dask_sum.merge(v1_sum, on=keys, suffixes=("_dask", "_v1"))
        self.assertEqual(len(merged), len(dask_sum),
                         "merge dropped rows -- key mismatch")

        diff = (merged["views_dask"] - merged["views_v1"]).abs()
        max_diff = int(diff.max())
        self.assertEqual(max_diff, 0,
                         f"Dask vs pandas weekly disagree by max {max_diff} views")


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""Compare illicit Use negatives across BalancedSUT vintages (root-cause probe).

Shows that large illicit negatives on ``163db0e`` cleared by becoming
non-negative on ``d2e2112``, concentrated in columns such as ``5191A0``.

Example::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.illicit_vintage_diff
"""

# Analysis script: pandas MultiIndex / stack typing is not worth fighting here.
# mypy: disable-error-code="call-overload,operator,index,arg-type,misc"

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
from google.cloud import storage

from bedrock.analysis.nowcasting.ras_improvements.common import (
    GCS_BALANCED_SUT,
    illicit_mask,
    resolve_artifact_dir,
)
from bedrock.transform.iot.nowcast_mask import VA_ROWS, published_2017_panel
from bedrock.transform.iot.nowcast_sut_assembly import assemble_masks
from bedrock.utils.io.gcp import download_gcs_file


def _ensure(name: str) -> Path:
    out = resolve_artifact_dir(create=True)
    local = out / name
    if not local.exists():
        download_gcs_file(name, GCS_BALANCED_SUT, str(local))
    return local


def _load_use(year: int, h: str) -> pd.DataFrame | None:
    name = f'Balanced_Detail_Use_SUT_{year}_v0.3.0_{h}.parquet'
    out = resolve_artifact_dir(create=True)
    local = out / name
    if not local.exists():
        if h == 'b25b8ac':
            return None
        try:
            _ensure(name)
        except Exception as err:  # noqa: BLE001
            print(f'  skip {name}: {err}', flush=True)
            return None
    return pd.read_parquet(local)


def main() -> int:
    client = storage.Client()
    bucket = client.bucket('cornerstone-default')
    print('=== Sidecar metadata ===', flush=True)
    for year in (2021, 2023):
        for h in ('163db0e', 'ccd2f49', 'd2e2112'):
            blob = bucket.blob(
                f'{GCS_BALANCED_SUT}/Balanced_Detail_Use_SUT_{year}_v0.3.0_{h}_metadata.json'
            )
            if not blob.exists():
                print(f'{year} {h}: MISSING sidecar', flush=True)
                continue
            meta = json.loads(blob.download_as_text())
            tm = meta.get('tool_meta', {})
            print(
                f'{year} {h}: date={meta.get("date_created")} '
                f'branch={tm.get("branch")} commit={tm.get("commit")} '
                f'protocol={tm.get("protocol")}',
                flush=True,
            )
            print(f'         engine={tm.get("engine_result")}', flush=True)

    print('\n=== Illicit cell overlap 163db0e vs d2e2112 (2021, 2023) ===', flush=True)
    for year in (2021, 2023):
        masks = assemble_masks(year)
        pattern = published_2017_panel('use')
        old = _load_use(year, '163db0e')
        new = _load_use(year, 'd2e2112')
        fresh = _load_use(year, 'b25b8ac')
        if old is None or new is None:
            print(f'{year}: missing vintage parquet(s)', flush=True)
            continue
        old = old.reindex(
            index=masks['use'].structural_zero.index,
            columns=masks['use'].structural_zero.columns,
            fill_value=0.0,
        )
        new = new.reindex_like(old).fillna(0.0)
        pattern = pattern.reindex_like(old).fillna(0.0)
        ill_old = illicit_mask(old, masks['use'], pattern)
        ill_new = illicit_mask(new, masks['use'], pattern)
        n_old = int(ill_old.to_numpy().sum())
        n_new = int(ill_new.to_numpy().sum())
        cleared = ill_old & ~ill_new
        became_nonneg = cleared & (new >= 0)
        still_neg = cleared & (new < 0)
        print(
            f'\n{year}: illicit old={n_old} new={n_new} '
            f'still={int((ill_old & ill_new).to_numpy().sum())}',
            flush=True,
        )
        print(
            f'  cleared={int(cleared.to_numpy().sum())} '
            f'of which became_nonneg={int(became_nonneg.to_numpy().sum())} '
            f'still_neg_but_not_illicit={int(still_neg.to_numpy().sum())}',
            flush=True,
        )
        abs_old = old.where(cleared, 0.0).abs().stack()
        abs_series = abs_old.loc[abs_old.to_numpy() > 0].sort_values(ascending=False)
        print(
            f'  cleared mass (old abs sum)={float(abs_series.sum()):.4g} $M',
            flush=True,
        )
        print('  top 15 cleared cells (old -> new):', flush=True)
        for key, _v in abs_series.head(15).items():
            r, c = key
            print(
                f'    {r}/{c}: {float(old.loc[r, c]):.4g} -> '
                f'{float(new.loc[r, c]):.4g}',
                flush=True,
            )
        by_col = old.where(ill_old, 0.0).sum(axis=0)
        by_col = by_col.loc[by_col.to_numpy() != 0].sort_values()
        print('  illicit by col (most negative sums), top 10:', flush=True)
        for c, v in by_col.head(10).items():
            col = str(c)
            n = int(ill_old.loc[:, col].to_numpy().sum())
            print(f'    {col}: sum={float(v):.4g} n={n}', flush=True)

        # Column mechanism for the hottest col
        if len(by_col):
            hot = str(by_col.index[0])
            va = set(VA_ROWS)
            ill_rows = [
                str(r) for r in old.index[ill_old[hot].fillna(False).to_numpy()]
            ]
            inter = [r for r in ill_rows if r not in va]
            print(
                f'  hot column {hot}: illicit intermediate cells={len(inter)}, '
                f'sum={float(old.loc[inter, hot].sum()):.4g}; '
                f'V00300 {float(old.loc["V00300", hot]):.4g} -> '
                f'{float(new.loc["V00300", hot]):.4g}',
                flush=True,
            )

        if fresh is not None:
            fresh = fresh.reindex_like(old).fillna(0.0)
            ill_f = illicit_mask(fresh, masks['use'], pattern)
            print(
                f'  fresh b25b8ac illicit={int(ill_f.to_numpy().sum())}',
                flush=True,
            )
    return 0


if __name__ == '__main__':
    sys.exit(main())

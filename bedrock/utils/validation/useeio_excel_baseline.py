"""Load USEEIO baseline matrices from a pinned GCS-hosted Excel export.

The default pin (``bedrock/utils/snapshots/useeio_baseline_pin.json``) targets a
frozen **USEEIOv2.6.0-phoebe-23** workbook. For citation and
upstream model context, see ``useeio_baseline_pin.provenance.md``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.utils.config.usa_config import USAConfig, get_usa_config
from bedrock.utils.io.gcp import download_gcs_file, download_gcs_file_if_not_exists
from bedrock.utils.snapshots.loader import useeio_baseline_local_dir

logger = logging.getLogger(__name__)

_CORNERSTONE_DEFAULT_PREFIX = 'gs://cornerstone-default/'


@dataclass(frozen=True)
class UseeioBaselineBundle:
    """USEEIO-from-Excel diagnostics baseline (native BEA detail codes after /US strip)."""

    adom_old: pd.DataFrame
    aimp_old: pd.DataFrame
    d_ghg_direct: pd.Series[float]
    n_ghg_total: pd.Series[float]
    b_old_synthetic: pd.DataFrame
    y_nab_old: pd.Series[float]
    dollar_year: int
    co2e_basis: str
    source_gs_uri: str
    file_sha256: str


def split_cornerstone_default_gs_uri(gs_uri: str) -> tuple[str, str]:
    """Return ``(file_name, sub_bucket)`` for ``download_gcs_file``."""
    if not gs_uri.startswith(_CORNERSTONE_DEFAULT_PREFIX):
        raise ValueError(
            f'USEEIO baseline URI must start with {_CORNERSTONE_DEFAULT_PREFIX!r}, got {gs_uri!r}'
        )
    rest = gs_uri[len(_CORNERSTONE_DEFAULT_PREFIX) :].strip('/')
    parts = rest.split('/')
    if len(parts) < 2:
        raise ValueError(
            f'Invalid GCS object path under cornerstone-default: {gs_uri!r}'
        )
    filename = parts[-1]
    sub_bucket = '/'.join(parts[:-1])
    return filename, sub_bucket


_PIN_JSON_TO_CONFIG: dict[str, str] = {
    'gs_uri': 'useeio_baseline_xlsx_gs_uri',
    'sha256': 'useeio_baseline_xlsx_sha256',
    'model_version_label': 'useeio_model_version_label',
}


def load_useeio_baseline_pin_overrides(pin_json_path: str) -> dict[str, str]:
    """Load committed pin file → keys accepted by ``diagnostics_cli_overrides`` / ``USAConfig``.

    JSON must contain ``gs_uri``, ``sha256`` (64 hex), and ``model_version_label``.
    """
    path = Path(pin_json_path)
    raw = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(raw, dict):
        raise ValueError(f'USEEIO pin JSON must be an object, got {type(raw).__name__}')
    out: dict[str, str] = {}
    for jk, ck in _PIN_JSON_TO_CONFIG.items():
        if jk not in raw:
            raise ValueError(f'Missing key {jk!r} in USEEIO pin JSON {pin_json_path!r}')
        val = raw[jk]
        if val is None or (isinstance(val, str) and not str(val).strip()):
            raise ValueError(
                f'Empty value for {jk!r} in USEEIO pin JSON {pin_json_path!r}'
            )
        out[ck] = str(val).strip()
    return out


def _file_sha256_hex(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _local_cache_path(gs_uri: str) -> str:
    safe = re.sub(
        r'[^a-zA-Z0-9_.-]+', '_', gs_uri.removeprefix(_CORNERSTONE_DEFAULT_PREFIX)
    )
    cache_dir = useeio_baseline_local_dir()
    # Avoid ``foo.xlsx.xlsx`` when the URI tail already ends with ``.xlsx``.
    filename = safe if safe.lower().endswith('.xlsx') else f'{safe}.xlsx'
    return os.path.join(cache_dir, filename)


def ensure_useeio_xlsx_local(gs_uri: str, expected_sha256: str, local_pth: str) -> None:
    """Download if missing or SHA256 mismatch (``download_gcs_file_if_not_exists`` is unsafe)."""
    exp = expected_sha256.strip().lower()
    if len(exp) != 64 or any(c not in '0123456789abcdef' for c in exp):
        raise ValueError(
            'useeio_baseline_xlsx_sha256 must be a 64-char lowercase hex string'
        )

    if os.path.isfile(local_pth):
        got = _file_sha256_hex(local_pth)
        if got.lower() == exp:
            logger.info('USEEIO baseline xlsx cache hit (SHA256 ok): %s', local_pth)
            return
        logger.warning(
            'USEEIO baseline xlsx SHA mismatch (got %s, expected %s); re-downloading',
            got[:16],
            exp[:16],
        )
        os.remove(local_pth)

    name, sub_bucket = split_cornerstone_default_gs_uri(gs_uri)
    download_gcs_file(name, sub_bucket, local_pth)
    got = _file_sha256_hex(local_pth)
    if got.lower() != exp:
        raise ValueError(
            f'USEEIO baseline xlsx SHA256 mismatch after download: got {got}, expected {exp}'
        )


def _strip_code_loc_index(idx: pd.Index) -> pd.Index:
    out: list[str] = []
    for x in idx.astype(str):
        x = str(x).strip()
        if x.endswith('/US'):
            out.append(x[: -len('/US')])
        else:
            out.append(x)
    return pd.Index(out, name=idx.name)


def _align_square_matrix(mat: pd.DataFrame) -> pd.DataFrame:
    """Strip ``/US`` from index and columns; align row/col order."""
    ri = _strip_code_loc_index(mat.index)
    ci = _strip_code_loc_index(mat.columns)
    m = mat.copy()
    m.index = ri
    m.columns = ci
    m = m.astype(np.float64)
    return m


def _read_square_matrix(path: str, sheet: str) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=sheet, header=None, engine='openpyxl')
    col_labels = raw.iloc[0, 1:].astype(str).str.strip()
    row_labels = raw.iloc[1:, 0].astype(str).str.strip()
    mat = raw.iloc[1:, 1:].astype(np.float64)
    mat.index = row_labels
    mat.columns = col_labels
    return _align_square_matrix(mat)


def _read_indicator_by_sector(path: str, sheet: str) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=sheet, header=None, engine='openpyxl')
    col_labels = raw.iloc[0, 1:].astype(str).str.strip()
    row_labels = raw.iloc[1:, 0].astype(str).str.strip()
    mat = raw.iloc[1:, 1:].astype(np.float64)
    mat.index = row_labels
    mat.columns = col_labels
    mat.columns = _strip_code_loc_index(mat.columns)
    return mat


def _select_ghg_row(mat: pd.DataFrame) -> pd.Series[float]:
    idx = mat.index.astype(str)
    mask = idx.str.contains('Greenhouse', case=False, na=False) & idx.str.contains(
        'Gases', case=False, na=False
    )
    hits = mat.loc[mask]
    if hits.shape[0] == 0:
        raise ValueError(
            f'No GHG indicator row found on sheet (expected label containing '
            f'"Greenhouse" and "Gases"); index sample: {idx[:5].tolist()}'
        )
    if hits.shape[0] > 1:
        raise ValueError(
            f'Multiple GHG indicator rows match on sheet: {hits.index.tolist()!r}'
        )
    row = hits.iloc[0].astype(float)
    row.index = _strip_code_loc_index(row.index)
    return row


def _infer_dollar_year(path: str) -> int:
    try:
        dem = pd.read_excel(path, sheet_name='demands', header=0, engine='openpyxl')
        if 'Year' in dem.columns:
            y = int(pd.to_numeric(dem['Year'].iloc[0], errors='coerce'))
            if 1990 <= y <= 2100:
                return y
    except Exception as e:
        logger.debug('Could not read demands Year: %s', e)
    return 2017


def _read_production_complete_demand(path: str) -> pd.Series[float]:
    raw = pd.read_excel(
        path,
        sheet_name='2017_US_Production_Complete',
        header=None,
        engine='openpyxl',
    )
    pairs = raw.iloc[1:, :2].dropna(how='all')
    codes = pairs.iloc[:, 0].astype(str).str.strip()
    vals = pairs.iloc[:, 1].astype(float)
    codes_stripped = pd.Index(
        [c[:-3] if c.endswith('/US') else c for c in codes], dtype=object
    )
    s = pd.Series(vals.values, index=codes_stripped, dtype=float)
    if s.index.has_duplicates:
        s = s.groupby(s.index).sum()
    return s


def load_useeio_baseline_bundle(cfg: USAConfig | None = None) -> UseeioBaselineBundle:
    """Download (if needed), verify SHA256, parse workbook, return the baseline bundle."""
    c = cfg or get_usa_config()
    if c.diagnostics_baseline_source != 'gcs_useeio_xlsx':
        raise ValueError(
            'load_useeio_baseline_bundle requires diagnostics_baseline_source '
            "== 'gcs_useeio_xlsx'"
        )
    if not c.useeio_baseline_xlsx_gs_uri:
        raise ValueError('useeio_baseline_xlsx_gs_uri is required')

    gs_uri = c.useeio_baseline_xlsx_gs_uri.strip()
    local_pth = _local_cache_path(gs_uri)
    sha_opt = (c.useeio_baseline_xlsx_sha256 or '').strip()
    if sha_opt:
        ensure_useeio_xlsx_local(gs_uri, sha_opt, local_pth)
    else:
        name, sub_bucket = split_cornerstone_default_gs_uri(gs_uri)
        download_gcs_file_if_not_exists(name, sub_bucket, local_pth)
        if not os.path.isfile(local_pth):
            raise FileNotFoundError(
                f'USEEIO baseline xlsx not found at GCS after download attempt: {gs_uri}'
            )
    file_sha = _file_sha256_hex(local_pth) if os.path.isfile(local_pth) else ''

    A = _read_square_matrix(local_pth, 'A')
    A_d = _read_square_matrix(local_pth, 'A_d')
    if not A.index.equals(A_d.index) or not A.columns.equals(A_d.columns):
        raise ValueError('A and A_d index/column mismatch after parse')

    aimp = A - A_d
    aimp = aimp.clip(lower=-1e-10)
    aimp = aimp.clip(lower=0.0)

    D_mat = _read_indicator_by_sector(local_pth, 'D')
    N_mat = _read_indicator_by_sector(local_pth, 'N')
    d_row = _select_ghg_row(D_mat)
    n_row = _select_ghg_row(N_mat)

    sectors = d_row.index.union(n_row.index).sort_values()
    d_row = d_row.reindex(sectors, fill_value=0.0)
    n_row = n_row.reindex(sectors, fill_value=0.0)

    y_nab = _read_production_complete_demand(local_pth)
    Adom = A_d.reindex(index=sectors, columns=sectors, fill_value=0.0)
    Aimp = aimp.reindex(index=sectors, columns=sectors, fill_value=0.0)
    y_nab = y_nab.reindex(sectors, fill_value=0.0)

    b_old = pd.DataFrame([d_row.values], columns=d_row.index, dtype=float)
    b_old.index = pd.Index(['GHG_CO2e'])

    dollar_year = _infer_dollar_year(local_pth)
    co2e_basis = 'Impact Potential/GHG/kg CO2 eq (USEEIO D/N row)'

    return UseeioBaselineBundle(
        adom_old=Adom,
        aimp_old=Aimp,
        d_ghg_direct=d_row,
        n_ghg_total=n_row,
        b_old_synthetic=b_old,
        y_nab_old=y_nab,
        dollar_year=dollar_year,
        co2e_basis=co2e_basis,
        source_gs_uri=gs_uri,
        file_sha256=file_sha,
    )


def derive_useeio_excel_y_nab_scaled_to_model_base_year(
    cfg: USAConfig,
    ub: UseeioBaselineBundle,
) -> pd.Series[float]:
    """Scale USEEIO workbook ``y_nab`` like ``derive_cornerstone_y_nab``.

    Builds summary national-accounting ``y_nab`` at ``cfg.usa_io_data_year`` (same
    BEA inputs as the cornerstone path), disaggregates to detail using the
    USEEIO workbook detail vector as weights (via
    ``get_bea_v2017_summary_to_useeio_corresp_df``). Negative workbook weights are
    clipped to zero before disaggregation so column normalization stays valid.
    ``_disaggregate_and_inflate_vector`` inflates using ``usa_io_data_year`` →
    ``model_base_year`` (not workbook ``dollar_year``).

    Note:
    This helper is currently used for diagnostics/support comparisons (e.g. CSV
    exports), not for the active USEEIO BLy ``y_old`` path. The BLy USEEIO path
    now uses Cornerstone ``derive_cornerstone_y_nab`` reindexed to USEEIO axis.
    """
    from bedrock.extract.iot.io_2017 import load_summary_Uimp_usa  # noqa: PLC0415
    from bedrock.transform.eeio.derived_2017 import (  # noqa: PLC0415
        derive_summary_Ytot_usa_matrix_set,
    )
    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        _disaggregate_and_inflate_vector,
    )
    from bedrock.utils.math.formulas import (  # noqa: PLC0415
        compute_y_for_national_accounting_balance,
        compute_y_imp,
    )
    from bedrock.utils.math.handle_negatives import (  # noqa: PLC0415
        handle_negative_vector_values,
    )
    from bedrock.utils.taxonomy.bea.v2017_industry_summary import (  # noqa: PLC0415
        USA_2017_SUMMARY_INDUSTRY_CODES,
    )
    from bedrock.utils.taxonomy.bea_v2017_to_ceda_v7_helpers import (  # noqa: PLC0415
        get_bea_v2017_summary_to_useeio_corresp_df,
    )

    corresp_df = get_bea_v2017_summary_to_useeio_corresp_df(ub.y_nab_old.index)
    summary_Y = derive_summary_Ytot_usa_matrix_set(cfg.usa_io_data_year)
    y_nab_summary = compute_y_for_national_accounting_balance(
        y_tot=summary_Y.ytot,
        y_imp=compute_y_imp(
            imports=summary_Y.imports,
            Uimp=load_summary_Uimp_usa(cfg.usa_io_data_year).loc[
                USA_2017_SUMMARY_INDUSTRY_CODES,
                USA_2017_SUMMARY_INDUSTRY_CODES,
            ],
        ),
        exports=summary_Y.exports,
    )
    y_base = y_nab_summary.reindex(corresp_df.columns, fill_value=0.0)
    # Summary columns with no 1 in any USEEIO row cannot receive mass in
    # ``disaggregate_vector`` (normalized column weights would be all zero).
    # Leaving y_nab there breaks conservation vs ``base_series.sum()``.
    unmapped_summary = corresp_df.columns[corresp_df.sum(axis=0) == 0]
    if len(unmapped_summary) > 0:
        dropped_mass = float(y_base.reindex(unmapped_summary).fillna(0).sum())
        y_base = y_base.copy()
        y_base.loc[unmapped_summary] = 0.0
        logger.warning(
            'USEEIO y scaling: %d BEA summary columns have no USEEIO detail row; '
            'zeroing y_nab there so disaggregation conserves (mass dropped ~%.6g). '
            'Columns: %s',
            len(unmapped_summary),
            dropped_mass,
            list(unmapped_summary),
        )
    weight = ub.y_nab_old.reindex(corresp_df.index, fill_value=0.0)
    # Workbook ``y_nab`` can be negative on some detail codes; within one BEA
    # summary column the weighted column sum can then be <= 0, which breaks
    # ``disaggregate_vector`` normalization and yields negative shares that
    # ``handle_negative_vector_values`` clears to zero (e.g. positive 115000
    # with siblings summing negative in 113FF).
    neg_mask = weight < 0
    if neg_mask.any():
        logger.warning(
            'USEEIO y scaling: clipping %d negative workbook y_nab weights to 0 '
            'for disaggregation (sum of clipped values ~%.6g)',
            int(neg_mask.sum()),
            float((-weight).where(neg_mask, 0.0).sum()),
        )
        weight = weight.clip(lower=0.0)
    y_scaled = _disaggregate_and_inflate_vector(
        base=y_base,
        weight=weight,
        corresp_df=corresp_df,
        original_year=cfg.usa_io_data_year,
        target_year=cfg.model_base_year,
    )
    return handle_negative_vector_values(y_scaled)

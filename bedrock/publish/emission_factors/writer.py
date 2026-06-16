"""Write cornerstone supply-chain emission factor CSVs."""

from __future__ import annotations

import logging
import os

from bedrock.publish.emission_factors.table import (
    build_emission_factor_table,
    build_purchaser_matrices,
    finalize_cornerstone_ef_table,
)
from bedrock.publish.model_objects import apply_loc_suffix, require_cornerstone_config

logger = logging.getLogger(__name__)


def write_emission_factors(
    output_dir: str,
    *,
    config_name: str,
    dollar_year: int,
    write_matrices: bool = False,
) -> dict[str, str]:
    """Write CO2e SEF CSV (and optional M/N purchaser matrices) under ``output_dir``."""
    require_cornerstone_config()
    os.makedirs(output_dir, exist_ok=True)

    table = finalize_cornerstone_ef_table(
        build_emission_factor_table(dollar_year=dollar_year)
    )
    co2e_name = f'CornerstoneSupplyChainGHG_CO2e_USD{dollar_year}.csv'
    co2e_path = os.path.join(output_dir, co2e_name)
    table.to_csv(co2e_path, index=False)
    logger.info(
        'publish: wrote %d emission-factor rows to %s (config=%s)',
        len(table),
        co2e_path,
        config_name,
    )

    paths: dict[str, str] = {'co2e': co2e_path}

    if write_matrices:
        matrices_dir = os.path.join(output_dir, 'matrices')
        os.makedirs(matrices_dir, exist_ok=True)
        m_pur, n_pur = build_purchaser_matrices(dollar_year=dollar_year)
        m_path = os.path.join(matrices_dir, f'M_pur_{dollar_year}.csv')
        n_path = os.path.join(matrices_dir, f'N_pur_{dollar_year}.csv')
        apply_loc_suffix(m_pur).to_csv(m_path)
        apply_loc_suffix(n_pur).to_csv(n_path)
        paths['M_pur'] = m_path
        paths['N_pur'] = n_path
        logger.info('publish: wrote purchaser matrices to %s', matrices_dir)

    return paths

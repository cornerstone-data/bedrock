"""Write ``electricity_payroll_split.csv``: private payroll of electric power's two halves (#1012).

QCEW's NAICS 22111 is power generation and 22112 is power transmission and
distribution. The generation and delivery split of electric power
(:func:`~bedrock.transform.eeio.electricity_delivery.delivery_payroll_share`) gives
delivery that share of compensation.

Private ownership only: government payroll sits outside ``221100``. National
(``Location`` 00000), annual payroll, in USD.

The CSV is committed so the build never loads the QCEW FBA, which is 9M rows a year.
Rerun this to refresh it, then commit the diff.

Run::

    uv run python -m bedrock.utils.mapping.write_electric_payroll_split
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

OUTPUT = Path('bedrock/transform/allocation/data/electricity_payroll_split.csv')
YEARS = range(2017, 2025)
GENERATION = '22111'
DELIVERY = '22112'
FLOW = 'Annual payroll, Private'


def payroll(year: int) -> dict[str, float]:
    """Private payroll in USD for generation and for delivery in *year*."""
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    fba = pd.DataFrame(getFlowByActivity('BLS_QCEW', year))
    rows = fba[
        (fba['Location'] == '00000')
        & (fba['FlowName'] == FLOW)
        & fba['ActivityProducedBy'].astype(str).isin([GENERATION, DELIVERY])
    ]
    by_code = rows.groupby(rows['ActivityProducedBy'].astype(str))['FlowAmount'].sum()
    return {
        'year': year,
        'generation_payroll_usd': float(by_code[GENERATION]),
        'delivery_payroll_usd': float(by_code[DELIVERY]),
    }


def main() -> None:
    frame = pd.DataFrame([payroll(year) for year in YEARS])
    frame.to_csv(OUTPUT, index=False)
    print(frame.to_string(index=False))
    print(f'wrote {OUTPUT}')


if __name__ == '__main__':
    main()

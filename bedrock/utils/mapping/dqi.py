"""
Functions associated with data quality scoring
"""

import numpy as np
import pandas as pd


def adjust_dqi_reliability_collection_scores(
        df: pd.DataFrame
) -> pd.DataFrame:
    """
    Adjust the dqi scores for
    Data Reliability, Data Collection

    based on source sectors and target sectors

    Df must have 5 columns: DataReliability, DataCollection, source_sector, target_sector, SectorSourceName

    :param df:
    :return:
    """

    if 'SectorSourceName' not in df.columns:
        return df

    from bedrock.utils.mapping.sector import _sector_level_table  # noqa: PLC0415

    levels = _sector_level_table()
    df2 = df.copy()
    for c in ['source', 'target']:
        df2 = df2.merge(
            levels.rename(
                columns={'Sector': f'{c}_sector', 'SectorLevel': f'{c}Level'}
            ),
            how='left',
            on=['SectorSourceName', f'{c}_sector'],
        ).drop_duplicates(subset=df.columns, keep='first')
    df2 = df2.assign(source_to_target_diff=df2['sourceLevel'] - df2['targetLevel'])

    # Data Reliability
    # If value maps to a different sector level than what the data set provides (maps down), then change all
    # 1/2 values to 3 because no longer direct representation (Non-verified data based on a calculation).
    # Leave values alone if maps up or no change.
    df2['DataReliability'] = np.where(
        (df2['source_to_target_diff'] < 0) & (df2['DataReliability'].isin([1, 2])),
        3,
        df2['DataReliability'],
    )

    # Data Collection
    # If sector level drops (coarser source → finer target), assign a score of 5
    # because no longer know if % of establishments/activities represented
    df2['DataCollection'] = np.where(
        df2['source_to_target_diff'] < 0, 5, df2['DataCollection']
    )

    return df2.drop(columns=['sourceLevel', 'targetLevel', 'source_to_target_diff'])

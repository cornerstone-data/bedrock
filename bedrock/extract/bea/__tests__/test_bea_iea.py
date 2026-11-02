import pandas as pd

from bedrock.extract.bea.BEA_IEA import bea_iea_parse


def test_suppressed_values_are_zero_with_the_note_not_imputed() -> None:
    """The FBA keeps only what BEA published; recovery is the FBS's (#904)."""
    raw = pd.DataFrame(
        {
            'TypeOfService': ['Health', 'Education'],
            'TradeDirection': ['Exports', 'Exports'],
            'DataValue': ['', '2,444'.replace(',', '')],
            'NoteRef': ['(D)', ''],
        }
    )
    out = bea_iea_parse(df_list=[raw], year='2021').set_index('ActivityProducedBy')

    assert out.loc['Health', 'FlowAmount'] == 0.0
    assert out.loc['Health', 'Suppressed'] == '(D)'
    assert out.loc['Education', 'FlowAmount'] == 2_444e6
    assert pd.isna(out.loc['Education', 'Suppressed'])

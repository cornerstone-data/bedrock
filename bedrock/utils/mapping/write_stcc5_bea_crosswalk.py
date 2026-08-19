"""Generate the STCC5 -> BEA 2017 commodity crosswalk for the rail margin (#611).

BEA allocates the rail transportation margin - 16.5% of ``TRANS`` - on **revenue
by product shipped by rail**, which ``STB_CRSR`` supplies as 371 five-digit
Standard Transportation Commodity Codes. STCC is a rail taxonomy that maps to
nothing else in the repo, so this concordance is the link, and BEA will not
publish theirs: the equivalent SAS-group map is *"internal to our database"*
(W. Nicolls, BEA, 2026-08-17), and there is no reason to think the rail one is
handled differently.

**The 2017 anchor cannot substitute for it.** The published ``Transportation``
column gives margin per BEA commodity summed over all five modes, so it says
what rail *plus* everything else delivered, never which STCC the rail part came
from. Nothing recovers the mapping from published data.

⚠️ **Thirteen codes are excluded rather than mapped**, because they name a
service class or an empty move rather than a commodity - trailer-on-flatcar and
NEC rate shipments, freight-forwarder traffic, returned empties, small packaged
freight, mixed shipments. Their revenue is dropped and the remaining shares
renormalised, which is the treatment BEA described for the equivalent bucket on
the truck side: *"We do not use the 'other' commodity from SAS Table 8 since we
have no information on what commodities it contains. Distributing it pro rata to
the other 10 would not change the result."* ⚠️ BEA said that about **truck**, so
applying it to rail is an inference, not something they told us.

Run this to regenerate ``Crosswalk_STCC5_to_BEA_2017.csv`` after editing the
mapping below. The CSV is the reviewable artefact; this file is where the
judgment lives.
"""

import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.transform.iot.nowcast_transport_margins import (
    published_transport_by_commodity,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_DESC as DESC

#: The concordance is authored against the margins anchor year.
ANCHOR_YEAR = 2017

CROSSWALK_PATH = 'bedrock/utils/mapping/Crosswalk_STCC5_to_BEA_2017.csv'

# --- codes with no commodity identity: dropped, then shares renormalised -----
EXCLUDE = {
    '46111': 'All freight rate shipments NEC or TOFC. Trailer-on-flatcar and NEC '
    'rate shipments carry no commodity identity. 14.0% of rail revenue alone.',
    '46211': 'Mixed shipments spanning two or more major STCC groups.',
    '47111': 'Small packaged freight shipments - a service class, not a commodity.',
    '44111': 'Freight forwarder traffic - the forwarder is shipper of record, so '
    'the underlying commodity is not reported.',
    '42211': 'Trailers, semi-trailers or containers returned empty.',
    '42311': 'Revenue movement of containers, carriers or devices - equipment.',
    '42312': 'Revenue moves of shipping devices - equipment, not freight.',
    '41111': 'Outfits or kits - unspecified mixed contents.',
    '41117': 'Military impedimenta - government materiel, no BEA goods commodity.',
    '41211': 'Special commodities not taken in regular freight service.',
    '20342': 'Published as "Unknown STCC Value".',
    '14922': 'Drinking water - not a BEA goods commodity. 0.4 $M.',
    '25515': 'Published with a blank commodity name.',
    '48105': 'Waste flammable liquids. The only BEA home is 562000 waste management '
    'services, which receives zero transportation margin in the published '
    '2017 table - hauling waste to disposal is not a margin-bearing goods '
    'purchase - so there is nowhere for it to go. 12.0 $M.',
    '48756': 'Waste stream, other regulated materials. Same as 48105. 6.7 $M.',
}

MAP = {
    # --- 01 farm products ---------------------------------------------------
    '01131': '1111B0',
    '01132': '1111B0',
    '01133': '1111B0',
    '01134': '1111B0',
    '01135': '1111B0',
    '01136': '1111B0',
    '01137': '1111B0',
    '01139': '1111B0',
    '01141': '1111A0',
    '01142': '1111A0',
    '01143': '1111A0',
    '01144': '1111A0',
    '01149': '1111A0',
    '01159': '111900',
    '01191': '111900',
    '01295': '111900',
    '01912': '111900',
    '01919': '111900',
    '01341': '111900',
    '01342': '111900',
    '01343': '111900',
    '01195': '111200',
    '01399': '111200',
    '01219': '111300',
    '01221': '111300',
    # --- 10 ores / 11 coal / 13 crude ---------------------------------------
    '10513': '2122A0',
    '10929': '2122A0',
    '11212': '212100',
    '13111': '211000',
    # --- 14 nonmetallic minerals --------------------------------------------
    '14219': '212310',
    '14412': '2123A0',
    '14413': '2123A0',
    '14511': '2123A0',
    '14519': '2123A0',
    '14711': '2123A0',
    '14715': '2123A0',
    '14716': '2123A0',
    '14917': '2123A0',
    '14918': '2123A0',
    '14919': '2123A0',
    # --- 19 ordnance ---------------------------------------------------------
    '19000': '33299A',
    # --- 20 food -------------------------------------------------------------
    '20129': '31161A',
    '20131': '31161A',
    '20139': '31161A',
    '20141': '31161A',
    '20143': '31161A',
    '20144': '31161A',
    '20149': '31161A',
    '20231': '311514',
    '20251': '311513',
    '20259': '311514',
    '20311': '311700',
    '20361': '311700',
    '20329': '311420',
    '20331': '311420',
    '20332': '311420',
    '20334': '311420',
    '20336': '311420',
    '20338': '311420',
    '20339': '311420',
    '20341': '311420',
    '20343': '311420',
    '20354': '311940',
    '20359': '311940',
    '20997': '311940',
    '20373': '311410',
    '20381': '311410',
    '20391': '311410',
    '20411': '311210',
    '20412': '311210',
    '20413': '311210',
    '20416': '311210',
    '20418': '311210',
    '20419': '311210',
    '20441': '311210',
    '20443': '311210',
    '20831': '311210',
    '20431': '311230',
    '20461': '311221',
    '20462': '311221',
    '20465': '311221',
    '20467': '311221',
    '20469': '311221',
    '20421': '311119',
    '20423': '311119',
    '20471': '311111',
    '20521': '3118A0',
    '20529': '3118A0',
    '20981': '3118A0',
    '20616': '311300',
    '20617': '311300',
    '20619': '311300',
    '20621': '311300',
    '20625': '311300',
    '20711': '311300',
    '20712': '311300',
    '20713': '311300',
    '20719': '311300',
    '20821': '312120',
    '20823': '312120',
    '20841': '312130',
    '20851': '312140',
    '20859': '312140',
    '20861': '312110',
    '20871': '311930',
    '20911': '311224',
    '20914': '311224',
    '20921': '311224',
    '20923': '311224',
    '20933': '311224',
    '20939': '311224',
    '20951': '311920',
    '20998': '311920',
    '20992': '311910',
    '20995': '311990',
    '20999': '311990',
    # --- 22 textile mill / 23 apparel ---------------------------------------
    '22119': '313200',
    '22211': '313200',
    '22999': '313200',
    '22799': '314110',
    '23111': '315000',
    '23311': '315000',
    '23891': '315000',
    '23929': '314120',
    '23999': '314900',
    # --- 24 wood -------------------------------------------------------------
    '24111': '113000',
    '24112': '113000',
    '24115': '113000',
    '24116': '113000',
    '24211': '321100',
    '24214': '321100',
    '24215': '321100',
    '24219': '321100',
    '24911': '321100',
    '24912': '321100',
    '24321': '321200',
    '24391': '321200',
    '24996': '321200',
    '24314': '321910',
    '24991': '3219A0',
    '24992': '3219A0',
    '24997': '3219A0',
    '24998': '3219A0',
    '24999': '3219A0',
    # --- 25 furniture ---------------------------------------------------------
    '25151': '337900',
    '25999': '337900',
    '25179': '337110',
    '25199': '337122',
    '25411': '337215',
    '25421': '337215',
    # --- 26 paper -------------------------------------------------------------
    '26111': '322110',
    '26112': '322110',
    '26211': '322120',
    '26212': '322120',
    '26213': '322120',
    '26214': '322120',
    '26217': '322120',
    '26218': '322120',
    '26219': '322120',
    '26311': '322130',
    '26431': '322220',
    '26471': '322291',
    '26499': '322299',
    '26614': '322299',
    '26511': '322210',
    '26515': '322210',
    '26543': '322210',
    '26551': '322210',
    # --- 27 printed matter ----------------------------------------------------
    '27211': '323110',
    '27311': '323110',
    '27419': '323110',
    '27812': '322230',
    # --- 28 chemicals ---------------------------------------------------------
    '28121': '325180',
    '28122': '325180',
    '28123': '325180',
    '28124': '325180',
    '28125': '325180',
    '28126': '325180',
    '28128': '325180',
    '28191': '325180',
    '28192': '325180',
    '28193': '325180',
    '28194': '325180',
    '28195': '325180',
    '28196': '325180',
    '28198': '325180',
    '28199': '325180',
    '28991': '325180',
    '28995': '325180',
    '28996': '325180',
    '28133': '325120',
    '28139': '325120',
    '28141': '325110',
    '28151': '325190',
    '28152': '325190',
    '28180': '325190',
    '28181': '325190',
    '28182': '325190',
    '28183': '325190',
    '28184': '325190',
    '28185': '325190',
    '28186': '325190',
    '28189': '325190',
    '28612': '325190',
    '28994': '325190',
    '28161': '325130',
    '28211': '325211',
    '28212': '3252A0',
    '28311': '325412',
    '28419': '325610',
    '28431': '325610',
    '28441': '325620',
    '28511': '325510',
    '28512': '325510',
    '28519': '325510',
    '28712': '325310',
    '28713': '325310',
    '28714': '325310',
    '28799': '325320',
    '28911': '325520',
    '28921': '3259A0',
    '28997': '3259A0',
    '28998': '3259A0',
    '28999': '3259A0',
    '28931': '325910',
    # --- 29 petroleum and coal products ---------------------------------------
    '29111': '324110',
    '29113': '324110',
    '29114': '324110',
    '29116': '324110',
    '29117': '324110',
    '29119': '324110',
    '29121': '324110',
    '29523': '324122',
    '29529': '324122',
    '29912': '324190',
    '29913': '324190',
    '29914': '324190',
    # --- 30 rubber and plastics -----------------------------------------------
    '30111': '326210',
    '30119': '326210',
    '30211': '316000',
    '30613': '326290',
    '30618': '326290',
    '30619': '326290',
    '30711': '326190',
    '30713': '326190',
    '30719': '326190',
    '30729': '326190',
    '30714': '326110',
    '30718': '326110',
    '30716': '326150',
    # --- 32 stone, clay and glass ---------------------------------------------
    '32113': '327200',
    '32211': '327200',
    '32219': '327200',
    '32291': '327200',
    '32292': '327200',
    '32299': '327200',
    '32411': '327310',
    '32511': '327100',
    '32531': '327100',
    '32611': '327100',
    '32719': '327390',
    '32741': '327400',
    '32752': '327400',
    '32754': '327400',
    '32952': '327999',
    '32959': '327992',
    # --- 33 primary metal ------------------------------------------------------
    '33111': '331110',
    '33121': '331110',
    '33122': '331110',
    '33123': '331110',
    '33124': '331110',
    '33125': '331110',
    '33128': '331110',
    '33134': '331110',
    '33126': '331200',
    '33127': '331200',
    '33155': '331200',
    '33211': '331510',
    '33311': '331410',
    '33321': '331410',
    '33331': '331410',
    '33341': '331313',
    '33521': '33131B',
    # --- 34 fabricated metal ---------------------------------------------------
    '34111': '332430',
    '34919': '332430',
    '34997': '332430',
    '34239': '332200',
    '34298': '332500',
    '34299': '332500',
    '34311': '332913',
    '34421': '332320',
    '34449': '332320',
    '34529': '332720',
    '34816': '332600',
    # --- 35 machinery -----------------------------------------------------------
    '35112': '333611',
    '35225': '333111',
    '35229': '333111',
    '35241': '333112',
    '35371': '333920',
    '35373': '333920',
    '35522': '33329A',
    '35531': '33329A',
    '35552': '33329A',
    '35641': '333413',
    '35741': '333318',
    '35853': '333415',
    '35857': '333415',
    '35891': '333318',
    '35999': '33399A',
    # --- 36 electrical -----------------------------------------------------------
    '36113': '334513',
    '36129': '335311',
    '36311': '335220',
    '36321': '335220',
    '36331': '335220',
    '36392': '335220',
    '36399': '335220',
    '36343': '335210',
    '36421': '335120',
    '36512': '334300',
    '36911': '335911',
    '36921': '335912',
    # --- 37 transportation equipment ----------------------------------------------
    '37111': '336111',
    '37112': ('336112', '336120'),
    '37119': '336111',
    '37142': '336111',
    '37143': '336370',
    '37144': '336310',
    '37147': '336211',
    '37149': '336390',
    '37422': '336500',
    '37426': '336500',
    '37511': '336991',
    '37512': '336991',
    '37999': '336999',
    # --- 38 instruments / 39 misc --------------------------------------------------
    '38411': '339112',
    '38421': '339113',
    '39411': '339930',
    '39499': '339920',
    '39921': '339990',
    '39995': '339990',
    '39998': '339990',
    '39999': '339990',
    # --- 40 waste and scrap ----------------------------------------------------------
    '40112': 'S00401',
    '40211': 'S00401',
    '40212': 'S00401',
    '40214': 'S00401',
    '40219': 'S00401',
    '40241': 'S00401',
    '40251': 'S00401',
    '40261': 'S00401',
    '40291': 'S00401',
    # --- 41 used goods ----------------------------------------------------------------
    '41112': 'S00402',
    '41114': 'S00402',
    '41115': 'S00402',
    '41116': 'S00402',
    '41118': 'S00402',
}

NOTES = {
    'S00401': 'BEA carries rail-hauled scrap as the Scrap commodity, not the metal it came from.',
    'S00402': 'Used and secondhand goods, which receives 23,868 $M of TRANS in 2017.',
    '113000': 'Logs and pulpwood are forestry output, not a sawmill product.',
    '311210': 'Flour milling and malt manufacturing are one BEA commodity, so malt lands here.',
    '211000': 'Crude oil and natural gas are one BEA detail commodity.',
    '336112': 'STCC 37112 is "motor trucks OR truck tractors", spanning BEA light trucks and heavy duty trucks, so it is split across both on published transport. Mapping it to 336120 alone allocates 1,745 $M against a 581 $M ceiling - a 3.0x over-allocation the bound check rejects.',
    '324110': 'Refinery output. Asphalt pitches and tars from petroleum are a refinery '
    'product; only shingles and coatings sit in 324122.',
}


def load_stcc_codes(year: int = ANCHOR_YEAR) -> pd.DataFrame:
    """Every STCC5 code in the CRSR for *year*, with its name and revenue."""
    fba = getFlowByActivity('STB_CRSR', year)
    items = fba[
        ~fba['ActivityProducedBy'].str.contains('TOTAL|Percent', case=False, na=False)
    ]
    revenue = items.groupby('ActivityProducedBy')['FlowAmount'].sum().div(1e6)
    names = items.drop_duplicates('ActivityProducedBy').set_index('ActivityProducedBy')[
        'FlowName'
    ]
    return (
        pd.DataFrame({'rev': revenue, 'name': names}).rename_axis('stcc5').reset_index()
    )


def main() -> None:
    src = load_stcc_codes()

    rows = []
    for _, r in src.sort_values('stcc5').iterrows():
        code = r['stcc5']
        if code in EXCLUDE:
            rows.append((code, r['name'], '', '', 'EXCLUDED: ' + EXCLUDE[code]))
            continue
        bea = MAP.get(code)
        if bea is None:
            rows.append((code, r['name'], '', '', 'UNMAPPED'))
            continue
        # a tuple is an STCC code that genuinely spans more than one BEA
        # commodity; the consumer splits its revenue across them
        for target in (bea,) if isinstance(bea, str) else bea:
            rows.append(
                (code, r['name'], target, DESC.get(target, '?'), NOTES.get(target, ''))
            )

    out = pd.DataFrame(
        rows,
        columns=[
            'stcc5',
            'stcc_description',
            'bea_2017_commodity',
            'bea_2017_description',
            'basis',
        ],
    )
    mapped = (out['bea_2017_commodity'] != '').sum()
    excluded = out['basis'].str.startswith('EXCLUDED').sum()
    unmapped = out[out['basis'] == 'UNMAPPED']
    print(
        f'{len(out)} codes: {mapped} mapped, {excluded} excluded, {len(unmapped)} unmapped'
    )

    rev = src.set_index('stcc5')['rev']
    for _, u in unmapped.iterrows():
        print(
            f'  UNMAPPED {u.stcc5} {rev.get(u.stcc5, 0):8,.1f}  {str(u.stcc_description)[:44]}'
        )

    bad = out[(out['bea_2017_commodity'] != '') & (out['bea_2017_description'] == '?')]
    if len(bad):
        print('BAD BEA CODES:', sorted(set(bad['bea_2017_commodity'])))

    # ⚠️ A target that receives no transportation margin in the published table
    # is a mapping error, not a small one: the rail allocation would put margin
    # on a commodity BEA gives none, which the bound check can only report as an
    # infinite share. 562000 waste management was caught this way.
    published = published_transport_by_commodity()
    targets = sorted(
        set(out.loc[out['bea_2017_commodity'] != '', 'bea_2017_commodity'])
    )
    dead = [c for c in targets if published.get(c, 0.0) <= 0]
    if dead:
        raise ValueError(
            f'{len(dead)} mapped commodities receive no transportation margin in '
            f'the published 2017 table, so rail margin sent there has nowhere to '
            f'sit: {dead}. Either map the STCC code elsewhere or exclude it.'
        )
    print(f'all {len(targets)} target commodities receive published TRANS')

    out.to_csv(CROSSWALK_PATH, index=False)
    print('written')


if __name__ == '__main__':
    main()

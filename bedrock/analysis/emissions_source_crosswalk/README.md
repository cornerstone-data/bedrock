# Mapping bedrock's GHG emissions to CEDA's emissions sources

## What this is for

CEDA breaks US emissions down by 123 emissions sources, such as "CO2 from
cement production", "CH4 from landfills" or "HFC-134a from foams". Each source
is one inventory category for one gas. Bedrock currently exports emissions by
gas only, so CEDA has to rebuild the source detail from its older v7 pipeline.

This folder works out how to tag every bedrock emission with the CEDA source
it belongs to. With that tagging, bedrock could export emissions by source
directly. For now it is an analysis: it reads the 2024 GHG flow-by-sector
dataset (FBS) and writes tables for review. The model pipeline does not use it
yet.

As of the 2024 run, 99.9% of emissions (by CO2e) land on exactly one CEDA
source.

## How an emission gets its CEDA source

### Step 1: know what each emission is

Normally the FBS remembers only which *group* of inventory lines an emission
came from. For example, one group called "UMD T_2_S1 direct" holds about 25
different inventory categories together, including cement, landfills, coal
mining and rice. That is too coarse to tell cement from landfills.

The FBS builder has an option to keep the original inventory line name. We
rebuild the FBS with it switched on. Each emission then carries three labels:
the group, the inventory line ("Cement Production") and the gas ("Carbon
dioxide"). We checked that switching it on changes no totals.

### Step 2: split the few lines that are still lumped together

A few emissions arrive as a single line even with the names kept, but CEDA
wants them in pieces:

- **Agricultural soils (N2O).** The inventory gives one national total. It is
  spread across farm sectors using an EPA table that separates fertilizer,
  cropland and pasture, but that detail is not carried into the FBS.
- **Non-energy use of fuels (CO2).** This one works the same way, with an EPA
  table that separates coal, petroleum, natural gas and lubricants.
- **General aviation (CO2).** The method file renames both general-aviation
  jet fuel and aviation gasoline to the same name, "General Aviation
  Aircraft".
- **Residential and commercial CH4 and N2O.** The inventory gives natural gas
  and petroleum as one "residential and commercial" line each.
- **HFCs and PFCs with no species named.** Part of each refrigerant, foam and
  vehicle air-conditioning line comes only as a CO2e total.

For soils and non-energy use, we build those EPA tables separately and split
each sector's emissions by that sector's own mix. With the current method this
already adds up to the EPA table's national mix, and the soils totals match
CEDA's to within 1%. As a safeguard, the split is then adjusted until two
things hold at once: every sector keeps its bedrock total, and the parts add
up to the EPA table's national shares. Today that adjustment changes nothing.
It matters if bedrock ever spreads a line over sectors with different weights
from the EPA table. An earlier build did that for non-energy use, and without
the adjustment natural gas came out at 27.9% instead of 11.1%.

For general aviation, both fuels go to the same air transportation sector, so
we split by the two fuels' national totals (3.4% aviation gasoline, 96.6% jet
fuel).

For residential and commercial, the FBS already puts the household part on
the household sector and the rest on commercial sectors, so the sector tells
them apart directly.

For the unnamed HFCs and PFCs, we split each CO2e amount in proportion to the
named species reported beside it, for the same activity and sector, and
convert to kilograms of each gas. The method converts these tables with AR5
global warming potentials, so we assume the amounts are AR5 CO2e. The split
keeps the AR5 total exactly. Re-valued at AR6, the total rises by about
2.8 Mt.

### Step 3: look each line up in the crosswalk table

`fbs_to_ceda_emissions_source.csv` is a hand-written lookup table. Each row
says "emissions with this group, inventory line and gas belong to this CEDA
source". A `*` means "any". For example, every gas from natural gas systems
can be covered by one row. Where two rows could apply, the more specific one
wins.

Most rows follow one of three simple patterns:

- **Same name.** "Cement Production" goes to CO2 cement production, and
  "Landfills" to CH4 landfills.
- **Fuel and end use.** Industrial coal burning goes to CO2 industrial coal.
  Gasoline in cars and trucks goes to CO2 transportation motor gasoline.
- **HFC species and use.** Refrigerant HFC-125 goes to HFC-125 substitution of
  ozone-depleting substances (others). Vehicle air conditioning goes to the
  "transport" version.

Every row also has a status. "Mapped" means it has one CEDA source.
"Bundled" means it still spans several sources. "No CEDA source" means CEDA
has no source for it. "No species" means an HFC amount with no gas named.
Each row can carry a note explaining the choice.

## Where we had to make a call

These choices are reasonable but not certain, and CEDA's owners should confirm
them. In the table they are marked "check:" or explained in the note.

- **Post-meter natural gas leaks** (14.7 Mt CO2e) are counted under natural
  gas systems. CEDA's inventory may be old enough not to include them.
- **Indirect soil N2O** from cropland (nitrogen that washes or blows off
  fields) is counted under cropland fertilizer. We don't know how CEDA treats
  indirect N2O.
- **Unnamed HFCs and PFCs** are assumed to have the same species mix as the
  named ones beside them, and to be in AR5 CO2e.
- **Non-energy use by fuel** follows each sector's own EPA mix. A sector the
  EPA table doesn't cover would borrow the mix of related sectors (the
  longest shared NAICS prefix); none needs to with the current method.
- **Small folds into the nearest category:**
  - anaerobic digestion into composting
  - LNG liquefaction and pipeline fuel into transportation natural gas
  - wood burning into residential
  - industrial petroleum CH4 and N2O into "industrial fuel oil"
  - natural gas for hydrogen production and at gas plants into industrial
    natural gas
- **Gases are never mixed.** Each CEDA source belongs to a single gas. If CEDA
  has no source for a gas in a category, the emission is left unassigned
  rather than moved to another gas. Examples are CO2 from coal mining and
  traces of HFC-23 in refrigerants.

## What is still unassigned

| What | Mt CO2e | Why |
|---|---|---|
| Fluorochemical production, CO2 from coal mining, SF6 in other products | 7.3 | CEDA has no source for these. Fluorochemical production is a newer inventory category that CEDA's older inventory doesn't have. |
| HFCs and PFCs with no species named and none reported beside them | 0.7 | fluorochemical production, military uses and electronics, where nothing tells us the gas |

Ten CEDA sources get nothing from bedrock. Most are outside what bedrock
covers: international bunker fuels, land use and forestry, peatlands, US
territories and biomass. Two still need checking: CO2 consumption and CH4
from petrochemicals.

## How to review it

**Rebuild the outputs.** Run these from the repo root. `$F` is the FBS that
`trial_retained_activity_fbs.py` builds with the inventory line names kept.

```
python -m bedrock.analysis.emissions_source_crosswalk.build_nested_split $F
python -m bedrock.analysis.emissions_source_crosswalk.apply_crosswalk $F \
    --shares bedrock/analysis/emissions_source_crosswalk/output/nested_split_GHG_national_Cornerstone_2024_shares.csv \
    --speciate-hfcs --ceda-root ../ceda
```

The scripts stop with an error if:

- a CEDA source in the table doesn't exist,
- two rows clash,
- or a split changes any total.

**Then read the outputs, in this order:**

1. **`output/*_by_ceda_source.csv`.** There is one row per CEDA source,
   largest first, listing the bedrock lines that feed it. For each source,
   ask whether those lines belong there. This is the quickest way to spot a
   wrong assignment.
2. **The crosswalk table's notes**, especially the "check:" ones.
3. **`output/*_crosswalked.csv`.** This has every bedrock line with its
   status. Sort by size and read anything not "mapped".
4. **The split shares file.** Each split says what it is based on. Anything
   other than "sector" used a fallback and is worth a look.

**Compare with CEDA's own numbers.** `compare_with_ceda.py` puts each CEDA
source's bedrock total beside CEDA v7.1 and v8.1, all in AR5 CO2e. CEDA's
figures are older inventory years, so differences of a few percent are real
change between years. A source at zero on one side, or several times the
other, is flagged.

```
python -m bedrock.analysis.emissions_source_crosswalk.compare_with_ceda $F \
    --shares bedrock/analysis/emissions_source_crosswalk/output/nested_split_GHG_national_Cornerstone_2024_shares.csv \
    --ceda-root ../ceda
```

Read the flags together with the "bedrock all sectors" column. CEDA puts
nothing on households, while bedrock puts household cars, home heating and
home air conditioning there. A source that looks low for industries can match
once households are added.

## Files

| File | What it does |
|---|---|
| `fbs_to_ceda_emissions_source.csv` | the crosswalk table |
| `apply_crosswalk.py` | splits the unnamed HFCs by species, tags each FBS line with its CEDA source, and writes the review tables |
| `build_nested_split.py` | works out the soils, non-energy use, aviation and residential/commercial splits |
| `nested_split_GHG_national_Cornerstone_2024.yaml` | builds the EPA tables behind the soils and non-energy use splits |
| `trial_retained_activity_fbs.py` | rebuilds the FBS with inventory line names kept, and checks totals |
| `compare_with_ceda.py` | compares each CEDA source's total with CEDA's own v7.1 and v8.1 figures |
| `inventory_fbs_sources.py` | the first rough pass, matching by name; superseded by the table |

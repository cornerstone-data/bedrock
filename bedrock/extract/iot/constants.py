import typing as ta

from bedrock.utils.io.gcp_paths import gcs_extract_input_path

GCS_USA_MAKE_USE_DIR = gcs_extract_input_path("USA_AllTables_MakeUse")
GCS_USA_SUP_DIR = gcs_extract_input_path("USA_AllTablesSUP")
# Shared directory for BEA's NIPA-final-demand-to-IOT "bridge" tables (PCE Bridge,
# PEQ Bridge, ...) - one BEA-published Excel workbook per final-demand category.
GCS_BEA_NIPA_IOT_BRIDGES_DIR = gcs_extract_input_path("BEA_NIPA_IOT_Bridges")

GCS_GDP_DIR = gcs_extract_input_path("BEA_PriceIndex")
GCS_GDP_DETAIL_TABLES = ta.Literal["UGO304-A", "UGO305-A"]

PRICE_INDEX_DETAIL_NAME_TO_BEA_2017_INDUSTRY_MAPPING: ta.Dict[str, ta.List[str]] = {
    # adopeted from USEEIO
    "Oilseed farming": ["1111A0"],
    "Grain farming": ["1111B0"],
    "Vegetable and melon farming": ["111200"],
    "Fruit and tree nut farming": ["111300"],
    "Greenhouse, nursery, and floriculture production": ["111400"],
    "Other crop farming": ["111900"],
    "Dairy cattle and milk production": ["112120"],
    "Beef cattle ranching and farming, including feedlots and dual-purpose ranching and farming": [
        "1121A0"
    ],
    "Poultry and egg production": ["112300"],
    "Animal production, except cattle and poultry and eggs": ["112A00"],
    "Forestry and logging": ["113000"],
    "Fishing, hunting and trapping": ["114000"],
    "Support activities for agriculture and forestry": ["115000"],
    "Oil and gas extraction": ["211000"],
    "Coal mining": ["212100"],
    "Copper, nickel, lead, and zinc mining": ["212230"],
    "Iron, gold, silver, and other metal ore mining": ["2122A0"],
    "Stone mining and quarrying": ["212310"],
    "Other nonmetallic mineral mining and quarrying": ["2123A0"],
    "Drilling oil and gas wells": ["213111"],
    "Other support activities for mining": ["21311A"],
    "Electric power generation, transmission, and distribution": ["221100"],
    "Natural gas distribution": ["221200"],
    "Water, sewage and other systems": ["221300"],
    "Nonresidential maintenance and repair": ["230301"],
    "Residential maintenance and repair": ["230302"],
    "Health care structures": ["233210"],
    "Manufacturing structures": ["233230"],
    "Power and communication structures": ["233240"],
    "Educational and vocational structures": ["233262"],
    "Office and commercial structures": ["2332A0"],
    "Transportation structures and highways and streets": ["2332C0"],
    "Other nonresidential structures": ["2332D0"],
    "Single-family residential structures": ["233411"],
    "Multifamily residential structures": ["233412"],
    "Other residential structures": ["2334A0"],
    "Dog and cat food manufacturing": ["311111"],
    "Other animal food manufacturing": ["311119"],
    "Flour milling and malt manufacturing": ["311210"],
    "Wet corn milling": ["311221"],
    "Soybean and other oilseed processing": ["311224"],
    "Fats and oils refining and blending": ["311225"],
    "Breakfast cereal manufacturing": ["311230"],
    "Sugar and confectionery product manufacturing": ["311300"],
    "Frozen food manufacturing": ["311410"],
    "Fruit and vegetable canning, pickling, and drying": ["311420"],
    "Cheese manufacturing": ["311513"],
    "Dry, condensed, and evaporated dairy product manufacturing": ["311514"],
    "Fluid milk and butter manufacturing": ["31151A"],
    "Ice cream and frozen dessert manufacturing": ["311520"],
    "Poultry processing": ["311615"],
    "Animal (except poultry) slaughtering, rendering, and processing": ["31161A"],
    "Seafood product preparation and packaging": ["311700"],
    "Bread and bakery product manufacturing": ["311810"],
    "Cookie, cracker, pasta, and tortilla manufacturing": ["3118A0"],
    "Snack food manufacturing": ["311910"],
    "Coffee and tea manufacturing": ["311920"],
    "Flavoring syrup and concentrate manufacturing": ["311930"],
    "Seasoning and dressing manufacturing": ["311940"],
    "All other food manufacturing": ["311990"],
    "Soft drink and ice manufacturing": ["312110"],
    "Breweries": ["312120"],
    "Wineries": ["312130"],
    "Distilleries": ["312140"],
    "Tobacco product manufacturing": ["312200"],
    "Fiber, yarn, and thread mills": ["313100"],
    "Fabric mills": ["313200"],
    "Textile and fabric finishing and fabric coating mills": ["313300"],
    "Carpet and rug mills": ["314110"],
    "Curtain and linen mills": ["314120"],
    "Other textile product mills": ["314900"],
    "Apparel manufacturing": ["315000"],
    "Leather and allied product manufacturing": ["316000"],
    "Sawmills and wood preservation": ["321100"],
    "Veneer, plywood, and engineered wood product manufacturing": ["321200"],
    "Millwork": ["321910"],
    "All other wood product manufacturing": ["3219A0"],
    "Pulp mills": ["322110"],
    "Paper mills": ["322120"],
    "Paperboard mills": ["322130"],
    "Paperboard container manufacturing": ["322210"],
    "Paper Bag and Coated and Treated Paper Manufacturing": ["322220"],
    "Paper bag and coated and treated paper manufacturing": ["322220"],
    "Stationery product manufacturing": ["322230"],
    "Sanitary paper product manufacturing": ["322291"],
    "All other converted paper product manufacturing": ["322299"],
    "Printing": ["323110"],
    "Support activities for printing": ["323120"],
    "Petroleum refineries": ["324110"],
    "Asphalt paving mixture and block manufacturing": ["324121"],
    "Asphalt shingle and coating materials manufacturing": ["324122"],
    "Other petroleum and coal products manufacturing": ["324190"],
    "Petrochemical manufacturing": ["325110"],
    "Industrial gas manufacturing": ["325120"],
    "Synthetic dye and pigment manufacturing": ["325130"],
    "Other Basic Inorganic Chemical Manufacturing": ["325180"],
    "Other basic inorganic chemical manufacturing": ["325180"],
    "Other basic organic chemical manufacturing": ["325190"],
    "Plastics material and resin manufacturing": ["325211"],
    "Synthetic rubber and artificial and synthetic fibers and filaments manufacturing": [
        "3252A0"
    ],
    "Fertilizer manufacturing": ["325310"],
    "Pesticide and other agricultural chemical manufacturing": ["325320"],
    "Medicinal and botanical manufacturing": ["325411"],
    "Pharmaceutical preparation manufacturing": ["325412"],
    "In-vitro diagnostic substance manufacturing": ["325413"],
    "Biological product (except diagnostic) manufacturing": ["325414"],
    "Paint and coating manufacturing": ["325510"],
    "Adhesive manufacturing": ["325520"],
    "Soap and cleaning compound manufacturing": ["325610"],
    "Toilet preparation manufacturing": ["325620"],
    "Printing ink manufacturing": ["325910"],
    "All other chemical product and preparation manufacturing": ["3259A0"],
    "Plastics packaging materials and unlaminated film and sheet manufacturing": [
        "326110"
    ],
    "Plastics pipe, pipe fitting, and unlaminated profile shape manufacturing": [
        "326120"
    ],
    "Laminated plastics plate, sheet (except packaging), and shape manufacturing": [
        "326130"
    ],
    "Polystyrene foam product manufacturing": ["326140"],
    "Urethane and other foam product (except polystyrene) manufacturing": ["326150"],
    "Plastics bottle manufacturing": ["326160"],
    "Other plastics product manufacturing": ["326190"],
    "Tire manufacturing": ["326210"],
    "Rubber and plastics hoses and belting manufacturing": ["326220"],
    "Other rubber product manufacturing": ["326290"],
    "Clay product and refractory manufacturing": ["327100"],
    "Glass and glass product manufacturing": ["327200"],
    "Cement manufacturing": ["327310"],
    "Ready-mix concrete manufacturing": ["327320"],
    "Concrete pipe, brick, and block manufacturing": ["327330"],
    "Other concrete product manufacturing": ["327390"],
    "Lime and gypsum product manufacturing": ["327400"],
    "Abrasive product manufacturing": ["327910"],
    "Cut stone and stone product manufacturing": ["327991"],
    "Ground or treated mineral and earth manufacturing": ["327992"],
    "Mineral wool manufacturing": ["327993"],
    "Miscellaneous nonmetallic mineral products": ["327999"],
    "Iron and steel mills and ferroalloy manufacturing": ["331110"],
    "Steel product manufacturing from purchased steel": ["331200"],
    "Alumina refining and primary aluminum production": ["331313"],
    "Secondary smelting and alloying of aluminum": ["331314"],
    "Aluminum product manufacturing from purchased aluminum": ["33131B"],
    "Nonferrous Metal (except Aluminum) Smelting and Refining": ["331410"],
    "Nonferrous metal (except aluminum) smelting and refining": ["331410"],
    "Copper rolling, drawing, extruding and alloying": ["331420"],
    "Nonferrous metal (except copper and aluminum) rolling, drawing, extruding and alloying": [
        "331490"
    ],
    "Ferrous metal foundries": ["331510"],
    "Nonferrous metal foundries": ["331520"],
    "Custom roll forming": ["332114"],
    "Metal crown, closure, and other metal stamping (except automotive)": ["332119"],
    "All other forging, stamping, and sintering": ["33211A"],
    "Cutlery and handtool manufacturing": ["332200"],
    "Plate work and fabricated structural product manufacturing": ["332310"],
    "Ornamental and architectural metal products manufacturing": ["332320"],
    "Power boiler and heat exchanger manufacturing": ["332410"],
    "Metal tank (heavy gauge) manufacturing": ["332420"],
    "Metal can, box, and other metal container (light gauge) manufacturing": ["332430"],
    "Hardware manufacturing": ["332500"],
    "Spring and wire product manufacturing": ["332600"],
    "Machine shops": ["332710"],
    "Turned product and screw, nut, and bolt manufacturing": ["332720"],
    "Coating, engraving, heat treating and allied activities": ["332800"],
    "Plumbing fixture fitting and trim manufacturing": ["332913"],
    "Valve and fittings other than plumbing": ["33291A"],
    "Ball and roller bearing manufacturing": ["332991"],
    "Fabricated pipe and pipe fitting manufacturing": ["332996"],
    "Other fabricated metal manufacturing": ["332999"],
    "Ammunition, arms, ordnance, and accessories manufacturing": ["33299A"],
    "Farm machinery and equipment manufacturing": ["333111"],
    "Lawn and garden equipment manufacturing": ["333112"],
    "Construction machinery manufacturing": ["333120"],
    "Mining and oil and gas field machinery manufacturing": ["333130"],
    "Semiconductor machinery manufacturing": ["333242"],
    "Other industrial machinery manufacturing": ["33329A"],
    "Optical instrument and lens manufacturing": ["333314"],
    "Photographic and photocopying equipment manufacturing": ["333316"],
    "Other commercial and service industry machinery manufacturing": ["333318"],
    "Industrial and commercial fan and blower and air purification equipment manufacturing": [
        "333413"
    ],
    "Heating equipment (except warm air furnaces) manufacturing": ["333414"],
    "Air conditioning, refrigeration, and warm air heating equipment manufacturing": [
        "333415"
    ],
    "Industrial mold manufacturing": ["333511"],
    "Special tool, die, jig, and fixture manufacturing": ["333514"],
    "Machine tool manufacturing": ["333517"],
    "Cutting and machine tool accessory, rolling mill, and other metalworking machinery manufacturing": [
        "33351B"
    ],
    "Turbine and turbine generator set units manufacturing": ["333611"],
    "Speed changer, industrial high-speed drive, and gear manufacturing": ["333612"],
    "Mechanical power transmission equipment manufacturing": ["333613"],
    "Other engine equipment manufacturing": ["333618"],
    "Air and gas compressor manufacturing": ["333912"],
    "Pump and pumping equipment manufacturing": ["33391A"],
    "Material handling equipment manufacturing": ["333920"],
    "Power-driven handtool manufacturing": ["333991"],
    "Packaging machinery manufacturing": ["333993"],
    "Industrial process furnace and oven manufacturing": ["333994"],
    "Other general purpose machinery manufacturing": ["33399A"],
    "Fluid power process machinery": ["33399B"],
    "Electronic computer manufacturing": ["334111"],
    "Computer storage device manufacturing": ["334112"],
    "Computer terminals and other computer peripheral equipment manufacturing": [
        "334118"
    ],
    "Telephone apparatus manufacturing": ["334210"],
    "Broadcast and wireless communications equipment": ["334220"],
    "Other communications equipment manufacturing": ["334290"],
    "Audio and video equipment manufacturing": ["334300"],
    "Semiconductor and related device manufacturing": ["334413"],
    "Printed circuit assembly (electronic assembly) manufacturing": ["334418"],
    "Other electronic component manufacturing": ["33441A"],
    "Electromedical and electrotherapeutic apparatus manufacturing": ["334510"],
    "Search, detection, and navigation instruments manufacturing": ["334511"],
    "Automatic environmental control manufacturing": ["334512"],
    "Industrial process variable instruments manufacturing": ["334513"],
    "Totalizing fluid meter and counting device manufacturing": ["334514"],
    "Electricity and signal testing instruments manufacturing": ["334515"],
    "Analytical laboratory instrument manufacturing": ["334516"],
    "Irradiation apparatus manufacturing": ["334517"],
    "Watch, clock, and other measuring and controlling device manufacturing": [
        "33451A"
    ],
    "Manufacturing and reproducing magnetic and optical media": ["334610"],
    "Electric lamp bulb and part manufacturing": ["335110"],
    "Lighting fixture manufacturing": ["335120"],
    "Small electrical appliance manufacturing": ["335210"],
    "Household cooking appliance manufacturing": ["335221"],
    "Household refrigerator and home freezer manufacturing": ["335222"],
    "Household laundry equipment manufacturing": ["335224"],
    "Other major household appliance manufacturing": ["335228"],
    "Power, distribution, and specialty transformer manufacturing": ["335311"],
    "Motor and generator manufacturing": ["335312"],
    "Switchgear and switchboard apparatus manufacturing": ["335313"],
    "Relay and industrial control manufacturing": ["335314"],
    "Storage battery manufacturing": ["335911"],
    "Primary battery manufacturing": ["335912"],
    "Communication and energy wire and cable manufacturing": ["335920"],
    "Wiring device manufacturing": ["335930"],
    "Carbon and graphite product manufacturing": ["335991"],
    "All other miscellaneous electrical equipment and component manufacturing": [
        "335999"
    ],
    "Automobile manufacturing": ["336111"],
    "Light truck and utility vehicle manufacturing": ["336112"],
    "Heavy duty truck manufacturing": ["336120"],
    "Motor vehicle body manufacturing": ["336211"],
    "Truck trailer manufacturing": ["336212"],
    "Motor home manufacturing": ["336213"],
    "Travel trailer and camper manufacturing": ["336214"],
    "Motor vehicle gasoline engine and engine parts manufacturing": ["336310"],
    "Motor vehicle electrical and electronic equipment manufacturing": ["336320"],
    "Motor vehicle transmission and power train parts manufacturing": ["336350"],
    "Motor vehicle seating and interior trim manufacturing": ["336360"],
    "Motor vehicle metal stamping": ["336370"],
    "Other Motor Vehicle Parts Manufacturing": ["336390"],
    "Other motor vehicle parts manufacturing": ["336390"],
    "Motor vehicle steering, suspension component (except spring), and brake systems manufacturing": [
        "3363A0"
    ],
    "Aircraft manufacturing": ["336411"],
    "Aircraft engine and engine parts manufacturing": ["336412"],
    "Other aircraft parts and auxiliary equipment manufacturing": ["336413"],
    "Guided missile and space vehicle manufacturing": ["336414"],
    "Propulsion units and parts for space vehicles and guided missiles": ["33641A"],
    "Railroad rolling stock manufacturing": ["336500"],
    "Ship building and repairing": ["336611"],
    "Boat building": ["336612"],
    "Motorcycle, bicycle, and parts manufacturing": ["336991"],
    "Military armored vehicle, tank, and tank component manufacturing": ["336992"],
    "All other transportation equipment manufacturing": ["336999"],
    "Wood kitchen cabinet and countertop manufacturing": ["337110"],
    "Upholstered household furniture manufacturing": ["337121"],
    "Nonupholstered wood household furniture manufacturing": ["337122"],
    "Institutional furniture manufacturing": ["337127"],
    "Other household nonupholstered furniture": ["33712N"],
    "Showcase, partition, shelving, and locker manufacturing": ["337215"],
    "Office furniture and custom architectural woodwork and millwork manufacturing": [
        "33721A"
    ],
    "Other furniture related product manufacturing": ["337900"],
    "Surgical and medical instrument manufacturing": ["339112"],
    "Surgical appliance and supplies manufacturing": ["339113"],
    "Dental equipment and supplies manufacturing": ["339114"],
    "Ophthalmic goods manufacturing": ["339115"],
    "Dental laboratories": ["339116"],
    "Jewelry and silverware manufacturing": ["339910"],
    "Sporting and athletic goods manufacturing": ["339920"],
    "Doll, toy, and game manufacturing": ["339930"],
    "Office supplies (except paper) manufacturing": ["339940"],
    "Sign manufacturing": ["339950"],
    "All other miscellaneous manufacturing": ["339990"],
    "Customs duties": ["4200ID"],
    "Motor vehicle and motor vehicle parts and supplies": ["423100"],
    "Professional and commercial equipment and supplies": ["423400"],
    "Household appliances and electrical and electronic goods": ["423600"],
    "Machinery, equipment, and supplies": ["423800"],
    "Other durable goods merchant wholesalers": ["423A00"],
    "Drugs and druggists' sundries": ["424200"],
    "Grocery and related product wholesalers": ["424400"],
    "Petroleum and petroleum products": ["424700"],
    "Other nondurable goods merchant wholesalers": ["424A00"],
    "Wholesale electronic markets and agents and brokers": ["425000"],
    "Motor vehicle and parts dealers": ["441000"],
    "Building material and garden equipment and supplies dealers": ["444000"],
    "Food and beverage stores": ["445000"],
    "Health and personal care stores": ["446000"],
    "Gasoline stations": ["447000"],
    "Clothing and clothing accessories stores": ["448000"],
    "General merchandise stores": ["452000"],
    "Nonstore retailers": ["454000"],
    "Air transportation": ["481000"],
    "Rail transportation": ["482000"],
    "Water transportation": ["483000"],
    "Truck transportation": ["484000"],
    "Transit and ground passenger transportation": ["485000"],
    "Pipeline transportation": ["486000"],
    "Scenic and sightseeing transportation and support activities for transportation": [
        "48A000"
    ],
    "Postal service": ["491000"],
    "Couriers and messengers": ["492000"],
    "Warehousing and storage": ["493000"],
    "All other retail": ["4B0000"],
    "Newspaper publishers": ["511110"],
    "Periodical Publishers": ["511120"],
    "Book publishers": ["511130"],
    "Directory, mailing list, and other publishers": ["5111A0"],
    "Software publishers": ["511200"],
    "Motion picture and video industries": ["512100"],
    "Sound recording industries": ["512200"],
    "Radio and television broadcasting": ["515100"],
    "Cable and other subscription programming": ["515200"],
    "Wired telecommunications carriers": ["517110"],
    "Wireless telecommunications carriers (except satellite)": ["517210"],
    "Satellite, telecommunications resellers, and all other telecommunications": [
        "517A00"
    ],
    "Data processing, hosting, and related services": ["518200"],
    "Internet publishing and broadcasting and Web search portals": ["519130"],
    "News syndicates, libraries, archives and all other information services": [
        "5191A0"
    ],
    "Nondepository credit intermediation and related activities": ["522A00"],
    "Other financial investment activities": ["523900"],
    "Securities and commodity contracts intermediation and brokerage": ["523A00"],
    "Direct life insurance carriers": ["524113"],
    "Insurance carriers, except direct life": ["5241XX"],
    "Insurance carriers, except direct life insurance": ["5241XX"],
    "Insurance agencies, brokerages, and related activities": ["524200"],
    "Funds, trusts, and other financial vehicles": ["525000"],
    "Monetary authorities and depository credit intermediation": ["52A000"],
    "Owner-occupied housing": ["531HSO"],
    "Tenant-occupied housing": ["531HST"],
    "Other real estate": ["531ORE"],
    "Automotive equipment rental and leasing": ["532100"],
    "Commercial and industrial machinery and equipment rental and leasing": ["532400"],
    "General and consumer goods rental": ["532A00"],
    "Lessors of nonfinancial intangible assets": ["533000"],
    "Legal services": ["541100"],
    "Accounting, tax preparation, bookkeeping, and payroll services": ["541200"],
    "Architectural, engineering, and related services": ["541300"],
    "Specialized design services": ["541400"],
    "Custom computer programming services": ["541511"],
    "Computer systems design services": ["541512"],
    "Other computer related services, including facilities management": ["54151A"],
    "Management consulting services": ["541610"],
    "Environmental and other technical consulting services": ["5416A0"],
    "Scientific research and development services": ["541700"],
    "Advertising, public relations, and related services": ["541800"],
    "Photographic services": ["541920"],
    "Veterinary services": ["541940"],
    "All other miscellaneous professional, scientific, and technical services": [
        "5419A0"
    ],
    "Management of companies and enterprises": ["550000"],
    "Office administrative services": ["561100"],
    "Facilities support services": ["561200"],
    "Employment services": ["561300"],
    "Business support services": ["561400"],
    "Travel arrangement and reservation services": ["561500"],
    "Investigation and security services": ["561600"],
    "Services to buildings and dwellings": ["561700"],
    "Other support services": ["561900"],
    "Waste management and remediation services": ["562000"],
    "Elementary and secondary schools": ["611100"],
    "Junior colleges, colleges, universities, and professional schools": ["611A00"],
    "Other educational services": ["611B00"],
    "Offices of physicians": ["621100"],
    "Offices of dentists": ["621200"],
    "Offices of other health practitioners": ["621300"],
    "Outpatient care centers": ["621400"],
    "Medical and diagnostic laboratories": ["621500"],
    "Home health care services": ["621600"],
    "Other ambulatory health care services": ["621900"],
    "Hospitals": ["622000"],
    "Nursing and community care facilities": ["623A00"],
    "Residential mental health, substance abuse, and other residential care facilities": [
        "623B00"
    ],
    "Individual and family services": ["624100"],
    "Child day care services": ["624400"],
    "Community food, housing, and other relief services, including rehabilitation services": [
        "624A00"
    ],
    "Performing arts companies": ["711100"],
    "Spectator sports": ["711200"],
    "Independent artists, writers, and performers": ["711500"],
    "Promoters of performing arts and sports and agents for public figures": ["711A00"],
    "Museums, historical sites, zoos, and parks": ["712000"],
    "Amusement parks and arcades": ["713100"],
    "Gambling industries (except casino hotels)": ["713200"],
    "Other amusement and recreation industries": ["713900"],
    "Accommodation": ["721000"],
    "Full-service restaurants": ["722110"],
    "Limited-service restaurants": ["722211"],
    "All other food and drinking places": ["722A00"],
    "Automotive repair and maintenance": ["811100"],
    "Electronic and precision equipment repair and maintenance": ["811200"],
    "Commercial and industrial machinery and equipment repair and maintenance": [
        "811300"
    ],
    "Personal and household goods repair and maintenance": ["811400"],
    "Personal care services": ["812100"],
    "Death care services": ["812200"],
    "Dry-cleaning and laundry services": ["812300"],
    "Other personal services": ["812900"],
    "Religious organizations": ["813100"],
    "Grantmaking, giving, and social advocacy organizations": ["813A00"],
    "Civic, social, professional, and similar organizations": ["813B00"],
    "Private households": ["814000"],
    "State and local government educational services": ["GSLGE"],
    "State and local government hospitals and health services": ["GSLGH"],
    "State and local government other services": ["GSLGO"],
    "Federal electric utilities": ["S00101"],
    "Other federal government enterprises": ["S00102"],
    "State and local government passenger transit": ["S00201"],
    "State and local government electric utilities": ["S00202"],
    "Other state and local government enterprises": ["S00203"],
    "Federal general government (defense)": ["S00500"],
    "Federal general government (nondefense)": ["S00600"],
    "Geothermal electric power generation": ["221100"],
    "Biomass electric power generation": ["221100"],
    "Other electric power generation": ["221100"],
    "Electric bulk power transmission and control": ["221100"],
    "Furniture and home furnishings stores": ["4B0000"],
    "Electronics and appliance stores": ["4B0000"],
    "Sporting goods, hobby, book, and music stores": ["4B0000"],
    "Miscellaneous store retailers": ["4B0000"],
    "Hydroelectric power generation": ["221100"],
    "Fossil fuel electric power generation": ["221100"],
    "Nuclear electric power generation": ["221100"],
    "Solar electric power generation": ["221100"],
    "Wind electric power generation": ["221100"],
    "Electric power distribution": ["221100"],
    "Measuring, dispensing, and other pumping equipment manufacturing": ["333914"],
    "Major household appliance manufacturing": ["335220"],
    "Tobacco manufacturing": ["312200"],
    "State and local government (educational services)": ["GSLGE"],
    "State and local government (hospitals and health services)": ["GSLGH"],
    "State and local government (other services)": ["GSLGO"],
}

PRICE_INDEX_SUMMARY_LINE_NUMBER_TO_BEA_2017_SUMMARY_MAPPING: ta.Dict[str, str] = {
    # manually created to replace /Crosswalk_SummaryGDPIndustrytoIO2012Schema.csv
    # which was adopeted from USEEIO
    "LINE_NUMBER_1": "",
    "LINE_NUMBER_2": "",
    "LINE_NUMBER_3": "",
    "LINE_NUMBER_4": "111CA",
    "LINE_NUMBER_5": "113FF",
    "LINE_NUMBER_6": "",
    "LINE_NUMBER_7": "211",
    "LINE_NUMBER_8": "212",
    "LINE_NUMBER_9": "213",
    "LINE_NUMBER_10": "22",
    "LINE_NUMBER_11": "23",
    "LINE_NUMBER_12": "",
    "LINE_NUMBER_13": "",
    "LINE_NUMBER_14": "321",
    "LINE_NUMBER_15": "327",
    "LINE_NUMBER_16": "331",
    "LINE_NUMBER_17": "332",
    "LINE_NUMBER_18": "333",
    "LINE_NUMBER_19": "334",
    "LINE_NUMBER_20": "335",
    "LINE_NUMBER_21": "3361MV",
    "LINE_NUMBER_22": "3364OT",
    "LINE_NUMBER_23": "337",
    "LINE_NUMBER_24": "339",
    "LINE_NUMBER_25": "",
    "LINE_NUMBER_26": "311FT",
    "LINE_NUMBER_27": "313TT",
    "LINE_NUMBER_28": "315AL",
    "LINE_NUMBER_29": "322",
    "LINE_NUMBER_30": "323",
    "LINE_NUMBER_31": "324",
    "LINE_NUMBER_32": "325",
    "LINE_NUMBER_33": "326",
    "LINE_NUMBER_34": "42",
    "LINE_NUMBER_35": "",
    "LINE_NUMBER_36": "441",
    "LINE_NUMBER_37": "445",
    "LINE_NUMBER_38": "452",
    "LINE_NUMBER_39": "4A0",
    "LINE_NUMBER_40": "",
    "LINE_NUMBER_41": "481",
    "LINE_NUMBER_42": "482",
    "LINE_NUMBER_43": "483",
    "LINE_NUMBER_44": "484",
    "LINE_NUMBER_45": "485",
    "LINE_NUMBER_46": "486",
    "LINE_NUMBER_47": "487OS",
    "LINE_NUMBER_48": "493",
    "LINE_NUMBER_49": "",
    "LINE_NUMBER_50": "511",
    "LINE_NUMBER_51": "512",
    "LINE_NUMBER_52": "513",
    "LINE_NUMBER_53": "514",
    "LINE_NUMBER_54": "",
    "LINE_NUMBER_55": "",
    "LINE_NUMBER_56": "521CI",
    "LINE_NUMBER_57": "523",
    "LINE_NUMBER_58": "524",
    "LINE_NUMBER_59": "525",
    "LINE_NUMBER_60": "",
    "LINE_NUMBER_61": "",
    "LINE_NUMBER_62": "HS",
    "LINE_NUMBER_63": "ORE",
    "LINE_NUMBER_64": "532RL",
    "LINE_NUMBER_65": "",
    "LINE_NUMBER_66": "",
    "LINE_NUMBER_67": "5411",
    "LINE_NUMBER_68": "5415",
    "LINE_NUMBER_69": "5412OP",
    "LINE_NUMBER_70": "55",
    "LINE_NUMBER_71": "",
    "LINE_NUMBER_72": "561",
    "LINE_NUMBER_73": "562",
    "LINE_NUMBER_74": "",
    "LINE_NUMBER_75": "61",
    "LINE_NUMBER_76": "",
    "LINE_NUMBER_77": "621",
    "LINE_NUMBER_78": "622",
    "LINE_NUMBER_79": "623",
    "LINE_NUMBER_80": "624",
    "LINE_NUMBER_81": "",
    "LINE_NUMBER_82": "",
    "LINE_NUMBER_83": "711AS",
    "LINE_NUMBER_84": "713",
    "LINE_NUMBER_85": "",
    "LINE_NUMBER_86": "721",
    "LINE_NUMBER_87": "722",
    "LINE_NUMBER_88": "81",
    "LINE_NUMBER_89": "",
    "LINE_NUMBER_90": "",
    "LINE_NUMBER_91": "",
    "LINE_NUMBER_92": "GFGD",
    "LINE_NUMBER_93": "GFGN",
    "LINE_NUMBER_94": "GFE",
    "LINE_NUMBER_95": "",
    "LINE_NUMBER_96": "GSLG",
    "LINE_NUMBER_97": "GSLE",
}

PRICE_INDEX_SUMMARY_LINE_NUMBER_TO_BEA_2017_SUMMARY_MAPPING_NON_EMPTY = {
    k: v
    for k, v in PRICE_INDEX_SUMMARY_LINE_NUMBER_TO_BEA_2017_SUMMARY_MAPPING.items()
    if v != ""
}

PRICE_INDEX_SUMMARY_LINE_NUMBER_TO_NAME_MAPPING: ta.Dict[str, str] = {
    # not directly used - just for reference
    "LINE_NUMBER_1": "    All industries",
    "LINE_NUMBER_2": "Private industries",
    "LINE_NUMBER_3": "  Agriculture, forestry, fishing, and hunting",
    "LINE_NUMBER_4": "    Farms",
    "LINE_NUMBER_5": "    Forestry, fishing, and related activities",
    "LINE_NUMBER_6": "  Mining",
    "LINE_NUMBER_7": "    Oil and gas extraction",
    "LINE_NUMBER_8": "    Mining, except oil and gas",
    "LINE_NUMBER_9": "    Support activities for mining",
    "LINE_NUMBER_10": "  Utilities",
    "LINE_NUMBER_11": "  Construction",
    "LINE_NUMBER_12": "  Manufacturing",
    "LINE_NUMBER_13": "    Durable goods",
    "LINE_NUMBER_14": "      Wood products",
    "LINE_NUMBER_15": "      Nonmetallic mineral products",
    "LINE_NUMBER_16": "      Primary metals",
    "LINE_NUMBER_17": "      Fabricated metal products",
    "LINE_NUMBER_18": "      Machinery",
    "LINE_NUMBER_19": "      Computer and electronic products",
    "LINE_NUMBER_20": "      Electrical equipment, appliances, and components",
    "LINE_NUMBER_21": "      Motor vehicles, bodies and trailers, and parts",
    "LINE_NUMBER_22": "      Other transportation equipment",
    "LINE_NUMBER_23": "      Furniture and related products",
    "LINE_NUMBER_24": "      Miscellaneous manufacturing",
    "LINE_NUMBER_25": "    Nondurable goods",
    "LINE_NUMBER_26": "      Food and beverage and tobacco products",
    "LINE_NUMBER_27": "      Textile mills and textile product mills",
    "LINE_NUMBER_28": "      Apparel and leather and allied products",
    "LINE_NUMBER_29": "      Paper products",
    "LINE_NUMBER_30": "      Printing and related support activities",
    "LINE_NUMBER_31": "      Petroleum and coal products",
    "LINE_NUMBER_32": "      Chemical products",
    "LINE_NUMBER_33": "      Plastics and rubber products",
    "LINE_NUMBER_34": "  Wholesale trade",
    "LINE_NUMBER_35": "  Retail trade",
    "LINE_NUMBER_36": "    Motor vehicle and parts dealers",
    "LINE_NUMBER_37": "    Food and beverage stores",
    "LINE_NUMBER_38": "    General merchandise stores",
    "LINE_NUMBER_39": "    Other retail",
    "LINE_NUMBER_40": "  Transportation and warehousing",
    "LINE_NUMBER_41": "    Air transportation",
    "LINE_NUMBER_42": "    Rail transportation",
    "LINE_NUMBER_43": "    Water transportation",
    "LINE_NUMBER_44": "    Truck transportation",
    "LINE_NUMBER_45": "    Transit and ground passenger transportation",
    "LINE_NUMBER_46": "    Pipeline transportation",
    "LINE_NUMBER_47": "    Other transportation and support activities",
    "LINE_NUMBER_48": "    Warehousing and storage",
    "LINE_NUMBER_49": "  Information",
    "LINE_NUMBER_50": "    Publishing industries, except internet (includes software)",
    "LINE_NUMBER_51": "    Motion picture and sound recording industries",
    "LINE_NUMBER_52": "    Broadcasting and telecommunications",
    "LINE_NUMBER_53": "    Data processing, internet publishing, and other information services",
    "LINE_NUMBER_54": "  Finance, insurance, real estate, rental, and leasing",
    "LINE_NUMBER_55": "    Finance and insurance",
    "LINE_NUMBER_56": "      Federal Reserve banks, credit intermediation, and related activities",
    "LINE_NUMBER_57": "      Securities, commodity contracts, and investments",
    "LINE_NUMBER_58": "      Insurance carriers and related activities",
    "LINE_NUMBER_59": "      Funds, trusts, and other financial vehicles",
    "LINE_NUMBER_60": "    Real estate and rental and leasing",
    "LINE_NUMBER_61": "      Real estate",
    "LINE_NUMBER_62": "        Housing",
    "LINE_NUMBER_63": "        Other real estate",
    "LINE_NUMBER_64": "      Rental and leasing services and lessors of intangible assets",
    "LINE_NUMBER_65": "  Professional and business services",
    "LINE_NUMBER_66": "    Professional, scientific, and technical services",
    "LINE_NUMBER_67": "      Legal services",
    "LINE_NUMBER_68": "      Computer systems design and related services",
    "LINE_NUMBER_69": "      Miscellaneous professional, scientific, and technical services",
    "LINE_NUMBER_70": "    Management of companies and enterprises",
    "LINE_NUMBER_71": "    Administrative and waste management services",
    "LINE_NUMBER_72": "      Administrative and support services",
    "LINE_NUMBER_73": "      Waste management and remediation services",
    "LINE_NUMBER_74": "  Educational services, health care, and social assistance",
    "LINE_NUMBER_75": "    Educational services",
    "LINE_NUMBER_76": "    Health care and social assistance",
    "LINE_NUMBER_77": "      Ambulatory health care services",
    "LINE_NUMBER_78": "      Hospitals",
    "LINE_NUMBER_79": "      Nursing and residential care facilities",
    "LINE_NUMBER_80": "      Social assistance",
    "LINE_NUMBER_81": "  Arts, entertainment, recreation, accommodation, and food services",
    "LINE_NUMBER_82": "    Arts, entertainment, and recreation",
    "LINE_NUMBER_83": "      Performing arts, spectator sports, museums, and related activities",
    "LINE_NUMBER_84": "      Amusements, gambling, and recreation industries",
    "LINE_NUMBER_85": "    Accommodation and food services",
    "LINE_NUMBER_86": "      Accommodation",
    "LINE_NUMBER_87": "      Food services and drinking places",
    "LINE_NUMBER_88": "  Other services, except government",
    "LINE_NUMBER_89": "Government",
    "LINE_NUMBER_90": "  Federal",
    "LINE_NUMBER_91": "    General government",
    "LINE_NUMBER_92": "      National defense",
    "LINE_NUMBER_93": "      Nondefense",
    "LINE_NUMBER_94": "    Government enterprises",
    "LINE_NUMBER_95": "  State and local",
    "LINE_NUMBER_96": "    General government",
    "LINE_NUMBER_97": "    Government enterprises",
}


# BEA's "underlying" industry detail - the 191-row frame shared by UGO205-A,
# UII205-A and UVA205-A. Keys are the workbook's own ``Line`` numbers, values
# are the BEA 2017 detail industry codes that aggregate to that line. Only the
# 138 leaves of the 191-row hierarchy appear; the 50 parent rows and the three
# addenda (lines 189-191) are omitted, so the values partition all 402 detail
# industry codes exactly once.
#
# Derived, not hand-written: see
# ``bedrock.extract.iot.gdp.derive_underlying_line_mapping``, which recovers it
# from UGO205-A and UGO305-A by matching gross output over 1997-2024, and
# ``--check-mapping`` on
# ``bedrock/analysis/nowcasting/underlying_industry_coverage.py``.
UNDERLYING_LINE_TO_BEA_2017_INDUSTRY_MAPPING: ta.Dict[int, ta.List[str]] = {
    # Crop production
    5: ["1111A0", "1111B0", "111200", "111300", "111400", "111900"],
    # Animal production and aquaculture
    6: ["112120", "1121A0", "112300", "112A00"],
    # Forestry, fishing, and related activities
    7: ["113000", "114000", "115000"],
    # Oil and gas extraction
    9: ["211000"],
    # Mining, except oil and gas
    10: ["212100", "212230", "2122A0", "212310", "2123A0"],
    # Support activities for mining
    11: ["213111", "21311A"],
    # Electric power generation, transmission, and distribution
    13: ["221100"],
    # Natural gas distribution and water, sewage and other systems
    14: ["221200", "221300"],
    # Education, hospital, and health structures
    16: ["233210", "233262"],
    # Maintenance and repair construction
    17: ["230301", "230302"],
    # Office and commercial structures
    18: ["2332A0"],
    # Other residential construction
    19: ["233412", "2334A0"],
    # Other nonresidential structures
    20: ["233230", "2332D0"],
    # Power and communication structures
    21: ["233240"],
    # Single-family residential structures
    22: ["233411"],
    # Transportation structures and highways and streets
    23: ["2332C0"],
    # Wood products
    26: ["321100", "321200", "321910", "3219A0"],
    # Nonmetallic mineral products
    27: [
        "327100",
        "327200",
        "327310",
        "327320",
        "327330",
        "327390",
        "327400",
        "327910",
        "327991",
        "327992",
        "327993",
        "327999",
    ],
    # Iron and steel mills and manufacturing from purchased steel
    29: ["331110", "331200"],
    # Nonferrous metal production and processing and foundries
    30: [
        "331313",
        "331314",
        "33131B",
        "331410",
        "331420",
        "331490",
        "331510",
        "331520",
    ],
    # Fabricated metal products
    31: [
        "332114",
        "332119",
        "33211A",
        "332200",
        "332310",
        "332320",
        "332410",
        "332420",
        "332430",
        "332500",
        "332600",
        "332710",
        "332720",
        "332800",
        "332913",
        "33291A",
        "332991",
        "332996",
        "332999",
        "33299A",
    ],
    # Agricultural implement manufacturing
    33: ["333111", "333112"],
    # Construction machinery manufacturing
    34: ["333120"],
    # Mining and oil and gas field machinery manufacturing
    35: ["333130"],
    # Other machinery
    36: [
        "333242",
        "33329A",
        "333314",
        "333316",
        "333318",
        "333413",
        "333414",
        "333415",
        "333511",
        "333514",
        "333517",
        "33351B",
        "333611",
        "333612",
        "333613",
        "333618",
        "333912",
        "333914",
        "333920",
        "333991",
        "333993",
        "333994",
        "33399A",
        "33399B",
    ],
    # Computer and peripheral equipment manufacturing
    38: ["334111", "334112", "334118"],
    # Communications equipment manufacturing
    39: ["334210", "334220", "334290"],
    # Semiconductor and other electronic component manufacturing
    40: ["334413", "334418", "33441A"],
    # Navigational, measuring, electromedical, and control instruments manufacturing
    41: [
        "334510",
        "334511",
        "334512",
        "334513",
        "334514",
        "334515",
        "334516",
        "334517",
        "33451A",
    ],
    # Other computer and electronic product manufacturing
    42: ["334300", "334610"],
    # Electrical equipment, appliances, and components
    43: [
        "335110",
        "335120",
        "335210",
        "335220",
        "335311",
        "335312",
        "335313",
        "335314",
        "335911",
        "335912",
        "335920",
        "335930",
        "335991",
        "335999",
    ],
    # Automobile manufacturing
    45: ["336111"],
    # Light truck and utility vehicle manufacturing
    46: ["336112"],
    # Heavy duty truck manufacturing
    47: ["336120"],
    # Motor vehicle body, trailer, and parts manufacturing
    48: [
        "336211",
        "336212",
        "336213",
        "336214",
        "336310",
        "336320",
        "336350",
        "336360",
        "336370",
        "336390",
        "3363A0",
    ],
    # Aerospace product and parts manufacturing
    50: ["336411", "336412", "336413", "336414", "33641A"],
    # All other transportation equipment manufacturing
    51: ["336500", "336611", "336612", "336991", "336992", "336999"],
    # Furniture and related products
    52: [
        "337110",
        "337121",
        "337122",
        "337127",
        "33712N",
        "337215",
        "33721A",
        "337900",
    ],
    # Medical equipment and supplies manufacturing
    54: ["339112", "339113", "339114", "339115", "339116"],
    # Other miscellaneous manufacturing
    55: ["339910", "339920", "339930", "339940", "339950", "339990"],
    # Food manufacturing
    58: [
        "311111",
        "311119",
        "311210",
        "311221",
        "311224",
        "311225",
        "311230",
        "311300",
        "311410",
        "311420",
        "311513",
        "311514",
        "31151A",
        "311520",
        "311615",
        "31161A",
        "311700",
        "311810",
        "3118A0",
        "311910",
        "311920",
        "311930",
        "311940",
        "311990",
    ],
    # Beverage manufacturing
    59: ["312110", "312120", "312130", "312140"],
    # Tobacco product manufacturing
    60: ["312200"],
    # Textile mills and textile product mills
    61: ["313100", "313200", "313300", "314110", "314120", "314900"],
    # Apparel and leather and allied products
    62: ["315000", "316000"],
    # Paper products
    63: [
        "322110",
        "322120",
        "322130",
        "322210",
        "322220",
        "322230",
        "322291",
        "322299",
    ],
    # Printing and related support activities
    64: ["323110", "323120"],
    # Petroleum and coal products
    65: ["324110", "324121", "324122", "324190"],
    # Basic chemical manufacturing
    67: ["325110", "325120", "325130", "325180", "325190"],
    # Resin, rubber, and artificial fibers manufacturing
    68: ["325211", "3252A0"],
    # Pharmaceutical and medicine manufacturing
    69: ["325411", "325412", "325413", "325414"],
    # Other chemical manufacturing
    70: [
        "325310",
        "325320",
        "325510",
        "325520",
        "325610",
        "325620",
        "325910",
        "3259A0",
    ],
    # Plastics and rubber products
    71: [
        "326110",
        "326120",
        "326130",
        "326140",
        "326150",
        "326160",
        "326190",
        "326210",
        "326220",
        "326290",
    ],
    # Motor vehicle and motor vehicle parts and supplies merchant wholesalers
    73: ["423100"],
    # Professional and commercial equipment and supplies merchant wholesalers
    74: ["423400"],
    # Household appliances and electrical and electronic goods merchant wholesalers
    75: ["423600"],
    # Machinery, equipment, and supplies merchant wholesalers
    76: ["423800"],
    # Other durable goods merchant wholesalers
    77: ["423A00"],
    # Drugs and druggists sundries merchant wholesalers
    78: ["424200"],
    # Grocery and related products merchant wholesalers
    79: ["424400"],
    # Petroleum and petroleum products merchant wholesalers
    80: ["424700"],
    # Other nondurable goods merchant wholesalers
    81: ["424A00"],
    # Wholesale electronic markets and agents and brokers
    82: ["425000"],
    # Customs duties
    83: ["4200ID"],
    # Motor vehicle and parts dealers
    85: ["441000"],
    # Food and beverage stores
    86: ["445000"],
    # General merchandise stores
    87: ["452000"],
    # Building material and garden equipment and supplies dealers
    89: ["444000"],
    # Health and personal care stores
    90: ["446000"],
    # Gasoline stations
    91: ["447000"],
    # Clothing and clothing accessories stores
    92: ["448000"],
    # Nonstore retailers
    93: ["454000"],
    # All other retail
    94: ["4B0000"],
    # Air transportation
    96: ["481000"],
    # Rail transportation
    97: ["482000"],
    # Water transportation
    98: ["483000"],
    # Truck transportation
    99: ["484000"],
    # Transit and ground passenger transportation
    100: ["485000"],
    # Pipeline transportation
    101: ["486000"],
    # Scenic and sightseeing transportation and support activities
    103: ["48A000"],
    # Couriers and messengers
    104: ["492000"],
    # Warehousing and storage
    105: ["493000"],
    # Newspaper, periodical, book, and directory publishers
    108: ["511110", "511120", "511130", "5111A0"],
    # Software publishers
    109: ["511200"],
    # Motion picture and sound recording industries
    110: ["512100", "512200"],
    # Broadcasting (except Internet)
    112: ["515100", "515200"],
    # Wired telecommunications carriers
    113: ["517110"],
    # Wireless telecommunications carriers (except satellites)
    114: ["517210"],
    # Other telecommunications, including satellite
    115: ["517A00"],
    # Data processing, hosting, and related services
    117: ["518200"],
    # Other information services
    118: ["519130", "5191A0"],
    # Federal Reserve banks, credit intermediation, and related activities
    121: ["522A00", "52A000"],
    # Securities, commodity contracts, and investments
    122: ["523900", "523A00"],
    # Direct life insurance carriers
    124: ["524113"],
    # Insurance carriers, except direct life insurance
    125: ["5241XX"],
    # Agencies, brokerages, and other insurance related activities
    126: ["524200"],
    # Funds, trusts, and other financial vehicles
    127: ["525000"],
    # Owner-occupied housing
    131: ["531HSO"],
    # Tenant-occupied housing
    132: ["531HST"],
    # Other real estate
    133: ["531ORE"],
    # Rental and leasing services and lessors of intangible assets
    134: ["532100", "532400", "532A00", "533000"],
    # Legal services
    137: ["541100"],
    # Computer systems design and related services
    138: ["541511", "541512", "54151A"],
    # Accounting, tax preparation, bookkeeping, and payroll services
    140: ["541200"],
    # Architectural, engineering, and related services
    141: ["541300"],
    # Management, scientific, and technical consulting services
    142: ["541610", "5416A0"],
    # Scientific research and development services
    143: ["541700"],
    # Advertising, public relations, and related services
    144: ["541800"],
    # Specialized design services and other professional, scientific, and technical services
    145: ["541400", "541920", "541940", "5419A0"],
    # Management of companies and enterprises
    146: ["550000"],
    # Employment services
    149: ["561300"],
    # Services to buildings and dwellings
    150: ["561700"],
    # Other administrative and support services
    151: ["561100", "561200", "561400", "561500", "561600", "561900"],
    # Waste management and remediation services
    152: ["562000"],
    # Educational services
    154: ["611100", "611A00", "611B00"],
    # Offices of physicians
    157: ["621100"],
    # Offices of dentists
    158: ["621200"],
    # Offices of other health practitioners
    159: ["621300"],
    # Outpatient care centers
    160: ["621400"],
    # Other ambulatory health care services
    161: ["621500", "621600", "621900"],
    # Hospitals
    162: ["622000"],
    # Nursing and residential care facilities
    163: ["623A00", "623B00"],
    # Social assistance
    164: ["624100", "624400", "624A00"],
    # Performing arts, spectator sports, museums, and related activities
    167: ["711100", "711200", "711500", "711A00", "712000"],
    # Amusements, gambling, and recreation industries
    168: ["713100", "713200", "713900"],
    # Accommodation
    170: ["721000"],
    # Food services and drinking places
    171: ["722110", "722211", "722A00"],
    # Repair and maintenance
    173: ["811100", "811200", "811300", "811400"],
    # Personal and laundry services
    174: ["812100", "812200", "812300", "812900"],
    # Religious, grantmaking, civic, professional, and similar organizations
    175: ["813100", "813A00", "813B00"],
    # Private households
    176: ["814000"],
    # National defense
    180: ["S00500"],
    # Nondefense
    181: ["S00600"],
    # Government enterprises
    182: ["491000", "S00101", "S00102"],
    # State and local government educational services
    185: ["GSLGE"],
    # State and local government hospitals and health services
    186: ["GSLGH"],
    # State and local government other services
    187: ["GSLGO"],
    # Government enterprises
    188: ["S00201", "S00202", "S00203"],
}

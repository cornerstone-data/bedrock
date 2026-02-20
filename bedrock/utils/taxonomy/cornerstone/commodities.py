import typing as ta

# 405 commodities.
# COMMODITY is the Literal type (for static type checking); COMMODITIES is the runtime list.
# COMMODITY_DESC maps each code to its description; key order must match the Literal order.
COMMODITY = ta.Literal[
    '1111A0',  # Fresh soybeans, canola, flaxseeds, and other oilseeds
    '1111B0',  # Fresh wheat, corn, rice, and other grains
    '111200',  # Fresh vegetables, melons, and potatoes
    '111300',  # Fresh fruits and tree nuts
    '111400',  # Greenhouse crops, mushrooms, nurseries, and flowers
    '111900',  # Tobacco, cotton, sugarcane, peanuts, sugar beets, herbs and spices, and other crops
    '112120',  # Dairies
    '1121A0',  # Cattle ranches and feedlots
    '112300',  # Poultry farms
    '112A00',  # Animal farms and aquaculture ponds (except cattle and poultry)
    '113000',  # Timber and raw forest products
    '114000',  # Wild-caught fish and game
    '115000',  # Agriculture and forestry support
    '211000',  # Unrefined oil and gas
    '212100',  # Coal
    '212230',  # Copper, nickel, lead, and zinc
    '2122A0',  # Iron, gold, silver, and other metal ores
    '212310',  # Dimensional stone
    '2123A0',  # Sand, gravel, clay, phosphate, other nonmetallic minerals
    '213111',  # Well drilling
    '21311A',  # Other support activities for mining
    '221100',  # Electricity
    '221200',  # Natural gas
    '221300',  # Drinking water and wastewater treatment
    '233210',  # Health care buildings
    '233262',  # Schools and vocational buildings
    '230301',  # Nonresidential building repair and maintenance
    '230302',  # Residential building repair and maintenance
    '2332A0',  # Commercial structures, including farm structures
    '233412',  # Multifamily homes
    '2334A0',  # Other residential structures
    '233230',  # Manufacturing buildings
    '2332D0',  # Other nonresidential structures
    '233240',  # Utilities buildings and infrastructure
    '233411',  # Single-family homes
    '2332C0',  # Highways, streets, and bridges
    '321100',  # Lumber and treated lumber
    '321200',  # Plywood and veneer
    '321910',  # Wooden windows, door, and flooring
    '3219A0',  # Veneer, plywood, and engineered wood
    '327100',  # Clay and ceramic products
    '327200',  # Glass and glass products
    '327310',  # Cement
    '327320',  # Ready-mix concrete
    '327330',  # Concrete pipe, bricks, and blocks
    '327390',  # Other concrete products
    '327400',  # Lime and gypsum products
    '327910',  # Abrasive products
    '327991',  # Cut stone and stone products
    '327992',  # Ground or treated minerals and earth
    '327993',  # Mineral wool
    '327999',  # Other nonmetallic mineral products
    '331110',  # Primary iron, steel, and ferroalloy products
    '331200',  # Secondary steel products
    '331313',  # Primary aluminum
    '33131B',  # Secondary aluminum
    '331410',  # Copper, gold and silver concentrates
    '331420',  # Secondary copper products
    '331490',  # Other secondary nonferrous metal products
    '331510',  # Cast iron and steel
    '331520',  # Nonferrous metal casts
    '332114',  # Custom metal rolls
    '33211A',  # All other forging, stamping, and sintering
    '332119',  # Lids, jars, bottle caps, other metal closures and crowns
    '332200',  # Cutlery and handtools
    '332310',  # Metal structural products
    '332320',  # Metal windows, doors, and architectural products
    '332410',  # Power boilers and heat exchangers
    '332420',  # Heavy gauge metal tanks
    '332430',  # Light gauge metal cans, boxes, and containers
    '332500',  # Metal hinges, keys, lock, and other hardware
    '332600',  # Springs and wires
    '332710',  # Machine shops
    '332720',  # Screws, nuts, and bolts
    '332800',  # Metal coatings, engravings, and heat treatments
    '332913',  # Metal plumbing drains, faucets, valves, and other fittings
    '33291A',  # Valve and fittings (except for plumbing)
    '332991',  # Ball and roller bearings
    '332996',  # Fabricated pipe and pipe fittings
    '33299A',  # Ammunition, arms, ordnance, and related accessories
    '332999',  # Misc. fabricated metal products
    '333111',  # Farm machinery and equipment
    '333112',  # Lawn and garden equipment
    '333120',  # Construction machinery
    '333130',  # Mining and oil/gas field machinery
    '333242',  # Semiconductor machinery
    '33329A',  # Machinery for the paper, textile, food or other industries (except semiconductor machinery)
    '333314',  # Optical instruments and lenses
    '333316',  # Photography and photocopying equipment
    '333318',  # Other commercial and service industry machinery
    '333414',  # Heating equipment other than warm air furnaces
    '333415',  # Air conditioning, refrigeration, and warm air heating equipment
    '333413',  # Air purification and ventilation equipment
    '333511',  # Industrial molds
    '333514',  # Special tools, dies, jigs, and fixtures
    '333517',  # Metal cutting and forming machine tools
    '33351B',  # Cutting and machine tool accessory, rolling mill, and other metalworking machines
    '333611',  # Turbines and turbine generator sets
    '333612',  # Speed changers, industrial high-speed drives, and gears
    '333613',  # Mechanical power transmission equipment
    '333618',  # Other engine equipment
    '333912',  # Air and gas compressors
    '333914',  # Pumps and pumping equipment
    '333920',  # Material handling equipment
    '333991',  # Power tools
    '333993',  # Packaging machinery
    '333994',  # Industrial process furnaces and ovens
    '33399A',  # Welding and Soldering Equipment, Scales and Balances, and other general purpose machinery
    '33399B',  # Hydraulic pumps, motors, cylinders and actuators
    '334111',  # Computers
    '334112',  # Computer storage device readers
    '334118',  # Computer terminals and other computer peripheral equipment
    '334210',  # Telephones
    '334220',  # Wireless communications
    '334290',  # Communications equipment
    '334413',  # Semiconductors
    '334418',  # Printed circuit and electronic assembly
    '33441A',  # Electronic capacitors, resistors, coils, transformers, connectors and other components (except  semiconductors and printed circuit assemblies)
    '334510',  # Electromedical appartuses
    '334511',  # Navigation instruments
    '334512',  # Automatic controls for HVAC and refrigeration equipment
    '334513',  # Industrial process variable instruments
    '334514',  # Fluid meters and counting devices
    '334515',  # Signal testing instruments
    '334516',  # Analytical laboratory instruments
    '334517',  # Irradiation apparatuses
    '33451A',  # Watches, clocks, and other measuring and controlling devices
    '334300',  # Audio and video equipment
    '334610',  # External hard drives, CDs, other storage media
    '335110',  # Light bulbs
    '335120',  # Light fixtures
    '335210',  # Small electrical appliances
    '335220',  # Major home appliances
    '335311',  # Specialty transformers
    '335312',  # Motors and generators
    '335313',  # Switchgear and switchboards
    '335314',  # Relay and industrial controls
    '335911',  # Storage batteries
    '335912',  # Primary batteries
    '335920',  # Communication and energy wire and cable
    '335930',  # Wiring devices
    '335991',  # Carbon and graphite products
    '335999',  # Other miscellaneous electrical equipment and components
    '336111',  # Automobiles
    '336112',  # Pickup trucks, vans, and SUVs
    '336120',  # Heavy duty trucks
    '336211',  # Vehicle bodies
    '336212',  # Truck trailers
    '336213',  # Motor homes
    '336214',  # Travel trailer and campers
    '336310',  # Vehicle engines and engine parts
    '336320',  # Vehicle electrical and electronic equipment
    '336350',  # Transmission and power train parts
    '336360',  # Vehicle seating and interior trim (upholstery)
    '336370',  # Vehicle metal stamping
    '336390',  # Other vehicle parts
    '3363A0',  # Motor vehicle steering, suspension components (except spring), and brake systems
    '336411',  # Aircraft
    '336412',  # Aircraft engines and parts
    '336413',  # Other aircraft parts
    '336414',  # Guided missiles and space vehicles
    '33641A',  # Propulsion units and parts for space vehicles and guided missiles
    '336500',  # Railroad rolling stock
    '336611',  # Ships and ship repair
    '336612',  # Boats
    '336991',  # Motorcycle, bicycle, and parts
    '336992',  # Military armored vehicles and tanks
    '336999',  # Other transportation equipment
    '337110',  # Wood kitchen cabinets and countertops
    '337121',  # Home furniture - upholstered
    '337122',  # Home furniture - wood, nonupholstered
    '337127',  # Institutional furniture
    '33712N',  # Home furniture - Cabinets and non-wood, nonupholstered
    '337215',  # Shelving and lockers
    '33721A',  # Office furniture and custom architectural woodwork and millwork
    '337900',  # Mattresses, blinds and shades
    '339112',  # Surgical and medical instruments
    '339113',  # Surgical appliance and supplies
    '339114',  # Dental equipment and supplies
    '339115',  # Ophthalmic goods
    '339116',  # Dental laboratories
    '339910',  # Jewelry and silverware
    '339920',  # Sporting and athletic goods
    '339930',  # Dolls, toys, and games
    '339940',  # Office supplies (not paper)
    '339950',  # Signs
    '339990',  # Gaskets, seals, musical instruments, fasteners, brooms, brushes, mop and other misc. goods
    '311111',  # Dog and cat food
    '311119',  # Other animal food
    '311210',  # Flours and malts
    '311221',  # Corn products
    '311225',  # Refined vegetable, olive, and seed oils
    '311224',  # Vegetable oils and by-products
    '311230',  # Breakfast cereals
    '311300',  # Sugar, candy, and chocolate
    '311410',  # Frozen food
    '311420',  # Fruit and vegetable preservation
    '311513',  # Cheese
    '311514',  # Dry, condensed, and evaporated dairy
    '31151A',  # Fluid milk and butter
    '311520',  # Ice cream and frozen desserts
    '311615',  # Packaged poultry
    '31161A',  # Packaged meat (except poultry)
    '311700',  # Seafood
    '311810',  # Bread and other baked goods
    '3118A0',  # Cookies, crackers, pastas, and tortillas
    '311910',  # Snack foods
    '311920',  # Coffee and tea
    '311930',  # Flavored drink concentrates
    '311940',  # Seasonings and dressings
    '311990',  # All other foods
    '312110',  # Soft drinks, bottled water, and ice
    '312120',  # Breweries and beer
    '312130',  # Wineries and wine
    '312140',  # Distilleries and spirits
    '312200',  # Tobacco products
    '313100',  # Fiber, yarn, and thread
    '313200',  # Fabric
    '313300',  # Finished and coated fabric
    '314110',  # Carpets and rugs
    '314120',  # Curtains and linens
    '314900',  # Other textiles
    '315000',  # Clothing
    '316000',  # Leather
    '322110',  # Wood pulp
    '322120',  # Paper
    '322130',  # Cardboard
    '322210',  # Cardboard containers
    '322220',  # Paper bags and coated paper
    '322230',  # Stationery
    '322291',  # Sanitary paper (tissues, napkins, diapers, etc.)
    '322299',  # All other converted paper products
    '323110',  # Books, newspapers, magazines, and other print media
    '323120',  # Printing support
    '324110',  # Gasoline, fuels, and by-products of petroleum refining
    '324121',  # Asphalt pavement
    '324122',  # Asphalt shingles
    '324190',  # Other petroleum and coal products
    '325110',  # Petrochemicals
    '325120',  # Compressed Gases
    '325130',  # Synthetic dyes and pigments
    '325180',  # Other basic inorganic chemicals
    '325190',  # Other basic organic chemicals
    '325211',  # Plastics
    '3252A0',  # Synthetic rubber and artificial and synthetic fibers
    '325411',  # Medicinal and botanical ingredients
    '325412',  # Pharmaceutical products (pills, powders, solutions, etc.)
    '325413',  # Blood sugar, pregnancy, and other diagnostic test kits
    '325414',  # Vaccines and other biological medical products
    '325310',  # Fertilizers
    '325320',  # Pesticides
    '325510',  # Paints and coatings
    '325520',  # Adhesives
    '325610',  # Soap and cleaning compounds
    '325620',  # Toiletries
    '325910',  # Ink and ink cartridges
    '3259A0',  # Chemicals (except basic chemicals, agrichemicals, polymers, paints, pharmaceuticals,soaps, cleaning compounds)
    '326110',  # Plastic bags, films, and sheets
    '326120',  # Plastic pipe, fittings, and sausage casings
    '326130',  # Laminated plastic plates and shapes
    '326140',  # Polystyrene foam products
    '326150',  # Urethane and other foam products
    '326160',  # Plastic bottles
    '326190',  # Other plastic products
    '326210',  # Rubber tires
    '326220',  # Rubber and plastic belts and hoses
    '326290',  # Other rubber products
    '423100',  # Motor vehicle and motor vehicle parts and supplies
    '423400',  # Professional and commercial equipment and supplies
    '423600',  # Household appliances and electrical and electronic goods
    '423800',  # Machinery, equipment, and supplies
    '423A00',  # Other durable goods merchant wholesalers
    '424200',  # Drugs and druggists sundries
    '424400',  # Grocery and related product wholesalers
    '424700',  # Petroleum and petroleum products
    '424A00',  # Other nondurable goods merchant wholesalers
    '425000',  # Wholesale electronic markets and agents and brokers
    '4200ID',  # Customs duties
    '441000',  # Vehicles and parts sales
    '445000',  # Food and beverage stores
    '452000',  # General merchandise stores
    '444000',  # Building material and garden equipment and supplies dealers
    '446000',  # Health and personal care stores
    '447000',  # Gasoline stations
    '448000',  # Clothing and clothing accessories stores
    '454000',  # Nonstore retailers
    '4B0000',  # Other retail
    '481000',  # Air transport
    '482000',  # Rail transport
    '483000',  # Water transport (boats, ships, ferries)
    '484000',  # Truck transport
    '485000',  # Passenger ground transport
    '486000',  # Pipeline transport
    '48A000',  # Scenic and sightseeing transportation and support activities for transportation
    '492000',  # Couriers and messengers
    '493000',  # Warehousing
    '511110',  # Newspapers
    '511120',  # Magazines and journals
    '511130',  # Books
    '5111A0',  # Directory, mailing list, and other publishers
    '511200',  # Software
    '512100',  # Movies and film
    '512200',  # Sound recording
    '515100',  # Radio and television
    '515200',  # Cable and subscription programming
    '517110',  # Telecommunications
    '517210',  # Wireless telecommunications
    '517A00',  # Satellite, telecommunications resellers, and all other telecommunications
    '518200',  # Data processing and hosting
    '519130',  # Internet publishing and broadcasting
    '5191A0',  # News syndicates, libraries, archives, Internet publishing and all other information services
    '522A00',  # Nondepository credit intermediation and related activities
    '52A000',  # Monetary authorities and depository credit intermediation
    '523900',  # Investment advice, portfolio management, and other financial advising services
    '523A00',  # Securities and commodities brokerage and exchanges
    '524113',  # Direct life insurance carriers
    '5241XX',  # Insurance carriers, except direct life
    '524200',  # Insurance agencies and brokerages
    '525000',  # Funds, trusts, and financial vehicles
    '531HSO',  # Owner-occupied housing
    '531HST',  # Tenant-occupied housing
    '531ORE',  # Other real estate
    '532100',  # Vehicle rental and leasing
    '532400',  # Commercial equipment rental
    '532A00',  # Consumer goods and general rental centers
    '533000',  # Lessors of nonfinancial intangible assets
    '541100',  # Legal services
    '541511',  # Custom computer programming
    '541512',  # Computer systems design
    '54151A',  # Other computer related services, including facilities management
    '541200',  # Accounting, tax preparation, bookkeeping, and payroll
    '541300',  # Architectural, engineering, and related services
    '541610',  # Management consulting
    '5416A0',  # Environmental and other technical consulting services
    '541700',  # Scientific research and development
    '541800',  # Advertising and public relations
    '541400',  # Specialized design
    '541920',  # Photographers
    '541940',  # Veterinarians
    '5419A0',  # Marketing research and all other miscellaneous professional, scientific, and technical services
    '550000',  # Company and enterprise management
    '561300',  # Employment services
    '561700',  # Buildings and dwellings services
    '561100',  # Office administration
    '561200',  # Facilities support
    '561400',  # Business support
    '561500',  # Travel arrangement and reservation
    '561600',  # Investigation and security
    '561900',  # Other support services
    '562111',  # Solid waste collection
    '562HAZ',  # Hazardous waste collection treatment and disposal
    '562212',  # Solid waste landfilling
    '562213',  # Solid waste combustors and incinerators
    '562910',  # Remediation services
    '562920',  # Material separation/recovery facilities
    '562OTH',  # Other waste collection and treatment services
    '611100',  # Elementary and secondary schools
    '611A00',  # Colleges, universities, junior colleges, and professional schools
    '611B00',  # Other educational services
    '621100',  # Physicians
    '621200',  # Dentists
    '621300',  # Healthcare practitioners (except physicians and dentists)
    '621400',  # Outpatient healthcare
    '621500',  # Medical laboratories
    '621600',  # Home healthcare
    '621900',  # Ambulances
    '622000',  # Hospitals
    '623A00',  # Nursing and community care facilities
    '623B00',  # Residential mental retardation, mental health, substance abuse and other facilities
    '624100',  # Individual and family services
    '624400',  # Child day care
    '624A00',  # Community food, housing, and other relief services, including rehabilitation services
    '711100',  # Performances
    '711200',  # Sports
    '711500',  # Independent artists, writers, and performers
    '711A00',  # Promoters and agents
    '712000',  # Museums, historical sites, zoos, and parks
    '713100',  # Amusement parks and arcades
    '713200',  # Gambling establishments (except casino hotels)
    '713900',  # Golf courses, marinas, ski resorts, fitness and other rec centers and industries
    '721000',  # Hotels and campgrounds
    '722110',  # Full-service restaurants
    '722211',  # Limited-service restaurants
    '722A00',  # All other food and drinking places
    '811100',  # Vehicle repair
    '811200',  # Electronic  equipment repair and maintenance
    '811300',  # Commercial machinery repair
    '811400',  # Household goods repair
    '812100',  # Salons and barber shops
    '812200',  # Funerary services
    '812300',  # Dry-cleaning and laundry
    '812900',  # Pet care, photofinishing, parking and other sundry services
    '813100',  # Religious organizations
    '813A00',  # Grantmaking, giving, and social advocacy organizations
    '813B00',  # Civic, social, professional, and similar organizations
    '814000',  # Household employees
    'S00500',  # Federal general government (defense)
    'S00600',  # Federal general government (nondefense)
    '491000',  # Postal service
    'S00102',  # Other federal government enterprises
    'GSLGE',  # State and local government educational services
    'GSLGH',  # State and local government hospitals and health services
    'GSLGO',  # State and local government other services
    'S00203',  # Other state and local government enterprises
    'S00402',  # Used and secondhand goods
]
COMMODITIES: ta.List[COMMODITY] = list(ta.get_args(COMMODITY))

COMMODITY_DESC: ta.Dict[COMMODITY, str] = {
    # order matters
    '1111A0': 'Fresh soybeans, canola, flaxseeds, and other oilseeds',
    '1111B0': 'Fresh wheat, corn, rice, and other grains',
    '111200': 'Fresh vegetables, melons, and potatoes',
    '111300': 'Fresh fruits and tree nuts',
    '111400': 'Greenhouse crops, mushrooms, nurseries, and flowers',
    '111900': 'Tobacco, cotton, sugarcane, peanuts, sugar beets, herbs and spices, and other crops',
    '112120': 'Dairies',
    '1121A0': 'Cattle ranches and feedlots',
    '112300': 'Poultry farms',
    '112A00': 'Animal farms and aquaculture ponds (except cattle and poultry)',
    '113000': 'Timber and raw forest products',
    '114000': 'Wild-caught fish and game',
    '115000': 'Agriculture and forestry support',
    '211000': 'Unrefined oil and gas',
    '212100': 'Coal',
    '212230': 'Copper, nickel, lead, and zinc',
    '2122A0': 'Iron, gold, silver, and other metal ores',
    '212310': 'Dimensional stone',
    '2123A0': 'Sand, gravel, clay, phosphate, other nonmetallic minerals',
    '213111': 'Well drilling',
    '21311A': 'Other support activities for mining',
    '221100': 'Electricity',
    '221200': 'Natural gas',
    '221300': 'Drinking water and wastewater treatment',
    '233210': 'Health care buildings',
    '233262': 'Schools and vocational buildings',
    '230301': 'Nonresidential building repair and maintenance',
    '230302': 'Residential building repair and maintenance',
    '2332A0': 'Commercial structures, including farm structures',
    '233412': 'Multifamily homes',
    '2334A0': 'Other residential structures',
    '233230': 'Manufacturing buildings',
    '2332D0': 'Other nonresidential structures',
    '233240': 'Utilities buildings and infrastructure',
    '233411': 'Single-family homes',
    '2332C0': 'Highways, streets, and bridges',
    '321100': 'Lumber and treated lumber',
    '321200': 'Plywood and veneer',
    '321910': 'Wooden windows, door, and flooring',
    '3219A0': 'Veneer, plywood, and engineered wood',
    '327100': 'Clay and ceramic products',
    '327200': 'Glass and glass products',
    '327310': 'Cement',
    '327320': 'Ready-mix concrete',
    '327330': 'Concrete pipe, bricks, and blocks',
    '327390': 'Other concrete products',
    '327400': 'Lime and gypsum products',
    '327910': 'Abrasive products',
    '327991': 'Cut stone and stone products',
    '327992': 'Ground or treated minerals and earth',
    '327993': 'Mineral wool',
    '327999': 'Other nonmetallic mineral products',
    '331110': 'Primary iron, steel, and ferroalloy products',
    '331200': 'Secondary steel products',
    '331313': 'Primary aluminum',
    '33131B': 'Secondary aluminum',
    '331410': 'Copper, gold and silver concentrates',
    '331420': 'Secondary copper products',
    '331490': 'Other secondary nonferrous metal products',
    '331510': 'Cast iron and steel',
    '331520': 'Nonferrous metal casts',
    '332114': 'Custom metal rolls',
    '33211A': 'All other forging, stamping, and sintering',
    '332119': 'Lids, jars, bottle caps, other metal closures and crowns',
    '332200': 'Cutlery and handtools',
    '332310': 'Metal structural products',
    '332320': 'Metal windows, doors, and architectural products',
    '332410': 'Power boilers and heat exchangers',
    '332420': 'Heavy gauge metal tanks',
    '332430': 'Light gauge metal cans, boxes, and containers',
    '332500': 'Metal hinges, keys, lock, and other hardware',
    '332600': 'Springs and wires',
    '332710': 'Machine shops',
    '332720': 'Screws, nuts, and bolts',
    '332800': 'Metal coatings, engravings, and heat treatments',
    '332913': 'Metal plumbing drains, faucets, valves, and other fittings',
    '33291A': 'Valve and fittings (except for plumbing)',
    '332991': 'Ball and roller bearings',
    '332996': 'Fabricated pipe and pipe fittings',
    '33299A': 'Ammunition, arms, ordnance, and related accessories',
    '332999': 'Misc. fabricated metal products',
    '333111': 'Farm machinery and equipment',
    '333112': 'Lawn and garden equipment',
    '333120': 'Construction machinery',
    '333130': 'Mining and oil/gas field machinery',
    '333242': 'Semiconductor machinery',
    '33329A': 'Machinery for the paper, textile, food or other industries (except semiconductor machinery)',
    '333314': 'Optical instruments and lenses',
    '333316': 'Photography and photocopying equipment',
    '333318': 'Other commercial and service industry machinery',
    '333414': 'Heating equipment other than warm air furnaces',
    '333415': 'Air conditioning, refrigeration, and warm air heating equipment',
    '333413': 'Air purification and ventilation equipment',
    '333511': 'Industrial molds',
    '333514': 'Special tools, dies, jigs, and fixtures',
    '333517': 'Metal cutting and forming machine tools',
    '33351B': 'Cutting and machine tool accessory, rolling mill, and other metalworking machines',
    '333611': 'Turbines and turbine generator sets',
    '333612': 'Speed changers, industrial high-speed drives, and gears',
    '333613': 'Mechanical power transmission equipment',
    '333618': 'Other engine equipment',
    '333912': 'Air and gas compressors',
    '333914': 'Pumps and pumping equipment',
    '333920': 'Material handling equipment',
    '333991': 'Power tools',
    '333993': 'Packaging machinery',
    '333994': 'Industrial process furnaces and ovens',
    '33399A': 'Welding and Soldering Equipment, Scales and Balances, and other general purpose machinery',
    '33399B': 'Hydraulic pumps, motors, cylinders and actuators',
    '334111': 'Computers',
    '334112': 'Computer storage device readers',
    '334118': 'Computer terminals and other computer peripheral equipment',
    '334210': 'Telephones',
    '334220': 'Wireless communications',
    '334290': 'Communications equipment',
    '334413': 'Semiconductors',
    '334418': 'Printed circuit and electronic assembly',
    '33441A': 'Electronic capacitors, resistors, coils, transformers, connectors and other components (except  semiconductors and printed circuit assemblies)',
    '334510': 'Electromedical appartuses',
    '334511': 'Navigation instruments',
    '334512': 'Automatic controls for HVAC and refrigeration equipment',
    '334513': 'Industrial process variable instruments',
    '334514': 'Fluid meters and counting devices',
    '334515': 'Signal testing instruments',
    '334516': 'Analytical laboratory instruments',
    '334517': 'Irradiation apparatuses',
    '33451A': 'Watches, clocks, and other measuring and controlling devices',
    '334300': 'Audio and video equipment',
    '334610': 'External hard drives, CDs, other storage media',
    '335110': 'Light bulbs',
    '335120': 'Light fixtures',
    '335210': 'Small electrical appliances',
    '335220': 'Major home appliances',
    '335311': 'Specialty transformers',
    '335312': 'Motors and generators',
    '335313': 'Switchgear and switchboards',
    '335314': 'Relay and industrial controls',
    '335911': 'Storage batteries',
    '335912': 'Primary batteries',
    '335920': 'Communication and energy wire and cable',
    '335930': 'Wiring devices',
    '335991': 'Carbon and graphite products',
    '335999': 'Other miscellaneous electrical equipment and components',
    '336111': 'Automobiles',
    '336112': 'Pickup trucks, vans, and SUVs',
    '336120': 'Heavy duty trucks',
    '336211': 'Vehicle bodies',
    '336212': 'Truck trailers',
    '336213': 'Motor homes',
    '336214': 'Travel trailer and campers',
    '336310': 'Vehicle engines and engine parts',
    '336320': 'Vehicle electrical and electronic equipment',
    '336350': 'Transmission and power train parts',
    '336360': 'Vehicle seating and interior trim (upholstery)',
    '336370': 'Vehicle metal stamping',
    '336390': 'Other vehicle parts',
    '3363A0': 'Motor vehicle steering, suspension components (except spring), and brake systems',
    '336411': 'Aircraft',
    '336412': 'Aircraft engines and parts',
    '336413': 'Other aircraft parts',
    '336414': 'Guided missiles and space vehicles',
    '33641A': 'Propulsion units and parts for space vehicles and guided missiles',
    '336500': 'Railroad rolling stock',
    '336611': 'Ships and ship repair',
    '336612': 'Boats',
    '336991': 'Motorcycle, bicycle, and parts',
    '336992': 'Military armored vehicles and tanks',
    '336999': 'Other transportation equipment',
    '337110': 'Wood kitchen cabinets and countertops',
    '337121': 'Home furniture - upholstered',
    '337122': 'Home furniture - wood, nonupholstered',
    '337127': 'Institutional furniture',
    '33712N': 'Home furniture - Cabinets and non-wood, nonupholstered',
    '337215': 'Shelving and lockers',
    '33721A': 'Office furniture and custom architectural woodwork and millwork',
    '337900': 'Mattresses, blinds and shades',
    '339112': 'Surgical and medical instruments',
    '339113': 'Surgical appliance and supplies',
    '339114': 'Dental equipment and supplies',
    '339115': 'Ophthalmic goods',
    '339116': 'Dental laboratories',
    '339910': 'Jewelry and silverware',
    '339920': 'Sporting and athletic goods',
    '339930': 'Dolls, toys, and games',
    '339940': 'Office supplies (not paper)',
    '339950': 'Signs',
    '339990': 'Gaskets, seals, musical instruments, fasteners, brooms, brushes, mop and other misc. goods',
    '311111': 'Dog and cat food',
    '311119': 'Other animal food',
    '311210': 'Flours and malts',
    '311221': 'Corn products',
    '311225': 'Refined vegetable, olive, and seed oils',
    '311224': 'Vegetable oils and by-products',
    '311230': 'Breakfast cereals',
    '311300': 'Sugar, candy, and chocolate',
    '311410': 'Frozen food',
    '311420': 'Fruit and vegetable preservation',
    '311513': 'Cheese',
    '311514': 'Dry, condensed, and evaporated dairy',
    '31151A': 'Fluid milk and butter',
    '311520': 'Ice cream and frozen desserts',
    '311615': 'Packaged poultry',
    '31161A': 'Packaged meat (except poultry)',
    '311700': 'Seafood',
    '311810': 'Bread and other baked goods',
    '3118A0': 'Cookies, crackers, pastas, and tortillas',
    '311910': 'Snack foods',
    '311920': 'Coffee and tea',
    '311930': 'Flavored drink concentrates',
    '311940': 'Seasonings and dressings',
    '311990': 'All other foods',
    '312110': 'Soft drinks, bottled water, and ice',
    '312120': 'Breweries and beer',
    '312130': 'Wineries and wine',
    '312140': 'Distilleries and spirits',
    '312200': 'Tobacco products',
    '313100': 'Fiber, yarn, and thread',
    '313200': 'Fabric',
    '313300': 'Finished and coated fabric',
    '314110': 'Carpets and rugs',
    '314120': 'Curtains and linens',
    '314900': 'Other textiles',
    '315000': 'Clothing',
    '316000': 'Leather',
    '322110': 'Wood pulp',
    '322120': 'Paper',
    '322130': 'Cardboard',
    '322210': 'Cardboard containers',
    '322220': 'Paper bags and coated paper',
    '322230': 'Stationery',
    '322291': 'Sanitary paper (tissues, napkins, diapers, etc.)',
    '322299': 'All other converted paper products',
    '323110': 'Books, newspapers, magazines, and other print media',
    '323120': 'Printing support',
    '324110': 'Gasoline, fuels, and by-products of petroleum refining',
    '324121': 'Asphalt pavement',
    '324122': 'Asphalt shingles',
    '324190': 'Other petroleum and coal products',
    '325110': 'Petrochemicals',
    '325120': 'Compressed Gases',
    '325130': 'Synthetic dyes and pigments',
    '325180': 'Other basic inorganic chemicals',
    '325190': 'Other basic organic chemicals',
    '325211': 'Plastics',
    '3252A0': 'Synthetic rubber and artificial and synthetic fibers',
    '325411': 'Medicinal and botanical ingredients',
    '325412': 'Pharmaceutical products (pills, powders, solutions, etc.)',
    '325413': 'Blood sugar, pregnancy, and other diagnostic test kits',
    '325414': 'Vaccines and other biological medical products',
    '325310': 'Fertilizers',
    '325320': 'Pesticides',
    '325510': 'Paints and coatings',
    '325520': 'Adhesives',
    '325610': 'Soap and cleaning compounds',
    '325620': 'Toiletries',
    '325910': 'Ink and ink cartridges',
    '3259A0': 'Chemicals (except basic chemicals, agrichemicals, polymers, paints, pharmaceuticals,soaps, cleaning compounds)',
    '326110': 'Plastic bags, films, and sheets',
    '326120': 'Plastic pipe, fittings, and sausage casings',
    '326130': 'Laminated plastic plates and shapes',
    '326140': 'Polystyrene foam products',
    '326150': 'Urethane and other foam products',
    '326160': 'Plastic bottles',
    '326190': 'Other plastic products',
    '326210': 'Rubber tires',
    '326220': 'Rubber and plastic belts and hoses',
    '326290': 'Other rubber products',
    '423100': 'Motor vehicle and motor vehicle parts and supplies',
    '423400': 'Professional and commercial equipment and supplies',
    '423600': 'Household appliances and electrical and electronic goods',
    '423800': 'Machinery, equipment, and supplies',
    '423A00': 'Other durable goods merchant wholesalers',
    '424200': 'Drugs and druggists sundries',
    '424400': 'Grocery and related product wholesalers',
    '424700': 'Petroleum and petroleum products',
    '424A00': 'Other nondurable goods merchant wholesalers',
    '425000': 'Wholesale electronic markets and agents and brokers',
    '4200ID': 'Customs duties',
    '441000': 'Vehicles and parts sales',
    '445000': 'Food and beverage stores',
    '452000': 'General merchandise stores',
    '444000': 'Building material and garden equipment and supplies dealers',
    '446000': 'Health and personal care stores',
    '447000': 'Gasoline stations',
    '448000': 'Clothing and clothing accessories stores',
    '454000': 'Nonstore retailers',
    '4B0000': 'Other retail',
    '481000': 'Air transport',
    '482000': 'Rail transport',
    '483000': 'Water transport (boats, ships, ferries)',
    '484000': 'Truck transport',
    '485000': 'Passenger ground transport',
    '486000': 'Pipeline transport',
    '48A000': 'Scenic and sightseeing transportation and support activities for transportation',
    '492000': 'Couriers and messengers',
    '493000': 'Warehousing',
    '511110': 'Newspapers',
    '511120': 'Magazines and journals',
    '511130': 'Books',
    '5111A0': 'Directory, mailing list, and other publishers',
    '511200': 'Software',
    '512100': 'Movies and film',
    '512200': 'Sound recording',
    '515100': 'Radio and television',
    '515200': 'Cable and subscription programming',
    '517110': 'Telecommunications',
    '517210': 'Wireless telecommunications',
    '517A00': 'Satellite, telecommunications resellers, and all other telecommunications',
    '518200': 'Data processing and hosting',
    '519130': 'Internet publishing and broadcasting',
    '5191A0': 'News syndicates, libraries, archives, Internet publishing and all other information services',
    '522A00': 'Nondepository credit intermediation and related activities',
    '52A000': 'Monetary authorities and depository credit intermediation',
    '523900': 'Investment advice, portfolio management, and other financial advising services',
    '523A00': 'Securities and commodities brokerage and exchanges',
    '524113': 'Direct life insurance carriers',
    '5241XX': 'Insurance carriers, except direct life',
    '524200': 'Insurance agencies and brokerages',
    '525000': 'Funds, trusts, and financial vehicles',
    '531HSO': 'Owner-occupied housing',
    '531HST': 'Tenant-occupied housing',
    '531ORE': 'Other real estate',
    '532100': 'Vehicle rental and leasing',
    '532400': 'Commercial equipment rental',
    '532A00': 'Consumer goods and general rental centers',
    '533000': 'Lessors of nonfinancial intangible assets',
    '541100': 'Legal services',
    '541511': 'Custom computer programming',
    '541512': 'Computer systems design',
    '54151A': 'Other computer related services, including facilities management',
    '541200': 'Accounting, tax preparation, bookkeeping, and payroll',
    '541300': 'Architectural, engineering, and related services',
    '541610': 'Management consulting',
    '5416A0': 'Environmental and other technical consulting services',
    '541700': 'Scientific research and development',
    '541800': 'Advertising and public relations',
    '541400': 'Specialized design',
    '541920': 'Photographers',
    '541940': 'Veterinarians',
    '5419A0': 'Marketing research and all other miscellaneous professional, scientific, and technical services',
    '550000': 'Company and enterprise management',
    '561300': 'Employment services',
    '561700': 'Buildings and dwellings services',
    '561100': 'Office administration',
    '561200': 'Facilities support',
    '561400': 'Business support',
    '561500': 'Travel arrangement and reservation',
    '561600': 'Investigation and security',
    '561900': 'Other support services',
    '562111': 'Solid waste collection',
    '562HAZ': 'Hazardous waste collection treatment and disposal',
    '562212': 'Solid waste landfilling',
    '562213': 'Solid waste combustors and incinerators',
    '562910': 'Remediation services',
    '562920': 'Material separation/recovery facilities',
    '562OTH': 'Other waste collection and treatment services',
    '611100': 'Elementary and secondary schools',
    '611A00': 'Colleges, universities, junior colleges, and professional schools',
    '611B00': 'Other educational services',
    '621100': 'Physicians',
    '621200': 'Dentists',
    '621300': 'Healthcare practitioners (except physicians and dentists)',
    '621400': 'Outpatient healthcare',
    '621500': 'Medical laboratories',
    '621600': 'Home healthcare',
    '621900': 'Ambulances',
    '622000': 'Hospitals',
    '623A00': 'Nursing and community care facilities',
    '623B00': 'Residential mental retardation, mental health, substance abuse and other facilities',
    '624100': 'Individual and family services',
    '624400': 'Child day care',
    '624A00': 'Community food, housing, and other relief services, including rehabilitation services',
    '711100': 'Performances',
    '711200': 'Sports',
    '711500': 'Independent artists, writers, and performers',
    '711A00': 'Promoters and agents',
    '712000': 'Museums, historical sites, zoos, and parks',
    '713100': 'Amusement parks and arcades',
    '713200': 'Gambling establishments (except casino hotels)',
    '713900': 'Golf courses, marinas, ski resorts, fitness and other rec centers and industries',
    '721000': 'Hotels and campgrounds',
    '722110': 'Full-service restaurants',
    '722211': 'Limited-service restaurants',
    '722A00': 'All other food and drinking places',
    '811100': 'Vehicle repair',
    '811200': 'Electronic  equipment repair and maintenance',
    '811300': 'Commercial machinery repair',
    '811400': 'Household goods repair',
    '812100': 'Salons and barber shops',
    '812200': 'Funerary services',
    '812300': 'Dry-cleaning and laundry',
    '812900': 'Pet care, photofinishing, parking and other sundry services',
    '813100': 'Religious organizations',
    '813A00': 'Grantmaking, giving, and social advocacy organizations',
    '813B00': 'Civic, social, professional, and similar organizations',
    '814000': 'Household employees',
    'S00500': 'Federal general government (defense)',
    'S00600': 'Federal general government (nondefense)',
    '491000': 'Postal service',
    'S00102': 'Other federal government enterprises',
    'GSLGE': 'State and local government educational services',
    'GSLGH': 'State and local government hospitals and health services',
    'GSLGO': 'State and local government other services',
    'S00203': 'Other state and local government enterprises',
    'S00402': 'Used and secondhand goods',
}

WASTE_DISAGG_COMMODITIES: ta.Dict[str, ta.List[COMMODITY]] = {
    '562000': [
        '562111',
        '562HAZ',
        '562212',
        '562213',
        '562910',
        '562920',
        '562OTH',
    ]
}

import os
from pathlib import Path

from esupy.processed_data_mgmt import Paths, mkdir_if_missing
from esupy.util import get_git_hash, return_pkg_version

MODULEPATH = Path(__file__).resolve().parents[2]

GCS_FLOWSA_DIR = "flowsa"

datapath = MODULEPATH / 'data'
mappingpath = MODULEPATH / 'utils' / 'mapping'
crosswalkpath = mappingpath / 'activitytosectormapping'
configpath = MODULEPATH / 'utils' / 'config'
externaldatapath = MODULEPATH / 'extract' / 'external_data'
process_adjustmentpath = MODULEPATH / 'extract' / 'process_adjustments'

extractpath = MODULEPATH / 'extract'
flowbysectormethodpath = MODULEPATH / 'transform' / 'flowbysectormethods'
flowbysectoractivitysetspath = MODULEPATH / 'transform' / 'flowbysectoractivitysets'

# "Paths()" are a class defined in esupy
PATHS = Paths()
PATHS.local_path = PATHS.local_path / 'bedrock'
outputpath = PATHS.local_path
fbaoutputpath = outputpath / 'FlowByActivity'
fbsoutputpath = outputpath / 'FlowBySector'
biboutputpath = outputpath / 'Bibliography'
logoutputpath = outputpath / 'Log'
diffpath = outputpath / 'FBSComparisons'
plotoutputpath = outputpath / 'Plots'
tableoutputpath = outputpath / 'DisplayTables'

# ensure directories exist
mkdir_if_missing(logoutputpath)
mkdir_if_missing(plotoutputpath)
mkdir_if_missing(tableoutputpath)

DEFAULT_DOWNLOAD_IF_MISSING = False

# paths to scripts
scriptpath = MODULEPATH.parent / 'scripts'
scriptsFBApath = scriptpath / 'FlowByActivity_Datasets'

NAME_SEP_CHAR = '.'
# ^^^ Used to separate source/activity set names as part of 'full_name' attr


# https://stackoverflow.com/a/41125461
def memory_limit(percentage=0.93):
    # Placed here becuase older versions of Python do not have this
    import resource  # noqa: PLC0415

    # noinspection PyBroadException
    try:
        max_memory = get_memory()
        print(f"Max Memory: {max_memory}")
    except Exception:
        print("Could not determine max memory")
    else:
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        resource.setrlimit(
            resource.RLIMIT_AS, (int(max_memory * 1024 * percentage), hard)
        )


def get_memory():
    with open('/proc/meminfo', 'r') as mem:
        free_memory = 0
        for i in mem:
            sline = i.split()
            if str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:', 'SwapFree:'):
                free_memory += int(sline[1])
    return free_memory


# metadata
PKG = "flowsa"
PKG_VERSION_NUMBER = return_pkg_version(MODULEPATH, 'flowsa')
GIT_HASH_LONG = os.environ.get('GITHUB_SHA') or get_git_hash('long')
if GIT_HASH_LONG:
    GIT_HASH = GIT_HASH_LONG[0:7]
else:
    GIT_HASH = None

# Common declaration of write format for package data products
WRITE_FORMAT = "parquet"

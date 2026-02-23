import os
import subprocess
from importlib.metadata import version
from pathlib import Path

from esupy.processed_data_mgmt import Paths, mkdir_if_missing

MODULEPATH = Path(__file__).resolve().parents[2]

GCS_FLOWSA_DIR = 'flowsa'

datapath = MODULEPATH / 'data'
mappingpath = MODULEPATH / 'utils' / 'mapping'
crosswalkpath = mappingpath / 'activitytosectormapping'
configpath = MODULEPATH / 'utils' / 'config'
externaldatapath = MODULEPATH / 'extract' / 'external_data'
process_adjustmentpath = MODULEPATH / 'extract' / 'process_adjustments'

extractpath = MODULEPATH / 'extract'
transformpath = MODULEPATH / 'transform'

# "Paths()" are a class defined in esupy
PATHS = Paths()
PATHS.local_path = PATHS.local_path / 'bedrock'
outputpath = PATHS.local_path
FBA_DIR = extractpath / 'output_data'
FBS_DIR = transformpath / 'output_data'
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


def return_folder_path(base_path: Path | str, filename: str) -> Path:
    """
    Return the folder path of a file

    :param base_path: path to "extract", "transform", "publish" directories
    :param filename: string, name of file for which to return the folder path
    """
    base_path = Path(base_path)
    folder = filename.lower()
    if '.' in folder:
        folder = folder.split('.')[0]

    while True:
        folder_path = base_path / folder
        if folder_path.is_dir():
            return folder_path

        if '_' not in folder:
            return base_path

        folder = folder.rsplit('_', 1)[0]


# https://stackoverflow.com/a/41125461
def memory_limit(percentage: float = 0.93) -> None:
    # Placed here becuase older versions of Python do not have this
    import resource  # noqa: PLC0415

    # noinspection PyBroadException
    try:
        max_memory = get_memory()
        print(f'Max Memory: {max_memory}')
    except Exception:
        print('Could not determine max memory')
    else:
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        resource.setrlimit(
            resource.RLIMIT_AS, (int(max_memory * 1024 * percentage), hard)
        )


def get_memory() -> int:
    with open('/proc/meminfo', 'r') as mem:
        free_memory = 0
        for i in mem:
            sline = i.split()
            if str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:', 'SwapFree:'):
                free_memory += int(sline[1])
    return free_memory


def return_pkg_version(MODULEPATH: Path, package_name: str) -> str:
    """
    Return package version, first look for git tag, then look for installed package version
    :param MODULEPATH: str, package path
    :param packagename: str, such as "bedrock"
    """

    # return version with git describe
    try:
        # set path to package repository, necessary if running method files
        # outside the package repo
        tags = (
            subprocess.check_output(
                ['git', 'describe', '--tags', '--always', '--match', 'v[0-9]*'],
                cwd=MODULEPATH,
            )
            .decode()
            .strip()
        )

        if tags.startswith('v'):
            return tags.split('-', 1)[0].replace('v', '')

    # If it's a hash, pass
    except subprocess.CalledProcessError:
        pass

    # else return installed package version
    return version(package_name)


def get_git_hash(MODULEPATH: Path, length: str = 'short') -> str | None:
    """
    Returns git_hash of current directory or None if no git found
    :param MODULEPATH: Path, module path
    :param length: str, 'short' for 7-digit, 'long' for full git hash
    :return git_hash: str
    """
    try:
        git_hash = (
            subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=MODULEPATH)
            .decode()
            .strip()
        )

        return git_hash if length == 'long' else git_hash[:7]

    except Exception:
        return None


def get_git_branch(module_path: Path) -> str | None:
    try:
        return (
            subprocess.check_output(
                ['git', 'rev-parse', '--abbrev-ref', 'HEAD'], cwd=module_path
            )
            .decode()
            .strip()
        )
    except Exception:
        return None


def get_git_pr_url(module_path: Path) -> str | None:
    try:
        return (
            subprocess.check_output(
                ['gh', 'pr', 'view', '--json', 'url', '-q', '.url'],
                cwd=module_path,
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        ) or None
    except Exception:
        return None


# metadata
PKG = 'bedrock'
PKG_VERSION_NUMBER = return_pkg_version(MODULEPATH, PKG)
GIT_HASH_LONG = os.environ.get('GITHUB_SHA') or get_git_hash(MODULEPATH, 'long')
GIT_HASH = GIT_HASH_LONG[:7] if GIT_HASH_LONG else None
GIT_BRANCH = (
    os.environ.get('GITHUB_HEAD_REF')
    or os.environ.get('GITHUB_REF_NAME')
    or get_git_branch(MODULEPATH)
)
GIT_PR_URL = get_git_pr_url(MODULEPATH)


# Common declaration of write format for package data products
WRITE_FORMAT = 'parquet'

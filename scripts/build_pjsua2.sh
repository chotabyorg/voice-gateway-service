#!/usr/bin/env bash
# Prepare an isolated native runtime; never targets a running service implicitly.
set -euo pipefail
VENV=${1:?Usage: bash scripts/build_pjsua2.sh /absolute/path/to/staging-venv}
WHEEL_DIRECTORY=${2:-}
[[ "$VENV" = /* ]] || { printf 'An absolute staging venv is required.\n' >&2; exit 2; }
if [[ -n "$WHEEL_DIRECTORY" ]]; then
    [[ "$WHEEL_DIRECTORY" = /* ]] || { printf 'An absolute wheel directory is required.\n' >&2; exit 2; }
fi
PYTHON="$VENV/bin/python"
"$PYTHON" -c 'import sys; assert (3,11) <= sys.version_info[:2] < (3,13)'
command -v swig >/dev/null
command -v g++ >/dev/null
BUILD=$(mktemp -d)
trap 'rm -rf -- "$BUILD"' EXIT

# Official pjproject 2.17, pinned to the dereferenced release commit.
REVISION=5a457451fa2712ba18e12b01738e8ff3af2b26fd
git clone --quiet --depth 1 --branch 2.17 https://github.com/pjsip/pjproject.git "$BUILD/pjproject"
cd "$BUILD/pjproject"
[[ "$(git rev-parse HEAD)" = "$REVISION" ]]
# Covers the supported 32 active + 8 waiting settings; actual runtime stays bounded.
printf '#define PJSUA_MAX_CALLS 64\n' > pjlib/include/pj/config_site.h
./configure CFLAGS='-O2 -fPIC' CXXFLAGS='-O2 -fPIC' --disable-video --disable-sound
make dep
# One compiler at a time to bound memory consumption on the voice host.
make -j1
"$PYTHON" -c 'import setuptools, wheel'
cd pjsip-apps/src/swig/python
# SWIG's large generated wrapper otherwise inherits Python's debug symbols,
# which can exhaust a small host. Keep the native libraries optimized above.
CFLAGS='-g0' CXXFLAGS='-g0' make wheel PYTHON_EXE="$PYTHON"
if [[ -n "$WHEEL_DIRECTORY" ]]; then
    mkdir -p "$WHEEL_DIRECTORY"
    cp dist/pjsua2-*.whl "$WHEEL_DIRECTORY/"
fi
"$PYTHON" -m pip install --no-deps dist/pjsua2-*.whl
"$PYTHON" - <<'PY'
import ctypes
import pjsua2 as pj
assert pj.Endpoint().libVersion().full == '2.17'
assert hasattr(pj.AudioMediaPort, 'createPort')
assert pj.EpConfig().uaConfig.maxCalls == 64
endpoint = pj.Endpoint()
endpoint.libCreate()
try:
    config = pj.EpConfig()
    config.uaConfig.maxCalls = 40
    config.uaConfig.threadCnt = 0
    config.uaConfig.mainThreadOnly = True
    config.logConfig.level = config.logConfig.consoleLevel = 0
    config.logConfig.msgLogging = False
    endpoint.libInit(config)
    maximum = ctypes.CDLL(pj._pjsua2.__file__).pjsua_call_get_max_count
    maximum.restype, maximum.argtypes = ctypes.c_uint, []
    assert maximum() == 40
finally:
    endpoint.libDestroy()
print('PJSUA2 2.17 native capacity verified: 40 configured / 64 compiled')
PY

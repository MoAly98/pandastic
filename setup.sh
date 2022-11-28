# set up some grid programs

# setup ATLAS
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
alias setupATLAS='source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh'
echo "=== running setupATLAS ==="
setupATLAS -q -3

# define functions
trap "_cleanup; trap - RETURN" RETURN
_cleanup() {
    unset -f _voms_proxy_long
    unset -f _cleanup
    echo "cleaning up"
}

_voms_proxy_long ()
{
    if ! type voms-proxy-info &> /dev/null; then
        echo "voms not set up!" 1>&2
        return 1
    fi
    local VOMS_ARGS="--voms atlas";
    if voms-proxy-info --exists --valid 24:00; then
        local TIME_LEFT=$(voms-proxy-info --timeleft);
        local HOURS=$(( $TIME_LEFT / 3600 ));
        local MINUTES=$(( $TIME_LEFT / 60 % 60 - 1 ));
        local NEW_TIME=$HOURS:$MINUTES;
        VOMS_ARGS+=" --noregen --valid $NEW_TIME";
    else
        VOMS_ARGS+=" --valid 96:00";
    fi;
    voms-proxy-init $VOMS_ARGS
}

if ! _voms_proxy_long; then return 1; fi
if ! lsetup panda pyami rucio -q; then return 1; fi

# The skimmer directory added to PYTHONPATH
export PYTHONPATH=${PYTHONPATH}:./
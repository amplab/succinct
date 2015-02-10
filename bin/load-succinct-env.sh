#!/usr/bin/env bash

# This script loads succinct-env.sh if it exists, and ensures it is only loaded once.
# succinct-env.sh is loaded from SUCCINCT_CONF_DIR if set, or within the current directory's
# conf/ subdirectory.

if [ -z "$SUCCINCT_ENV_LOADED" ]; then
  export SUCCINCT_ENV_LOADED=1

  # Returns the parent of the directory this script lives in.
  parent_dir="$(cd "`dirname "$0"`"/..; pwd)"

  user_conf_dir="${SUCCINCT_CONF_DIR:-"$parent_dir"/conf}"

  if [ -f "${user_conf_dir}/succinct-env.sh" ]; then
    # Promote all variable declarations to environment (exported) variables
    set -a
    . "${user_conf_dir}/succinct-env.sh"
    set +a
  fi
fi

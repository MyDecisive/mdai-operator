#!/bin/bash

set -euo pipefail

HELM_CHART_ROOT='deployment'
TEMPLATES_DIR="${HELM_CHART_ROOT}/templates"
DEPLOYMENT_TEMPLATE="${TEMPLATES_DIR}/deployment.yaml"

HEADER='{{- if (index .Values "greptimedb-standalone").enabled }}'
FOOTER='{{- end }}'

if [ -f "${DEPLOYMENT_TEMPLATE}" ]; then
  awk -v header="$HEADER" -v footer="$FOOTER" '
    # Start of the GreptimeDB env block
    !inblock && $0 ~ /^[[:space:]]*-[[:space:]]+name:[[:space:]]+GREPTIME_/ {
      match($0, /^[[:space:]]*/); indent = RLENGTH
      print header
      inblock = 1
      print
      next
    }
    # Inside the block: a sibling/parent line (indent <= block indent) that is
    # not another GREPTIME_* entry marks the end of the block.
    inblock {
      match($0, /^[[:space:]]*/); cur = RLENGTH
      isGreptime = ($0 ~ /^[[:space:]]*-[[:space:]]+name:[[:space:]]+GREPTIME_/)
      if (cur <= indent && !isGreptime) {
        print footer
        inblock = 0
      }
      print
      next
    }
    { print }
    END { if (inblock) print footer }
  ' "${DEPLOYMENT_TEMPLATE}" > "${TEMPLATES_DIR}/.tmp_greptimedb_env" \
    && mv "${TEMPLATES_DIR}/.tmp_greptimedb_env" "${DEPLOYMENT_TEMPLATE}"
fi

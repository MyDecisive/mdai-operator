#!/bin/bash

set -euo pipefail

HELM_CHART_ROOT='deployment'
TEMPLATES_DIR="${HELM_CHART_ROOT}/templates"
XDS_TEMPLATE="${TEMPLATES_DIR}/xds-service.yaml"

if [ -f "${XDS_TEMPLATE}" ]; then
  {
    printf '%s\n' '{{- if .Values.xdsService.enabled }}'
    cat "${XDS_TEMPLATE}"
    printf '%s\n' '{{- end }}'
  } > "${TEMPLATES_DIR}/.tmp_xds_service"
  mv "${TEMPLATES_DIR}/.tmp_xds_service" "${XDS_TEMPLATE}"
fi

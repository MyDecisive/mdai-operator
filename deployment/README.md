# mdai-operator

![Version: 0.2.21](https://img.shields.io/badge/Version-0.2.21-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.2.21](https://img.shields.io/badge/AppVersion-0.2.21-informational?style=flat-square)

MDAI Operator Helm Chart

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| admissionWebhooks.autoGenerateCert.certPeriodDays | int | `365` |  |
| admissionWebhooks.autoGenerateCert.enabled | bool | `false` |  |
| admissionWebhooks.autoGenerateCert.recreate | bool | `true` |  |
| admissionWebhooks.caFile | string | `""` |  |
| admissionWebhooks.certFile | string | `""` |  |
| admissionWebhooks.certManager.certificateAnnotations | object | `{}` |  |
| admissionWebhooks.certManager.duration | string | `""` |  |
| admissionWebhooks.certManager.enabled | bool | `true` |  |
| admissionWebhooks.certManager.issuerAnnotations | object | `{}` |  |
| admissionWebhooks.certManager.issuerRef | object | `{}` |  |
| admissionWebhooks.certManager.renewBefore | string | `""` |  |
| admissionWebhooks.create | bool | `true` |  |
| admissionWebhooks.keyFile | string | `""` |  |
| admissionWebhooks.secretName | string | `""` |  |
| controllerManager.manager.args[0] | string | `"--metrics-bind-address=:8443"` |  |
| controllerManager.manager.args[1] | string | `"--leader-elect=false"` |  |
| controllerManager.manager.args[2] | string | `"--health-probe-bind-address=:8081"` |  |
| controllerManager.manager.args[3] | string | `"--metrics-cert-path=/tmp/k8s-metrics-server/metrics-certs"` |  |
| controllerManager.manager.args[4] | string | `"--webhook-cert-path=/tmp/k8s-webhook-server/serving-certs"` |  |
| controllerManager.manager.args[5] | string | `"--xds-port=18000"` |  |
| controllerManager.manager.containerSecurityContext.allowPrivilegeEscalation | bool | `false` |  |
| controllerManager.manager.containerSecurityContext.capabilities.drop[0] | string | `"ALL"` |  |
| controllerManager.manager.env.otelExporterOtlpEndpoint | string | `"http://hub-monitor-mdai-collector-service.mdai.svc.cluster.local:4318"` |  |
| controllerManager.manager.env.otelSdkDisabled | string | `"false"` |  |
| controllerManager.manager.env.useConsoleLogEncoder | string | `"false"` |  |
| controllerManager.manager.env.valkeyAuditStreamExpiryMs | string | `"2592000000"` |  |
| controllerManager.manager.image.repository | string | `"ghcr.io/mydecisive/mdai-operator"` |  |
| controllerManager.manager.resources.limits.cpu | string | `"500m"` |  |
| controllerManager.manager.resources.limits.memory | string | `"128Mi"` |  |
| controllerManager.manager.resources.requests.cpu | string | `"10m"` |  |
| controllerManager.manager.resources.requests.memory | string | `"64Mi"` |  |
| controllerManager.nodeSelector | object | `{}` |  |
| controllerManager.podSecurityContext.runAsNonRoot | bool | `true` |  |
| controllerManager.podSecurityContext.seccompProfile.type | string | `"RuntimeDefault"` |  |
| controllerManager.replicas | int | `1` |  |
| controllerManager.tolerations | list | `[]` |  |
| controllerManager.topologySpreadConstraints | list | `[]` |  |
| crds.enabled | bool | `true` |  |
| kubernetesClusterDomain | string | `"cluster.local"` |  |
| metricsService.ports[0].name | string | `"https"` |  |
| metricsService.ports[0].port | int | `8443` |  |
| metricsService.ports[0].protocol | string | `"TCP"` |  |
| metricsService.ports[0].targetPort | int | `8443` |  |
| metricsService.type | string | `"ClusterIP"` |  |
| serviceAccount.annotations | object | `{}` |  |
| serviceAccount.automount | bool | `true` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.name | string | `""` |  |
| webhookService.ports[0].port | int | `443` |  |
| webhookService.ports[0].protocol | string | `"TCP"` |  |
| webhookService.ports[0].targetPort | int | `9443` |  |
| webhookService.type | string | `"ClusterIP"` |  |
| xdsService.enabled | bool | `true` |  |
| xdsService.ports[0].name | string | `"xds"` |  |
| xdsService.ports[0].port | int | `18000` |  |
| xdsService.ports[0].protocol | string | `"TCP"` |  |
| xdsService.ports[0].targetPort | int | `18000` |  |
| xdsService.type | string | `"ClusterIP"` |  |

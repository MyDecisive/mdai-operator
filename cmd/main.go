// nolint: mnd
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"path/filepath"

	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/go-logr/zapr"
	opentelemetryv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	prometheusv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/mydecisive/mdai-operator/internal/controller"
	webhookmdaiv1 "github.com/mydecisive/mdai-operator/internal/webhook/v1"
	"github.com/mydecisive/mdai-operator/internal/xds"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

const (
	enableWebhooksEnvVar           = "ENABLE_WEBHOOKS"
	useConsoleLogEncoderEnvVar     = "USE_CONSOLE_LOG_ENCODER"
	otelSdkDisabledEnvVar          = "OTEL_SDK_DISABLED"
	otelExporterOtlpEndpointEnvVar = "OTEL_EXPORTER_OTLP_ENDPOINT"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() { //nolint:gochecknoinits
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(mdaiv1.AddToScheme(scheme))
	utilruntime.Must(opentelemetryv1beta1.AddToScheme(scheme))
	utilruntime.Must(prometheusv1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var (
		metricsAddr                                      string
		metricsCertPath, metricsCertName, metricsCertKey string
		webhookCertPath, webhookCertName, webhookCertKey string
		enableLeaderElection                             bool
		probeAddr                                        string
		secureMetrics                                    bool
		enableHTTP2                                      bool
		xdsPort                                          int
		tlsOpts                                          []func(*tls.Config)
	)
	bindFlags(
		&metricsAddr, &metricsCertPath, &metricsCertName, &metricsCertKey,
		&webhookCertPath, &webhookCertName, &webhookCertKey,
		&enableLeaderElection, &probeAddr, &secureMetrics, &enableHTTP2,
		&xdsPort,
	)

	ctx := context.Background()

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(ctx)
	if err != nil {
		setupLog.Error(err, "Error setting up OpenTelemetry SDK. Set "+otelSdkDisabledEnvVar+` to "true" to bypass this.`)
		os.Exit(1)
	}

	gracefullyShutdownWithCode := func(code int) {
		if err := otelShutdown(ctx); err != nil {
			setupLog.Error(err, "OTEL SDK did not shut down gracefully!")
		}
		os.Exit(code)
	}

	defer gracefullyShutdownWithCode(0)

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	var encoder zapcore.Encoder

	if os.Getenv(useConsoleLogEncoderEnvVar) == "true" {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	}

	zapOpts := ctrlzap.Options{
		Encoder:     encoder,
		Development: true,
	}

	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	zapLogger := ctrlzap.NewRaw(ctrlzap.UseFlagOptions(&zapOpts))
	logger := zapr.NewLogger(zapLogger)
	if !otelSdkEnabled() {
		logger.Info("OTEL SDK has been disabled with " + otelSdkDisabledEnvVar + " environment variable")
	}
	otelSdkEnabledStr := os.Getenv(otelSdkDisabledEnvVar)
	otlpEndpointStr := os.Getenv(otelExporterOtlpEndpointEnvVar)
	if otelSdkEnabledStr == "" && otlpEndpointStr == "" {
		logger.Info("WARNING: No OTLP endpoint is defined, but OTEL SDK is enabled." +
			" Please set either " + otelSdkDisabledEnvVar + " or " + otelExporterOtlpEndpointEnvVar +
			" environment variable. You will receive 'OTEL SDK error' logs until this is resolved.")
	}
	otelLogger := attachOtelLogger(logger)
	ctrl.SetLogger(otelLogger)

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Create watchers for metrics and webhooks certificates
	var metricsCertWatcher, webhookCertWatcher *certwatcher.CertWatcher

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts

	if webhookCertPath != "" {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		var err error
		webhookCertWatcher, err = certwatcher.New(
			filepath.Join(webhookCertPath, webhookCertName),
			filepath.Join(webhookCertPath, webhookCertKey),
		)
		if err != nil {
			setupLog.Error(err, "Failed to initialize webhook certificate watcher")
			gracefullyShutdownWithCode(1)
		}

		webhookTLSOpts = append(webhookTLSOpts, func(config *tls.Config) {
			config.GetCertificate = webhookCertWatcher.GetCertificate
		})
	}
	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: webhookTLSOpts,
	})

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.2/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.2/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if metricsCertPath != "" {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		var err error
		metricsCertWatcher, err = certwatcher.New(
			filepath.Join(metricsCertPath, metricsCertName),
			filepath.Join(metricsCertPath, metricsCertKey),
		)
		if err != nil {
			setupLog.Error(err, "to initialize metrics certificate watcher", "error", err)
			gracefullyShutdownWithCode(1)
		}

		metricsServerOptions.TLSOpts = append(metricsServerOptions.TLSOpts, func(config *tls.Config) {
			config.GetCertificate = metricsCertWatcher.GetCertificate
		})
	}

	cacheOptions := cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&corev1.ConfigMap{}: {
				Label: labels.SelectorFromSet(labels.Set{
					controller.LabelManagedByMdaiKey: controller.LabelManagedByMdaiValue,
				}),
			},
			&corev1.Service{}: {
				Label: labels.SelectorFromSet(labels.Set{
					controller.LabelManagedByMdaiKey: controller.LabelManagedByMdaiValue,
				}),
			},
			&appsv1.Deployment{}: {
				Label: labels.SelectorFromSet(labels.Set{
					controller.LabelManagedByMdaiKey: controller.LabelManagedByMdaiValue,
				}),
			},
		},
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		Cache:                  cacheOptions,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "ec755d87.mydecisive.ai",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		gracefullyShutdownWithCode(1)
	}

	// Initialize XDS
	xdsManager := xds.NewXDSManager()
	xdsServer := xds.NewXDSServer(xdsManager, xdsPort)
	if err := mgr.Add(xdsServer); err != nil {
		setupLog.Error(err, "unable to add xDS server to manager")
		gracefullyShutdownWithCode(1)
	}

	if err = (&controller.MdaiHubReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		ZapLogger: zapLogger,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MdaiHub")
		gracefullyShutdownWithCode(1)
	}

	if err = (&controller.MdaiCollectorReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("mdaicollector-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MdaiCollector")
		gracefullyShutdownWithCode(1)
	}

	if err = (&controller.XDSReconciler{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		XDSManager: xdsManager,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create xDS controller", "controller", "XDS")
		gracefullyShutdownWithCode(1)
	}

	if err = (&controller.TelemetryValidationReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "TelemetryValidation")
		gracefullyShutdownWithCode(1)
	}

	if err = (&controller.MdaiObserverReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MdaiObserver")
		gracefullyShutdownWithCode(1)
	}

	if err := controller.SetMdaiIngressIndexers(ctx, mgr); err != nil {
		setupLog.Error(err, "unable to create indexers", "controller", "MdaiIngress")
		gracefullyShutdownWithCode(1)
	}
	if err := (&controller.MdaiIngressReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Logger: zapLogger,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MdaiIngress")
		gracefullyShutdownWithCode(1)
	}
	// +kubebuilder:scaffold:builder

	if err = (&controller.MdaiReplayReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MdaiReplay")
		gracefullyShutdownWithCode(1)
	}
	if err := (&controller.MdaiDalReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MdaiDal")
		gracefullyShutdownWithCode(1)
	}
	// +kubebuilder:scaffold:builder

	// nolint:goconst
	if os.Getenv(enableWebhooksEnvVar) != "false" {
		setupWebhooksOrExplode(mgr, gracefullyShutdownWithCode)
	}

	if metricsCertWatcher != nil {
		setupLog.Info("Adding metrics certificate watcher to manager")
		if err := mgr.Add(metricsCertWatcher); err != nil {
			setupLog.Error(err, "unable to add metrics certificate watcher to manager")
			gracefullyShutdownWithCode(1)
		}
	}

	if webhookCertWatcher != nil {
		setupLog.Info("Adding webhook certificate watcher to manager")
		if err := mgr.Add(webhookCertWatcher); err != nil {
			setupLog.Error(err, "unable to add webhook certificate watcher to manager")
			gracefullyShutdownWithCode(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		gracefullyShutdownWithCode(1)
	}

	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		gracefullyShutdownWithCode(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		gracefullyShutdownWithCode(1)
	}
}

func setupWebhooksOrExplode(mgr manager.Manager, gracefullyShutdownWithCode func(code int)) {
	if err := webhookmdaiv1.SetupMdaiHubWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "MdaiHub")
		gracefullyShutdownWithCode(1)
	}
	if err := webhookmdaiv1.SetupMdaiCollectorWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "MdaiCollector")
		gracefullyShutdownWithCode(1)
	}
	if err := webhookmdaiv1.SetupMdaiObserverWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "MdaiObserver")
		gracefullyShutdownWithCode(1)
	}
	if err := webhookmdaiv1.SetupMdaiIngressWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "MdaiIngress")
		gracefullyShutdownWithCode(1)
	}
	if err := webhookmdaiv1.SetupMdaiReplayWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "MdaiReplay")
		gracefullyShutdownWithCode(1)
	}
}

func bindFlags(
	metricsAddr *string,
	metricsCertPath, metricsCertName, metricsCertKey *string,
	webhookCertPath, webhookCertName, webhookCertKey *string,
	enableLeaderElection *bool,
	probeAddr *string,
	secureMetrics *bool,
	enableHTTP2 *bool,
	xdsPort *int,
) {
	flag.StringVar(metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.IntVar(xdsPort, "xds-port", 18000, "The port the xDS server binds to.")
	flag.BoolVar(enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
}

func otelSdkEnabled() bool {
	return os.Getenv(otelSdkDisabledEnvVar) != "true"
}

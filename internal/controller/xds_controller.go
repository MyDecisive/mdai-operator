package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logger "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/mydecisive/mdai-operator/internal/xds"
)

const (
	xdsProxyServiceNameEnv      = "XDS_PROXY_SERVICE_NAME"
	xdsProxyServiceNamespaceEnv = "XDS_PROXY_SERVICE_NAMESPACE"
	xdsProxyServiceSelectorEnv  = "XDS_PROXY_SERVICE_SELECTOR"
	PodNamespaceEnv             = "POD_NAMESPACE"
	xdsProxyMarkerKey           = "hub.mydecisive.ai/xds-proxy"
	xdsRoleLabelKey             = "hub.mydecisive.ai/role"
	connectionCollectorRole     = "connection-collector"
	xdsManagedServicePortPrefix = "xds-"
	splitNKeyValue              = 2
	defaultOTLPGRPCPort         = 4317
	defaultOTLPHTTPPort         = 4318
)

var (
	errProxyServiceNotConfigured = errors.New("proxy service not configured")
	errMarkedProxyServiceMissing = errors.New("marked proxy service missing")
)

// XDSManager defines the interface for updating the xDS snapshot
type XDSManager interface {
	UpdateSnapshot(ctx context.Context, nodeID string, collectors []otelv1beta1.OpenTelemetryCollector, validations []hubv1.TelemetryValidation) error
}

// XDSReconciler reconciles xDS snapshots based on OpenTelemetryCollector objects
type XDSReconciler struct {
	client.Client

	APIReader  client.Reader
	Scheme     *runtime.Scheme
	XDSManager XDSManager
	Namespace  string // operator namespace; scopes marked-service/deployment discovery
}

// +kubebuilder:rbac:groups=opentelemetry.io,resources=opentelemetrycollectors,verbs=get;list;watch

// Reconcile handles xDS snapshot updates
func (r *XDSReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logger.FromContext(ctx)
	log.Info("-- Starting xDS reconciliation (OpenTelemetryCollector) --", "trigger", req.NamespacedName)

	if r.XDSManager == nil {
		log.V(1).Info("xDS Manager not initialized, skipping reconciliation")
		return ctrl.Result{}, nil
	}

	var collectors otelv1beta1.OpenTelemetryCollectorList
	if err := r.List(ctx, &collectors); err != nil {
		log.Error(err, "unable to list OpenTelemetryCollectors for xDS update")
		return ctrl.Result{}, err
	}
	eligibleCollectors := eligibleCollectorsForXDS(collectors.Items)

	var validations hubv1.TelemetryValidationList
	if err := r.List(ctx, &validations); err != nil {
		log.Error(err, "unable to list TelemetryValidations for xDS update")
		return ctrl.Result{}, err
	}

	// For now, we use a fixed nodeID as configured in the hub chart
	if err := r.XDSManager.UpdateSnapshot(ctx, "envoy-hub-proxy", eligibleCollectors, validations.Items); err != nil {
		log.Error(err, "failed to update xDS snapshot")
		return ctrl.Result{}, err
	}

	if err := r.reconcileProxyServicePorts(ctx, eligibleCollectors); err != nil {
		log.Error(err, "failed to reconcile xDS proxy service ports")
		return ctrl.Result{}, err
	}

	log.Info("-- Finished xDS reconciliation (OpenTelemetryCollector) --")
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *XDSReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&otelv1beta1.OpenTelemetryCollector{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
			if obj == nil {
				return false
			}
			return isWatchedCollectorObjectForXDS(obj)
		}))).
		Watches(
			&hubv1.TelemetryValidation{},
			handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
				tv, ok := obj.(*hubv1.TelemetryValidation)
				if !ok || tv.Spec.CollectorRef.Name == "" {
					return nil
				}
				return []reconcile.Request{{
					NamespacedName: types.NamespacedName{
						Name:      tv.Spec.CollectorRef.Name,
						Namespace: tv.Namespace,
					},
				}}
			}),
		).
		Named("xds-reconciler").
		Complete(r)
}

func (r *XDSReconciler) reconcileProxyServicePorts(ctx context.Context, collectors []otelv1beta1.OpenTelemetryCollector) error {
	desiredManagedPorts := managedServicePorts(discoveredCollectorPorts(collectors))

	var (
		service *corev1.Service
		err     error
	)
	if len(desiredManagedPorts) > 0 {
		service, err = r.getOrCreateProxyService(ctx, desiredManagedPorts)
		if err != nil {
			return err
		}
	} else {
		// No collectors: find an existing service to clean up stale managed ports,
		// but do not create one.
		if service, err = r.findExistingProxyService(ctx); err != nil || service == nil {
			return err
		}
	}

	updatedPorts := mergeServicePorts(service.Spec.Ports, desiredManagedPorts)

	if servicePortsEqual(service.Spec.Ports, updatedPorts) {
		return nil
	}

	service.Spec.Ports = updatedPorts
	return r.Update(ctx, service)
}

// findExistingProxyService locates the proxy service using the same resolution order as
// getOrCreateProxyService but never creates one. Returns (nil, nil) if none exists.
func (r *XDSReconciler) findExistingProxyService(ctx context.Context) (*corev1.Service, error) {
	serviceName := strings.TrimSpace(os.Getenv(xdsProxyServiceNameEnv))
	serviceNamespace := strings.TrimSpace(os.Getenv(xdsProxyServiceNamespaceEnv))
	if serviceName != "" && serviceNamespace != "" {
		service := &corev1.Service{}
		if err := r.reader().Get(ctx, types.NamespacedName{Name: serviceName, Namespace: serviceNamespace}, service); err == nil {
			return service, nil
		} else if !apierrors.IsNotFound(err) {
			return nil, err
		}
		return nil, nil
	}

	if service, err := r.getMarkedProxyService(ctx); err == nil {
		return service, nil
	} else if !errors.Is(err, errMarkedProxyServiceMissing) {
		return nil, err
	}

	return nil, nil
}

func (r *XDSReconciler) getOrCreateProxyService(ctx context.Context, desiredManagedPorts []corev1.ServicePort) (*corev1.Service, error) {
	if service, err := r.getProxyServiceByEnv(ctx); err == nil {
		return service, nil
	} else if !errors.Is(err, errProxyServiceNotConfigured) {
		return service, err
	}

	if service, err := r.getMarkedProxyService(ctx); err == nil {
		return service, nil
	} else if !errors.Is(err, errMarkedProxyServiceMissing) {
		return service, err
	}

	if service, err := r.createProxyServiceFromMarkedDeployment(ctx, desiredManagedPorts); err != nil || service != nil {
		return service, err
	}

	return nil, errors.New("unable to resolve xDS proxy service target")
}

func (r *XDSReconciler) getProxyServiceByEnv(ctx context.Context) (*corev1.Service, error) {
	serviceName := strings.TrimSpace(os.Getenv(xdsProxyServiceNameEnv))
	serviceNamespace := strings.TrimSpace(os.Getenv(xdsProxyServiceNamespaceEnv))
	if serviceName == "" || serviceNamespace == "" {
		return nil, errProxyServiceNotConfigured
	}

	service := &corev1.Service{}
	serviceKey := types.NamespacedName{Name: serviceName, Namespace: serviceNamespace}
	err := r.reader().Get(ctx, serviceKey, service)
	if err == nil {
		return service, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	service = &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: serviceNamespace,
			Labels: map[string]string{
				xdsProxyMarkerKey:     "true",
				LabelManagedByMdaiKey: LabelManagedByMdaiValue,
			},
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: parseLabelSelector(os.Getenv(xdsProxyServiceSelectorEnv)),
		},
	}
	if len(service.Spec.Selector) == 0 {
		return nil, fmt.Errorf(
			"proxy service %s/%s not found and %s is empty; cannot create service without selector",
			serviceNamespace, serviceName, xdsProxyServiceSelectorEnv,
		)
	}
	if err := r.Create(ctx, service); err != nil {
		return nil, err
	}
	return service, nil
}

func (r *XDSReconciler) getMarkedProxyService(ctx context.Context) (*corev1.Service, error) {
	var services corev1.ServiceList
	listOpts := []client.ListOption{client.MatchingLabels{xdsProxyMarkerKey: "true"}}
	if r.Namespace != "" {
		listOpts = append(listOpts, client.InNamespace(r.Namespace))
	}
	if err := r.reader().List(ctx, &services, listOpts...); err != nil {
		return nil, err
	}

	var matches []corev1.Service
	for _, service := range services.Items {
		matches = append(matches, service)
	}

	switch len(matches) {
	case 0:
		return nil, errMarkedProxyServiceMissing
	case 1:
		match := matches[0]
		return &match, nil
	default:
		return nil, fmt.Errorf("found %d services marked with %q=true; expected exactly one", len(matches), xdsProxyMarkerKey)
	}
}

func (r *XDSReconciler) createProxyServiceFromMarkedDeployment(ctx context.Context, desiredManagedPorts []corev1.ServicePort) (*corev1.Service, error) {
	var deployments appsv1.DeploymentList
	listOpts := []client.ListOption{client.MatchingLabels{xdsProxyMarkerKey: "true"}}
	if r.Namespace != "" {
		listOpts = append(listOpts, client.InNamespace(r.Namespace))
	}
	if err := r.reader().List(ctx, &deployments, listOpts...); err != nil {
		return nil, err
	}

	var matches []appsv1.Deployment
	for _, deployment := range deployments.Items {
		matches = append(matches, deployment)
	}

	switch len(matches) {
	case 0:
		return nil, fmt.Errorf(
			"no proxy service target found; set %s/%s, mark one Service with %q=true, or mark one Deployment with %q=true",
			xdsProxyServiceNameEnv,
			xdsProxyServiceNamespaceEnv,
			xdsProxyMarkerKey,
			xdsProxyMarkerKey,
		)
	case 1:
		deployment := matches[0]
		selector := deployment.Spec.Selector.MatchLabels
		if len(selector) == 0 {
			return nil, fmt.Errorf(
				"marked deployment %s/%s has empty matchLabels selector; cannot create proxy service",
				deployment.Namespace,
				deployment.Name,
			)
		}

		serviceNamespace := deployment.Namespace
		serviceName := deployment.Name + "-xds-proxy"

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceName,
				Namespace: serviceNamespace,
				Labels: map[string]string{
					xdsProxyMarkerKey:     "true",
					LabelManagedByMdaiKey: LabelManagedByMdaiValue,
				},
			},
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeClusterIP,
				Selector: selector,
				Ports:    desiredManagedPorts,
			},
		}

		if err := r.Create(ctx, service); err != nil {
			if apierrors.IsAlreadyExists(err) {
				existing := &corev1.Service{}
				if getErr := r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: serviceNamespace}, existing); getErr != nil {
					return nil, getErr
				}
				return existing, nil
			}
			return nil, err
		}
		return service, nil
	default:
		return nil, fmt.Errorf("found %d deployments marked with %q=true; expected exactly one", len(matches), xdsProxyMarkerKey)
	}
}

func (r *XDSReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func parseLabelSelector(raw string) map[string]string {
	result := map[string]string{}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return result
	}

	for part := range strings.SplitSeq(trimmed, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", splitNKeyValue)
		if len(kv) != splitNKeyValue {
			continue
		}
		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])
		if key == "" || value == "" {
			continue
		}
		result[key] = value
	}
	return result
}

func discoveredCollectorPorts(collectors []otelv1beta1.OpenTelemetryCollector) []uint32 {
	seen := make(map[uint32]struct{})

	for _, collector := range collectors {
		ports := extractPortsFromConfigForXDS(collector.Spec.Config)
		if len(ports) == 0 {
			ports = []uint32{defaultOTLPGRPCPort, defaultOTLPHTTPPort}
		}
		for _, p := range ports {
			seen[p] = struct{}{}
		}
	}

	result := make([]uint32, 0, len(seen))
	for p := range seen {
		result = append(result, p)
	}
	slices.Sort(result)
	return result
}

func managedServicePorts(ports []uint32) []corev1.ServicePort {
	result := make([]corev1.ServicePort, 0, len(ports))
	for _, port := range ports {
		if port > math.MaxInt32 {
			continue
		}
		portInt32 := int32(port)
		result = append(result, corev1.ServicePort{
			Name:       fmt.Sprintf("%s%d", xdsManagedServicePortPrefix, port),
			Port:       portInt32,
			TargetPort: intstr.FromInt32(portInt32),
			Protocol:   corev1.ProtocolTCP,
		})
	}
	return result
}

func mergeServicePorts(existing, managed []corev1.ServicePort) []corev1.ServicePort {
	result := make([]corev1.ServicePort, 0, len(existing)+len(managed))
	existingPorts := make(map[int32]struct{}, len(existing))
	for _, port := range existing {
		if strings.HasPrefix(port.Name, xdsManagedServicePortPrefix) {
			continue
		}
		result = append(result, port)
		existingPorts[port.Port] = struct{}{}
	}
	for _, port := range managed {
		if _, exists := existingPorts[port.Port]; exists {
			continue
		}
		result = append(result, port)
	}
	slices.SortFunc(result, func(a, b corev1.ServicePort) int {
		if a.Port < b.Port {
			return -1
		}
		if a.Port > b.Port {
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})
	return result
}

func servicePortsEqual(a, b []corev1.ServicePort) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name ||
			a[i].Port != b[i].Port ||
			a[i].Protocol != b[i].Protocol ||
			a[i].TargetPort != b[i].TargetPort {
			return false
		}
	}
	return true
}

func isWatchedCollectorObjectForXDS(obj client.Object) bool {
	if obj.GetLabels()[xdsRoleLabelKey] != connectionCollectorRole {
		return false
	}
	if obj.GetLabels()["hub.mydecisive.ai/shadow"] == "true" {
		return false
	}
	if obj.GetAnnotations()["hub.mydecisive.ai/shadow"] == "true" {
		return false
	}
	return !strings.HasSuffix(obj.GetName(), "-shadow")
}

func eligibleCollectorsForXDS(collectors []otelv1beta1.OpenTelemetryCollector) []otelv1beta1.OpenTelemetryCollector {
	filtered := make([]otelv1beta1.OpenTelemetryCollector, 0, len(collectors))
	for _, collector := range collectors {
		if collector.Labels[xdsRoleLabelKey] != connectionCollectorRole {
			continue
		}
		if xds.IsShadowCollector(collector) {
			continue
		}
		filtered = append(filtered, collector)
	}
	return filtered
}

func extractPortsFromConfigForXDS(config otelv1beta1.Config) []uint32 {
	ports := make([]uint32, 0)
	receivers := config.Receivers.Object

	for _, receiver := range receivers {
		receiverMap, ok := receiver.(map[string]any)
		if !ok {
			continue
		}

		if receiverEndpoint, ok := receiverMap["endpoint"].(string); ok {
			if port := extractPort(receiverEndpoint); port != 0 {
				ports = append(ports, port)
			}
		}

		if protocols, ok := receiverMap["protocols"].(map[string]any); ok {
			for _, protocol := range protocols {
				protocolMap, ok := protocol.(map[string]any)
				if !ok {
					continue
				}
				if protocolEndpoint, ok := protocolMap["endpoint"].(string); ok {
					if port := extractPort(protocolEndpoint); port != 0 {
						ports = append(ports, port)
					}
				}
			}
		}
	}

	return ports
}

func (r *XDSReconciler) collectorsWithReadyEndpoints(ctx context.Context, collectors []otelv1beta1.OpenTelemetryCollector) ([]otelv1beta1.OpenTelemetryCollector, error) {
	readyCollectors := make([]otelv1beta1.OpenTelemetryCollector, 0, len(collectors))

	for _, collector := range eligibleCollectorsForXDS(collectors) {
		requiredPorts := extractCollectorPortsForXDS(collector.Spec.Config)
		if len(requiredPorts) == 0 {
			requiredPorts = []uint32{defaultOTLPGRPCPort, defaultOTLPHTTPPort}
		}

		var endpointSlices discoveryv1.EndpointSliceList
		if err := r.reader().List(
			ctx,
			&endpointSlices,
			client.InNamespace(collector.Namespace),
			client.MatchingLabels{
				discoveryv1.LabelServiceName: collector.Name + "-collector",
			},
		); err != nil {
			return nil, err
		}

		readyPorts := readyEndpointPorts(endpointSlices.Items)
		if hasAllPorts(readyPorts, requiredPorts) {
			readyCollectors = append(readyCollectors, collector)
		}
	}

	return readyCollectors, nil
}

func extractCollectorPortsForXDS(config otelv1beta1.Config) []uint32 {
	return extractPortsFromConfigForXDS(config)
}

func readyEndpointPorts(endpointSlices []discoveryv1.EndpointSlice) map[uint32]struct{} {
	ready := make(map[uint32]struct{})
	for _, endpointSlice := range endpointSlices {
		for _, endpoint := range endpointSlice.Endpoints {
			if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready {
				continue
			}
			for _, port := range endpointSlice.Ports {
				if port.Port != nil && *port.Port > 0 {
					ready[uint32(*port.Port)] = struct{}{}
				}
			}
		}
	}
	return ready
}

func hasAllPorts(available map[uint32]struct{}, required []uint32) bool {
	for _, port := range required {
		if _, ok := available[port]; !ok {
			return false
		}
	}
	return true
}

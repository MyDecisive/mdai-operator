// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// nolint:mnd
package manifestutils

import (
	"regexp"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/mydecisive/mdai-operator/internal/naming"
)

func IsFilteredSet(sourceSet string, filterSet []string) bool {
	for _, basePattern := range filterSet {
		pattern, _ := regexp.Compile(basePattern)
		if match := pattern.MatchString(sourceSet); match {
			return match
		}
	}
	return false
}

// Labels return the common labels to all objects that are part of a managed CR.
func Labels(instance metav1.ObjectMeta, name string, image string, filterLabels []string) map[string]string {
	var versionLabel string
	// new map every time, so that we don't touch the instance's label
	base := map[string]string{}
	base["app.kubernetes.io/managed-by"] = "mdai-operator"

	if instance.Labels != nil {
		for k, v := range instance.Labels {
			if !IsFilteredSet(k, filterLabels) {
				base[k] = v
			}
		}
	}

	version := strings.Split(image, ":")
	for _, v := range version {
		if before, ok := strings.CutSuffix(v, "@sha256"); ok {
			versionLabel = before
		}
	}
	switch lenVersion := len(version); lenVersion {
	case 3:
		base["app.kubernetes.io/version"] = versionLabel
	case 2:
		base["app.kubernetes.io/version"] = naming.Truncate("%s", 63, version[len(version)-1])
	default:
		base["app.kubernetes.io/version"] = "latest"
	}

	// Don't override the app name if it already exists
	if _, ok := base["app.kubernetes.io/name"]; !ok {
		base["app.kubernetes.io/name"] = name
	}
	return base
}

// SelectorLabels return the common labels to all objects that are part of a managed CR to use as selector.
// Selector labels are immutable for Deployment, StatefulSet and DaemonSet, therefore, no labels in selector should be
// expected to be modified for the lifetime of the object.
func SelectorLabels(instance metav1.ObjectMeta, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by": "opentelemetry-operator",
		"app.kubernetes.io/instance":   naming.Truncate("%s.%s", 63, instance.Namespace, instance.Name),
		"app.kubernetes.io/component":  component,
	}
}

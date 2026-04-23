package utils

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"

	ginkgov2 "github.com/onsi/ginkgo/v2"
)

const (
	certmanagerVersion = "v1.17.0"
	certmanagerURLTmpl = "https://github.com/cert-manager/cert-manager/releases/download/%s/cert-manager.yaml"

	otelUrl = "https://github.com/open-telemetry/opentelemetry-operator/releases/latest/" +
		"download/opentelemetry-operator.yaml"
)

func warnError(err error) {
	ginkgov2.GinkgoWriter.Printf("warning: %v\n", err)
}

// Run executes the provided command within this context
func Run(cmd *exec.Cmd) (string, error) {
	dir, _ := GetProjectDir()
	cmd.Dir = dir

	if err := os.Chdir(cmd.Dir); err != nil {
		ginkgov2.GinkgoWriter.Printf("chdir dir: %s\n", err)
	}

	cmd.Env = append(os.Environ(), "GO111MODULE=on")
	command := strings.Join(cmd.Args, " ")
	ginkgov2.GinkgoWriter.Printf("running: %s\n", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%s failed with error: (%w) %s", command, err, string(output))
	}

	return string(output), nil
}

// UninstallCertManager uninstalls the cert manager
func UninstallCertManager() {
	url := fmt.Sprintf(certmanagerURLTmpl, certmanagerVersion)
	cmd := exec.Command("kubectl", "delete", "-f", url)
	if _, err := Run(cmd); err != nil {
		warnError(err)
	}
}

// InstallCertManager installs the cert manager bundle.
func InstallCertManager() error {
	url := fmt.Sprintf(certmanagerURLTmpl, certmanagerVersion)
	cmd := exec.Command("kubectl", "apply", "-f", url)
	if _, err := Run(cmd); err != nil {
		return err
	}
	// Wait for cert-manager-webhook to be ready, which can take time if cert-manager
	// was re-installed after uninstalling on a cluster.
	cmd = exec.Command("kubectl", "wait", "deployment.apps/cert-manager-webhook",
		"--for", "condition=Available",
		"--namespace", "cert-manager",
		"--timeout", "5m",
	)

	_, err := Run(cmd)
	return err
}

// IsCertManagerCRDsInstalled checks if any Cert Manager CRDs are installed
// by verifying the existence of key CRDs related to Cert Manager.
func IsCertManagerCRDsInstalled() bool {
	// List of common Cert Manager CRDs
	certManagerCRDs := []string{
		"certificates.cert-manager.io",
		"issuers.cert-manager.io",
		"clusterissuers.cert-manager.io",
		"certificaterequests.cert-manager.io",
		"orders.acme.cert-manager.io",
		"challenges.acme.cert-manager.io",
	}

	// Execute the kubectl command to get all CRDs
	cmd := exec.Command("kubectl", "get", "crds")
	output, err := Run(cmd)
	if err != nil {
		return false
	}

	// Check if any of the Cert Manager CRDs are present
	crdList := GetNonEmptyLines(output)
	for _, crd := range certManagerCRDs {
		for _, line := range crdList {
			if strings.Contains(line, crd) {
				return true
			}
		}
	}

	return false
}

// LoadImageToKindClusterWithName loads a local docker image to the kind cluster
func LoadImageToKindClusterWithName(name string) error {
	cluster := "kind"
	if v, ok := os.LookupEnv("KIND_CLUSTER"); ok {
		cluster = v
	}
	kindOptions := []string{"load", "docker-image", name, "--name", cluster}
	cmd := exec.Command("kind", kindOptions...)
	_, err := Run(cmd)
	return err
}

// GetNonEmptyLines converts given command output string into individual objects
// according to line breakers, and ignores the empty elements in it.
func GetNonEmptyLines(output string) []string {
	var res []string
	elements := strings.SplitSeq(output, "\n")
	for element := range elements {
		if element != "" {
			res = append(res, element)
		}
	}

	return res
}

// GetProjectDir will return the directory where the project is
func GetProjectDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return wd, err
	}
	wd = strings.ReplaceAll(wd, "/test/e2e", "")
	return wd, nil
}

// UncommentCode searches for target in the file and remove the comment prefix
// of the target content. The target content may span multiple lines.
func UncommentCode(filename, target, prefix string) error {
	// false positive
	// nolint:gosec
	content, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	strContent := string(content)

	idx := strings.Index(strContent, target)
	if idx < 0 {
		return fmt.Errorf("unable to find the code %s to be uncomment", target)
	}

	out := new(bytes.Buffer)
	_, err = out.Write(content[:idx])
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(bytes.NewBufferString(target))
	if !scanner.Scan() {
		return nil
	}
	for {
		_, err := out.WriteString(strings.TrimPrefix(scanner.Text(), prefix))
		if err != nil {
			return err
		}
		// Avoid writing a newline in case the previous line was the last in target.
		if !scanner.Scan() {
			break
		}
		if _, err := out.WriteString("\n"); err != nil {
			return err
		}
	}

	_, err = out.Write(content[idx+len(target):])
	if err != nil {
		return err
	}
	// false positive
	// nolint:gosec
	return os.WriteFile(filename, out.Bytes(), 0o644)
}

func InstallValkey() error {
	cmd := exec.Command("helm", "install", "valkey",
		"oci://registry-1.docker.io/bitnamicharts/valkey",
		"--set", "auth.password=abc",
		"--set", "image.registry=public.ecr.aws",
		"--set", "image.repository=decisiveai/valkey",
		"--set", "image.tag=latest",
		"-f", "test/test-samples/valkey-values.yaml",
	)
	_, err := Run(cmd)
	return err
}

func WaitForValkeyReady() error {
	cmd := exec.Command("kubectl", "rollout", "status", "statefulset/valkey-primary", "--timeout", "5m")
	_, err := Run(cmd)
	return err
}

func UninstallValkey() {
	cmd := exec.Command("helm", "uninstall", "valkey")
	if _, err := Run(cmd); err != nil {
		warnError(err)
	}
}

func InstallOtelOperator() error {
	cmd := exec.Command("kubectl", "apply", "-f", otelUrl)
	if _, err := Run(cmd); err != nil {
		return err
	}

	// Wait for otel-operator to be ready
	cmd = exec.Command("kubectl", "wait", "deployment.apps/opentelemetry-operator-controller-manager",
		"--for", "condition=Available",
		"--namespace", "opentelemetry-operator-system",
		"--timeout", "5m",
	)

	_, err := Run(cmd)
	return err
}

func UninstallOtelOperator() {
	cmd := exec.Command("kubectl", "delete", "namespace", "opentelemetry-operator-system")
	if _, err := Run(cmd); err != nil {
		warnError(err)
	}
}

func InstallPrometheusOperatorCRD() error {
	cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/external-crds/monitoring.coreos.com_prometheusrules.yaml")
	if _, err := Run(cmd); err != nil {
		return err
	}
	cmd = exec.Command("kubectl", "apply", "-f", "test/e2e/external-crds/servicemonitors.monitoring.coreos.com.yaml")
	if _, err := Run(cmd); err != nil {
		return err
	}
	return nil
}

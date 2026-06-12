# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

VERSION ?= $(shell git describe --tags --abbrev=0 2>/dev/null || echo "vdev")
VERSION := $(patsubst v%,%,$(VERSION))
GOTOOLCHAIN ?= go$(shell go list -m -f '{{.GoVersion}}')
GO := CGO_ENABLED=0 GOTOOLCHAIN=$(GOTOOLCHAIN) go
GO_TEST := $(GO) test -count=1

# Image URL to use all building/pushing image targets
IMG ?= ghcr.io/mydecisive/mdai-operator:$(if $(NEW_VERSION),$(NEW_VERSION),$(VERSION))

CHART_PATH := deployment

IS_CI ?= $(if $(CI),1,0)
vecho = $(if $(filter 0,$(IS_CI)),@echo $(1),@:)

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	@$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	@$(CONTROLLER_GEN) object paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	$(GO) fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	$(GO) vet ./...

TEST_EXCLUDE ?= /e2e|/test/utils|/cmd|/pkg/generated
TEST_PKGS = $$(go list ./... | grep -v -E "$(TEST_EXCLUDE)")
KUBEBUILDER_ENV = KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)"

.PHONY: test-coverage
test-coverage: manifests generate fmt vet envtest ## Run tests and generate code coverage.
	$(KUBEBUILDER_ENV) $(GO_TEST) $(TEST_PKGS) -coverprofile=coverage.out
	@sed '/zz_generated.deepcopy.go/d' coverage.out > coverage.tmp
	@mv coverage.tmp coverage.out

.PHONY: test
test: manifests generate fmt vet envtest ## Run tests
	$(KUBEBUILDER_ENV) $(GO_TEST) $(TEST_PKGS)

.PHONY: test-e2e
test-e2e: manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	@command -v kind >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@kind get clusters | grep -q 'kind' || { \
		echo "No Kind cluster is running. Please start a Kind cluster before running the e2e tests."; \
		exit 1; \
	}
	$(GO_TEST) ./test/e2e/ -v -ginkgo.v

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	$(GOLANGCI_LINT) config verify

.PHONY: fix
fix:
	$(GO) fix ./...

.PHONY: fix-diff
fix-diff:
	$(GO) fix -diff ./...

.PHONY: tidy
tidy:
	@$(GO) mod tidy

.PHONY: vendor
vendor:
	@$(GO) mod vendor

.PHONY: tidy-check
tidy-check:
	@$(GO) mod tidy -diff || { echo >&2 "go.mod or go.sum is out of sync. Run 'make tidy'."; exit 1; }

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	$(GO) build -trimpath -ldflags="-w -s" -o bin/manager ./cmd

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	$(GO) run ./cmd

# buildx is the standard path for both local and multi-arch image builds.
# Local builds are loaded into the Docker image store; multi-arch builds are pushed.
BUILDX_BUILDER ?= mdai-operator-builder
.PHONY: docker-login 
docker-login: 
	aws ecr-public get-login-password | $(CONTAINER_TOOL) login --username AWS --password-stdin public.ecr.aws/decisiveai 

.PHONY: docker-buildx-ensure-builder
docker-buildx-ensure-builder:
	@$(CONTAINER_TOOL) buildx inspect $(BUILDX_BUILDER) >/dev/null 2>&1 || \
		$(CONTAINER_TOOL) buildx create --name $(BUILDX_BUILDER) --driver docker-container
	@$(CONTAINER_TOOL) buildx use $(BUILDX_BUILDER)

.PHONY: docker-build
docker-build: docker-buildx-ensure-builder ## Build docker image with the manager.
	$(CONTAINER_TOOL) buildx build --load --platform=$(LOCAL_PLATFORM) --tag ${IMG} .

.PHONY: docker-push
docker-push: docker-buildx-ensure-builder ## Build and push docker image for the manager for cross-platform support.
	$(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} .

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
LOCAL_PLATFORM ?= linux/$(shell go env GOARCH)
PLATFORMS ?= linux/arm64,linux/amd64
.PHONY: docker-buildx
docker-buildx: docker-push ## Backward-compatible alias for multi-arch push via buildx.

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	@tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT; \
	cp -r config "$$tmp/"; \
	cd "$$tmp/config/manager" && $(KUSTOMIZE) edit set image "controller=${IMG}"; \
	$(KUSTOMIZE) build "$$tmp/config/default" > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	@tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT; \
	cp -r config "$$tmp/"; \
	cd "$$tmp/config/manager" && $(KUSTOMIZE) edit set image "controller=$(IMG)"; \
	$(KUSTOMIZE) build "$$tmp/config/default" | $(KUBECTL) apply -f -
	$(KUBECTL) -n mdai rollout status deployment/mdai-operator-controller-manager

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/default | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBECTL ?= kubectl
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint
HELMIFY ?= $(LOCALBIN)/helmify
HELM_DOCS = $(LOCALBIN)/helm-docs
HELM ?= $(LOCALBIN)/helm
HELM_PLUGINS ?= $(LOCALBIN)/helm-plugins
export HELM_PLUGINS
YQ ?= $(LOCALBIN)/yq
UNAME := $(shell uname -s)

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.0
CONTROLLER_TOOLS_VERSION ?= v0.19.0
#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell go list -m -f "{{ .Version }}" sigs.k8s.io/controller-runtime | awk -F'[v.]' '{printf "release-%d.%d", $$2, $$3}')
#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell go list -m -f "{{ .Version }}" k8s.io/api | awk -F'[v.]' '{printf "1.%d", $$3}')
GOLANGCI_LINT_VERSION ?= v2.11.4
HELMIFY_VERSION ?= e57c93d0641d5699967d861dbc055b908d6c671f
HELM_DOCS_VERSION ?= v1.14.2
HELM_VERSION ?= v3.19.4
YQ_VERSION ?= v4.45.4

YQ_VERSIONED := $(YQ)-$(YQ_VERSION)

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

.PHONY: helmify
helmify: $(HELMIFY) ## Download helmify locally if necessary.
$(HELMIFY): $(LOCALBIN)
	$(call go-install-tool,$(HELMIFY),github.com/arttor/helmify/cmd/helmify,$(HELMIFY_VERSION))

.PHONY: helm-docs
helm-docs: $(HELM_DOCS) ## Download helm-docs locally if necessary.
$(HELM_DOCS): $(LOCALBIN)
	$(call go-install-tool,$(HELM_DOCS),github.com/norwoodj/helm-docs/cmd/helm-docs,$(HELM_DOCS_VERSION))

$(HELM): $(LOCALBIN)
	$(call go-install-tool,$(HELM),helm.sh/helm/v3/cmd/helm,$(HELM_VERSION))

.PHONY: helm-values-schema-json-plugin
helm-values-schema-json-plugin: $(HELM)
	@mkdir -p $(HELM_PLUGINS)
	@$(HELM) plugin list | grep -q '^schema' || \
		$(HELM) plugin install https://github.com/losisin/helm-values-schema-json.git

.PHONY: yq
yq:
	@if [ ! -f "$(YQ_VERSIONED)" ]; then \
		OS=$$(uname | tr '[:upper:]' '[:lower:]') && \
		ARCH=$$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/') && \
		URL=$$(printf "https://github.com/mikefarah/yq/releases/download/$(YQ_VERSION)/yq_%s_%s" $$OS $$ARCH) && \
		curl -sSLf "$$URL" -o "$(YQ_VERSIONED)" && \
		chmod +x "$(YQ_VERSIONED)"; \
	fi
	@ln -sf $(YQ_VERSIONED) $(YQ)

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef

.PHONY: local-deploy
local-deploy: IMG=mdai-operator:${VERSION}
local-deploy: tidy vendor generate manifests lint helm-update install
	$(MAKE) docker-build IMG=$(IMG)
	kind load docker-image $(IMG) --name mdai
	$(MAKE) deploy IMG=$(IMG)

##@ Release

.PHONY: fetch-tags
fetch-tags: ## Fetch all tags from remote.
	@git fetch --tags --force

.PHONY: release
release: fetch-tags ## Prepare a release. Usage: make release NEW_VERSION=x.y.z
	@[ -n "$(NEW_VERSION)" ] || { echo "Error: NEW_VERSION is required. Usage: make release NEW_VERSION=x.y.z"; exit 1; }
	@$(MAKE) helm-update NEW_VERSION=$(NEW_VERSION)
	@echo ""
	@echo "Release v$(NEW_VERSION) prepared. Next steps:"
	@echo "  1. Commit the changes and open a PR"
	@echo "  2. After merge to main: git tag v$(NEW_VERSION) && git push origin v$(NEW_VERSION)"

.PHONY: bump-version
bump-version: yq ## Bump version in chart and kustomization. No-op if NEW_VERSION is unset.
	@if [ -n "$(NEW_VERSION)" ]; then \
		echo "Bumping version $(VERSION) → $(NEW_VERSION)"; \
		$(YQ) -i '(.images[] | select(.name == "controller")).newTag = "$(NEW_VERSION)"' config/manager/kustomization.yaml; \
		$(YQ) -i '.version = "$(NEW_VERSION)" | .appVersion = "$(NEW_VERSION)"' $(CHART_PATH)/Chart.yaml; \
	fi

CHART_VERSION ?= $(VERSION)
CHART_DIR := ./deployment
CHART_NAME := mdai-operator
CHART_PACKAGE := $(CHART_NAME)-$(CHART_VERSION).tgz
.PHONY: helm
helm:
	@echo "Usage: make helm-<command>"
	@echo "Available commands:"
	@echo "  helm-update    Update the Helm chart (versions, images, etc)"
	@echo "  helm-package   Package the Helm chart"

.PHONY: helm-update
helm-update: HELMIFY_ARGS="-optional-crds"
helm-update: bump-version manifests kustomize helmify helm-docs helm-values-schema-json-plugin yq
	$(call vecho,"🐳 Updating image to $(IMG)...")
	@pushd config/manager > /dev/null && $(KUSTOMIZE) edit set image controller=$(IMG) && popd > /dev/null
	$(call vecho,"🛠️ Kustomizing and Helmifying...")
	@$(KUSTOMIZE) build config/default | $(HELMIFY) $(HELMIFY_ARGS) $(CHART_PATH) > /dev/null 2>&1
	$(call vecho,"🛠️ Wrapping xds service template...")
	@$(CHART_PATH)/files/wrap_xds_service.sh
	$(call vecho,"🛠️ Enabling xds service by default...")
	@$(YQ) -i '.xdsService.enabled = true' $(CHART_PATH)/values.yaml
	$(call vecho,"🛠️ Adding conditionals for cert manager...")
	@$(CHART_PATH)/files/no_cert_manager_option.sh
	$(call vecho,"🧩 Removing hardcoded tag version...")
	@$(YQ) -i 'del(.controllerManager.manager.image.tag)' $(CHART_PATH)/values.yaml
	$(call vecho,"📝 Updating Helm chart docs...")
	@$(HELM_DOCS) --skip-version-footer $(CHART_PATH) -f values.yaml -l warning
	$(call vecho,"📐 Updating Helm chart JSON schema...")
	@$(HELM) schema --values $(CHART_PATH)/values.yaml --output $(CHART_PATH)/values.schema.json

.PHONY: helm-package
helm-package: helm-update
	$(call vecho, "📦 Packaging Helm chart...")
	@$(HELM) package -u --version $(CHART_VERSION) --app-version $(CHART_VERSION) $(CHART_DIR) > /dev/null


.PHONY: generate-clientset
generate-clientset:
	./hack/update-codegen.sh

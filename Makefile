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

# Update this version to match new release tag and run helm targets
VERSION = 0.2.11

# Image URL to use all building/pushing image targets
IMG ?= public.ecr.aws/p3k6k6h3/mdai-operator:${VERSION}

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
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test-coverage
test-coverage: manifests generate fmt vet envtest ## Run tests and generate code coverage.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test $$(go list ./... | grep -v -E "/e2e|/test/utils|cmd|pkg/generated") -coverprofile=coverage.txt 
	@sed '/zz_generated.deepcopy.go/d' coverage.txt > coverage.tmp
	@mv coverage.tmp coverage.txt

.PHONY: test
test: manifests generate fmt vet envtest ## Run tests and generate code coverage.
	# Run Go tests and produce coverage report
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test $$(go list ./... | grep -v /e2e) -count=1 -coverprofile cover.txt

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
	go test ./test/e2e/ -v -ginkgo.v

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	$(GOLANGCI_LINT) config verify

.PHONY: tidy
tidy:
	@go mod tidy

.PHONY: vendor
vendor:
	@go mod vendor

.PHONY: tidy-check
tidy-check: tidy
	@git diff --quiet --exit-code go.mod go.sum || { echo >&2 "go.mod or go.sum is out of sync. Run 'make tidy'."; exit 1; }

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager ./cmd

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name mdai-operator-builder
	$(CONTAINER_TOOL) buildx use mdai-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm mdai-operator-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	@if [ -d "config/crd" ]; then \
		$(KUSTOMIZE) build config/crd > dist/install.yaml; \
	fi
	echo "---" >> dist/install.yaml  # Add a document separator before appending
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default > dist/install.yaml

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
	cp -r config $$tmp/; \
	cd $$tmp/config/manager && $(KUSTOMIZE) edit set image controller=$(IMG); \
	$(KUSTOMIZE) build $$tmp/config/default | $(KUBECTL) apply -f -; \
	rm -rf $$tmp
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
GOLANGCI_LINT_VERSION ?= v2.8
HELMIFY_VERSION ?= v0.4.19
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
local-deploy: manifests install
	go mod tidy
	go mod vendor
	make manifests
	make generate
	make lint
	make helm-update
	make docker-build IMG=mdai-operator:${VERSION}
	kind load docker-image mdai-operator:${VERSION} --name mdai
	make deploy IMG=mdai-operator:${VERSION}
	kubectl rollout restart deployment mdai-operator-controller-manager -n mdai

LATEST_TAG := $(shell git describe --tags --abbrev=0 $(git rev-parse --abbrev-ref HEAD) | sed 's/^v//')
CHART_VERSION ?= $(LATEST_TAG)
CHART_DIR := ./deployment
CHART_NAME := mdai-operator
CHART_PACKAGE := $(CHART_NAME)-$(CHART_VERSION).tgz
CHART_REPO := git@github.com:MyDecisive/mdai-helm-charts.git
BASE_BRANCH := gh-pages
TARGET_BRANCH := $(CHART_NAME)-v$(CHART_VERSION)
CLONE_DIR := $(shell mktemp -d /tmp/mdai-helm-charts.XXXXXX)
REPO_DIR := $(shell pwd)

.PHONY: helm
helm:
	@echo "Usage: make helm-<command>"
	@echo "Available commands:"
	@echo "  helm-update    Update the Helm chart (versions, images, etc)"
	@echo "  helm-package   Package the Helm chart"
	@echo "  helm-publish   Publish the Helm chart"

.PHONY: helm-update
helm-update: manifests kustomize helmify helm-docs helm-values-schema-json-plugin yq
	$(call vecho,"🐳 Updating image to $(IMG)...")
	@pushd config/manager > /dev/null && $(KUSTOMIZE) edit set image controller=$(IMG) && popd > /dev/null
	$(call vecho,"🛠️ Kustomizing and Helmifying...")
	@$(KUSTOMIZE) build config/default | $(HELMIFY) $(CHART_PATH) > /dev/null 2>&1
	$(call vecho,"🛠️ Adding conditionals for cert manager...")
	@$(CHART_PATH)/files/no_cert_manager_option.sh
	$(call vecho,"🛠️ Adding conditionals for CRDs...")
	@$(CHART_PATH)/files/wrap_crds.sh
	$(call vecho,"📈 Updating Helm chart version to $(VERSION)...")
	@$(YQ) -i '.version = "$(VERSION)"' $(CHART_PATH)/Chart.yaml
	$(call vecho,"🧩 Updating Helm chart appVersion to $(VERSION)...")
	@$(YQ) -i '.appVersion = "$(VERSION)"' $(CHART_PATH)/Chart.yaml
	$(call vecho,"📝 Updating Helm chart docs...")
	@$(HELM_DOCS) --skip-version-footer $(CHART_PATH) -f values.yaml -l warning
	$(call vecho,"📐 Updating Helm chart JSON schema...")
	@$(HELM) schema --values $(CHART_PATH)/values.yaml --output $(CHART_PATH)/values.schema.json

.PHONY: helm-package
helm-package: helm-update
	$(call vecho, "📦 Packaging Helm chart...")
	@$(HELM) package -u --version $(CHART_VERSION) --app-version $(CHART_VERSION) $(CHART_DIR) > /dev/null

.PHONY: helm-publish
helm-publish: helm-package
	$(call vecho,"🚀 Cloning $(CHART_REPO)...")
	@rm -rf $(CLONE_DIR)
	@git clone -q --branch $(BASE_BRANCH) $(CHART_REPO) $(CLONE_DIR)

	$(call vecho,"🌿 Creating branch $(TARGET_BRANCH) from $(BASE_BRANCH)...")
	@cd $(CLONE_DIR) && git checkout -q -b $(TARGET_BRANCH)

	$(call vecho,"📤 Copying and indexing chart...")
	@cd $(CLONE_DIR) && \
		$(HELM) repo index $(REPO_DIR) --merge index.yaml && \
		mv $(REPO_DIR)/$(CHART_PACKAGE) $(CLONE_DIR)/ && \
		mv $(REPO_DIR)/index.yaml $(CLONE_DIR)/

	$(call vecho,"🚀 Committing changes...")
	@cd $(CLONE_DIR) && \
		git add $(CHART_PACKAGE) index.yaml && \
		git commit -q -m "chore: publish $(CHART_PACKAGE)" && \
		git push -q origin $(TARGET_BRANCH) && \
		rm -rf $(CLONE_DIR)

	$(call vecho,"✅ Chart published")

.PHONY: generate-clientset
generate-clientset:
	./hack/update-codegen.sh

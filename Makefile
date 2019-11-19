# variable definitions
NAME := lemur
DESC := Lustre HSM Agent and Movers
PREFIX ?= $(PWD)/usr/local
BUILDROOT ?=
VERSION := $(shell git describe --tags --always --dirty | tr '-' '_')
BUILDDATE := $(shell date -u +"%B %d, %Y")
GOVERSION := $(shell go version)
PKG_RELEASE ?= 1
PROJECT_URL := "https://github.com/edwardsp/$(NAME)"
LDFLAGS := -X 'main.version=$(VERSION)'

CMD_SOURCES := $(shell find cmd -name main.go)

TARGETS := $(patsubst cmd/%/main.go,%,$(CMD_SOURCES))
RACE_TARGETS := $(patsubst cmd/%/main.go,%.race,$(CMD_SOURCES))
PANDOC_BIN := $(shell if which pandoc >/dev/null 2>&1; then echo pandoc; else echo true; fi)

$(TARGETS):
	go build -v -i -ldflags "$(LDFLAGS)" -o $@ ./cmd/$@

$(RACE_TARGETS):
	go build -v -i -ldflags "$(LDFLAGS)" --race -o $@ ./cmd/$(basename $@)

# build tasks
rpm: docker-rpm
docker-rpm: docker
	rm -fr $(CURDIR)/output
	mkdir -p $(CURDIR)/output/{BUILD,BUILDROOT,RPMS/{noarch,x86_64},SPECS,SRPMS}
	docker run --rm -v $(CURDIR):/source:z -v $(CURDIR)/output:/root/rpmbuild:z lemur-rpm-build

local-rpm:
	$(MAKE) -C packaging/rpm NAME=$(NAME) VERSION=$(VERSION) RELEASE=$(PKG_RELEASE) URL=$(PROJECT_URL)

docker:
	$(MAKE) -C packaging/docker

vendor:
	$(MAKE) -C vendor

# development tasks
coverage:
	@-go test -v -coverprofile=cover.out $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)
	@-go tool cover -html=cover.out -o cover.html

benchmark:
	@echo "Running tests..."
	@go test -bench=. $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)

all: lint $(TARGETS) 
.DEFAULT_GOAL:=all

# Installation
INSTALLED_TARGETS = $(addprefix $(PREFIX)/bin/, $(TARGETS))
# test targets
UAT_RACE_TARGETS_DEST := libexec/$(NAME)-testing
INSTALLED_RACE_TARGETS = $(addprefix $(PREFIX)/$(UAT_RACE_TARGETS_DEST)/, $(RACE_TARGETS))
UAT_FEATURES_DEST := share/$(NAME)/test/features
INSTALLED_FEATURES = $(addprefix $(PREFIX)/$(UAT_FEATURES_DEST)/, $(FEATURE_FILES))

# Sample config files
#
EXAMPLES = $(shell find doc -name "*.example")
EXAMPLE_TARGETS = $(patsubst doc/%,%,$(EXAMPLES))
INSTALLED_EXAMPLES = $(addprefix $(PREFIX)/etc/lhsmd/, $(EXAMPLE_TARGETS))

# Cleanliness...
lint:
	@ln -sf vendor src
	git rev-parse HEAD
	GOPATH=$(PWD):$(GOPATH) gometalinter -j2 --vendor -D gotype -D errcheck -D dupl -D gocyclo --deadline 60s ./... --exclude pdm/
	@rm src

# install tasks
$(PREFIX)/bin/%: %
	install -d $$(dirname $@)
	install -m 755 $< $@

$(PREFIX)/$(UAT_FEATURES_DEST)/%: $(FEATURE_TESTS)/%
	install -d $$(dirname $@)
	install -m 644 $< $@

$(PREFIX)/$(UAT_RACE_TARGETS_DEST)/%: %
	install -d $$(dirname $@)
	install -m 755 $< $@

$(PREFIX)/etc/lhsmd/%:
	install -d $$(dirname $@)
	install -m 644 doc/$$(basename $@) $@

install-example: $(INSTALLED_EXAMPLES)

install: $(INSTALLED_TARGETS) $(INSTALLED_MAN_TARGETS)

local-install:
	$(MAKE) install PREFIX=usr/local


# clean up tasks
clean-docs:
	rm -rf ./docs

clean-deps:
	rm -rf $(DEPDIR)

clean: clean-docs clean-deps
	rm -rf ./usr
	rm -f $(TARGETS)
	rm -f $(RACE_TARGETS)
	rm -f $(MAN_TARGETS)

.PHONY: $(TARGETS) $(RACE_TARGETS)
.PHONY: all check test rpm deb install local-install packages  coverage docs jekyll deploy-docs clean-docs clean-deps clean vendor

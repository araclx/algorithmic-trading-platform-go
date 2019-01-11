# Copyright 2018 REKTRA Network, All Rights Reserved.

TAG = test-latest
ORGANIZATION = rektranetwork
PRODUCT = trekt
GO_VER = 1.11.2
NODE_OS_NAME = alpine
NODE_OS_TAG = 3.8

IMAGE_TAG_ACCESSPOINT = $(ORGANIZATION)/$(PRODUCT).accesspoint:$(TAG)
IMAGE_TAG_AUTH = $(ORGANIZATION)/$(PRODUCT).auth:$(TAG)
IMAGE_TAG_EXCHANGE_BINANCE = $(ORGANIZATION)/$(PRODUCT).exchange.binance:$(TAG)

THIS_FILE := $(lastword $(MAKEFILE_LIST))

.DEFAULT_GOAL := help

define build_docker_cmd_image
	docker build \
		--rm \
		--build-arg GO_VER=$(GO_VER) \
		--build-arg NODE_OS_NAME=$(NODE_OS_NAME) \
		--build-arg NODE_OS_TAG=$(NODE_OS_TAG) \
		--file "$(CURDIR)/cmd/$(1)/Dockerfile" \
		--tag $(2) \
		./
endef

define push_docker_cmd_image
	docker push $(1)
endef

define make_target
	$(MAKE) -f $(THIS_FILE) $(1)
endef


.PHONY: help build build-accesspoint build-auth	release release-accesspoint release-auth


help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'


build-accesspoint: ## Build access point node docker image from actual local sources.
	@$(call build_docker_cmd_image,scrtr-accesspoint,$(IMAGE_TAG_ACCESSPOINT))

build-auth: ## Build auth-node docker image from actual local sources.
	@$(call build_docker_cmd_image,trekt-auth,$(IMAGE_TAG_AUTH))

build-binance: ## Build Binance connector docker image from actual local sources.
	@$(call build_docker_cmd_image,trekt-exchange-binance,$(IMAGE_TAG_EXCHANGE_BINANCE))

build: ## Build all docker images from actual local sources.
	$(call make_target,build-accesspoint)
	$(call make_target,build-auth)
	$(call make_target,build-binance)


release-accesspoint: ## Push access point node image to the hub.
	@$(call push_docker_cmd_image,$(IMAGE_TAG_ACCESSPOINT))

release-auth: ## Push auth-node image to the hub.
	@$(call push_docker_cmd_image,$(IMAGE_TAG_AUTH))

release-binance: ## Push Binance exchange connector image to the hub.
	@$(call push_docker_cmd_image,$(IMAGE_TAG_EXCHANGE_BINANCE))

release: ## Push all images on the hub.
	@$(call make_target,release-accesspoint)
	@$(call make_target,release-auth)
	@$(call make_target,release-binance)

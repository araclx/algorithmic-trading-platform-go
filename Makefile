TAG				= test-latest
ORGANIZATION	= rektranetwork
PRODUCT			= trekt
REPO 			= 
GO_VER			= 1.11.2
NODE_OS_NAME	= alpine
NODE_OS_TAG		= 3.8

IMAGE_TAG_ACCESSPOINT = $(ORGANIZATION)/$(PRODUCT).accesspoint:$(TAG)

.DEFAULT_GOAL := help

help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

build: ## Build docker images from actual local sources. 
	@docker build \
		--rm \
		--build-arg GO_VER=$(GO_VER) \
		--build-arg NODE_OS_NAME=$(NODE_OS_NAME) \
		--build-arg NODE_OS_TAG=$(NODE_OS_TAG) \
		--file "$(CURDIR)/cmd/scrtr-accesspoint/Dockerfile" \
		--tag $(IMAGE_TAG_ACCESSPOINT) \
		./

release: ## Push images on the hub.
	docker push $(IMAGE_TAG_ACCESSPOINT)

.PHONY: build
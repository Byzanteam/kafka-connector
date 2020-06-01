TAG?=latest-dev
NAMESPACE?=registry.cn-hangzhou.aliyuncs.com/byzanteam
.PHONY: build

build:
	./build.sh $(TAG) $(NAMESPACE)

ci-armhf-build:
	./build.sh $(TAG) $(NAMESPACE)

ci-armhf-push:
	./build.sh $(TAG) $(NAMESPACE)

push:
	./push.sh $(TAG) $(NAMESPACE)




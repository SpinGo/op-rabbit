.PHONY: docs-core

docs-core: core/target/docs-published

core/target/docs-published: core/target/scala-2.11/api/index.html
	aws s3 sync core/target/scala-2.11/api s3://spingo-oss/docs/op-rabbit/core/current --delete
	touch $@

.PHONY: docs-core docs-addons-json4s docs docs-addons-play-json

core/target/docs-published: core/target/scala-2.11/api/index.html
	aws s3 sync core/target/scala-2.11/api s3://spingo-oss/docs/op-rabbit/core/current --delete
	touch $@

addons/%/target/docs-published: addons/%/target/scala-2.11/api/index.html
	aws s3 sync addons/$*/target/scala-2.11/api s3://spingo-oss/docs/op-rabbit/$*/current --delete
	touch $@

docs-core: core/target/docs-published
docs-json4s: addons/json4s/target/docs-published
docs-play-json: addons/play-json/target/docs-published

prev-%: addons/%/target/scala-2.11/api/index.html
	open addons/$*/target/scala-2.11/api/index.html

docs: docs-json4s docs-core docs-play-json

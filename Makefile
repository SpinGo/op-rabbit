.PHONY: docs-core docs-addons-json4s docs docs-addons-play-json clean
.SECONDARY:

core/target/scala-2.11/api/index.html:
	sbt doc

addons/%/target/scala-2.11/api/index.html: core/target/scala-2.11/api/index.html
	[ -f $@ ]

core/target/docs-published: core/target/scala-2.11/api/index.html
	aws s3 sync core/target/scala-2.11/api s3://spingo-oss/docs/op-rabbit/core/current --delete
	touch $@

addons/%/target/docs-published: addons/%/target/scala-2.11/api/index.html
	aws s3 sync addons/$*/target/scala-2.11/api s3://spingo-oss/docs/op-rabbit/$*/current --delete
	touch $@

README.md: project/version.properties
	bin/update-version
prev-%: addons/%/target/scala-2.11/api/index.html
	open addons/$*/target/scala-2.11/api/index.html

docs: README.md core/target/docs-published addons/json4s/target/docs-published addons/airbrake/target/docs-published addons/play-json/target/docs-published addons/akka-stream/target/docs-published

clean:
	rm -rf {addons/*,core}/target/scala-2.11/api {addons/*,core}/target/docs-published

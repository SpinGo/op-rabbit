.PHONY: docs clean preview publish
.SECONDARY:

target/scala-2.11/unidoc/index.html:
	sbt unidoc

target/docs-published: target/scala-2.11/unidoc/index.html
	aws s3 sync target/scala-2.11/unidoc/ s3://spingo-oss/docs/op-rabbit/current/ --acl public-read --delete
	touch $@

README.md: project/version.properties
	bin/update-version

preview: target/scala-2.11/unidoc/index.html
	open target/scala-2.11/unidoc/index.html

docs: README.md target/scala-2.11/unidoc/index.html

publish: target/docs-published

clean:
	rm -rf target/docs-published target/scala-2.11/unidoc

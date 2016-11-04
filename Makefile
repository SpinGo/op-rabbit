.PHONY: docs clean preview publish
.SECONDARY:

target/scala-2.11/unidoc/index.html:
	sbt unidoc

target/docs-published: target/scala-2.11/unidoc/index.html
	rsync -av target/scala-2.11/unidoc/ ../op-rabbit.github.io/docs/
	cd ../op-rabbit.github.io/docs/; git add . -A; git commit -m "update docs"; git push
	touch $@

README.md: project/version.properties
	bin/update-version

preview: target/scala-2.11/unidoc/index.html
	open target/scala-2.11/unidoc/index.html

docs: README.md target/scala-2.11/unidoc/index.html

publish: target/docs-published

clean:
	rm -rf target/docs-published target/scala-2.11/unidoc

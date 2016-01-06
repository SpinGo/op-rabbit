- Edit project/version.properties

Run the following:

    . project/version.properties
    make README.md
    make clean
    sbt +test
    sbt +publishSigned
    sbt +akka-stream-2/publishSigned
    make docs publish
    git add README.md project/version.properties
    git commit -m v$version
    git tag v$version
    git push origin v$version master

Presently, you need to comment out the `akka-stream-2-M2` block when running `make docs publish`, as referring to the same source tree twice introduces compile errors for unidoc.

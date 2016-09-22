- Edit project/version.properties

- Edit README.md; note the appropriate stable version as appropriate.

- Make sure when making changes to stable master, merge them into the corresponding release branches.

Run the following:

    . project/version.properties
    make README.md
    make clean
    sbt test
    sbt publishSigned
    make docs publish
    git add README.md project/version.properties
    git commit -m v$version
    git tag v$version
    git push origin v$version master

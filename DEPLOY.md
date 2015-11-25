- Edit project/version.properties

Run the following:

    . project/version.properties
    make README.md
    make clean
    sbt +test
    sbt +publishSigned
    make docs publish
    git add README.md project/version.properties
    git commit -m v$version
    git tag v$version
    git push origin v$version master

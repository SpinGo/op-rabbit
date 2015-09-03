# Running the test suite

In order to run the test-suite, you should have an instance of `rabbitmq` running on your local machine. (you can use a remote instance, too). You'll need to set your configuration in `core/src/test/resources/application.conf`. Default configuration can be copied, as follows:

    cp core/src/test/resources/application.conf.default core/src/test/resources/application.conf

The entire suite (including all modules), is run by issuing the following:

    sbt test

To test a single module, such as `akka-stream`, run the following:

    sbt akka-stream/test


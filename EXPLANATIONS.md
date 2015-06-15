# Why use akka-rabbitmq? Why not use the Java JVM client auto-recovery features?

This is something I considered doing, deeply. Half-way through the process of switching from using `akka-rabbitmq` to the Java client for connection recovery, I started to see some areas in which the JVM client was insufficient on it's own. If my understanding is incorrect, please correct me, and I will update this list accordingly.

## When creating a channel synchronously, exceptions become the problem of module creating the channel. This leads to increased distribution of error handling logic.

When creating a subscription actor, it requires a channel. If you allocate a channel, and the connection is momentarily unavailable (due to network issue or other case), error handling and recovery now becomes the responsibility of the module asking for a channel. The actor can now:

A) Crash, get rebooted after a time, and try again.

  - This would work fine, but there are impliciations in rebooting child actors depend on that state.

B) Try and re-establish it's connection periodically. Stash all messages until connected.

Also, questions like this made me nervous about the reliability of topology recovery: https://groups.google.com/forum/#!topic/clojure-rabbitmq/PBFF6ol_150 . Admittedly, this could be FUD, and I'm happy to be challenged on it. Ultimately, when waying and balancing the choices above, I felt it simplest to separate the concern of both fault tolerance when connecting to RabbitMQ and fault tolerance when recovering from lost connections. `Akka-rabbitmq` provides an elegant solution for both of these concerns. My subscription actor concerns are reduced to "I need a channel. Hey, connection actor, give me a channel". The topology is declared independent of current connection state such that it will converge towards correctness as the network becomes available, without needing to restart the entire JVM.

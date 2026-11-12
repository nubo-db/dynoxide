# Roadmap

What's being worked on, what's likely next, and what Dynoxide won't do. It
changes as things ship and as people ask for things, so treat it as direction
rather than dates. If something here matters to you, or something missing
does, say so in an [issue](https://github.com/nubo-db/dynoxide/issues).

## Now: 1.7.0

Starting Dynoxide from a test suite, in any language, without hand-rolled
process code.

- **A launcher contract.** One written description of how a wrapper starts
  Dynoxide, learns its address and stops it, so every language behaves the
  same way. The engine gains the two flags it needs: a machine-readable ready
  line, and shutting down when the process that started it goes away.
- **`start()` in the npm package.** Run Dynoxide on a free port from a Node
  test suite and get back its endpoint and a `stop()`.
- **A PHP package.** `nubo-db/dynoxide` on Packagist downloads and verifies the
  binary, starts it, and comes with a PHPUnit extension (Pest and ParaTest
  included).

## Next

- **A Python launcher** on PyPI, built on the same contract.
- **Laravel support**, on top of the PHP package. A
  [recipe](docs/laravel.md) covers the cache, locks, sessions and failed jobs
  today.
- **Guides for Symfony and Bref**, and for Architect's sandbox.
- **Closing the remaining differences from DynamoDB.** The open
  [bug issues](https://github.com/nubo-db/dynoxide/issues?q=is%3Aopen+label%3Abug)
  are each a captured case where Dynoxide answers differently, and they're
  worked through steadily, measured by the
  [conformance suite](https://paritysuite.org).

## Later, or open to help

- **JVM launchers for Maven, Gradle and JUnit.** Scala builds are covered
  already by Hapag-Lloyd's [sbt-dynoxide](https://github.com/Hapag-Lloyd/sbt-dynoxide)
  and [dynoxide-scala-core](https://github.com/Hapag-Lloyd/dynoxide-scala-core).
  Plain Java is open: if you'd use or build it, open an issue.
- **[Other storage backends](https://github.com/nubo-db/dynoxide/issues/90)**,
  under discussion.
- **[More detailed logging](https://github.com/nubo-db/dynoxide/issues/14)**
  for seeing what a test suite sent.

## Not planned

- **Running in production.** Dynoxide is for development, tests and CI.
- **Cloud-only operations** such as backups, global tables and Kinesis
  streaming. They return `UnknownOperationException`; see
  [Differences from DynamoDB](docs/differences-from-dynamodb.md).
- **A web console.** Any DynamoDB client or GUI pointed at the endpoint can
  browse and edit the data.

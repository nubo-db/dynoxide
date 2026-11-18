# Roadmap

What's being worked on, what's likely next, and what Dynoxide won't do. It
changes as things ship and as people ask for things, so treat it as direction
rather than dates. If something here matters to you, or something missing
does, say so in an [issue](https://github.com/nubo-db/dynoxide/issues).

## Now: 1.8.0

A fixed port for dev setups, where something needs the endpoint before the
server starts: a second app, an `.env` file or NoSQL Workbench.

- **A `port` option** on `start()` in the npm and PHP packages. It defaults to
  0, so tests still get a free port.
- **A taken port says so.** The engine exits with status `4` when the port it
  was given is in use, and both packages turn that into an error naming the
  port, without reading stderr.
- **Laravel's `dynoxide:serve`** names a taken port and how to change it.

## Shipped in 1.7.0

Starting Dynoxide from a test suite, in any language, without hand-rolled
process code: the [launcher contract](docs/launcher.md), `start()` in the npm
package, the `nubo-db/dynoxide` PHP package with its PHPUnit extension, and
the `nubo-db/dynoxide-laravel` package.

## Next

- **A Python launcher** on PyPI, built on the same contract.
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

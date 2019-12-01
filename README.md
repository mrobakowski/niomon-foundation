# Niomon Foundation
This library contains common machinery for all the microservices in the Niomon project. In particular, it abstracts
kafka connections away requiring the microservice to only (well, ideally) implement the per-message processing part.
It also provides some caching support using [redis-cache](../redis-cache/README.md).

## Development
The most interesting class is [NioMicroserviceLive](src/main/scala/com/ubirch/niomon/base/NioMicroserviceLive.scala).
Refer to ScalaDoc there.

### Core libraries
* akka-kafka
* akka-streams
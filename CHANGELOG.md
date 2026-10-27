# Changelog

## [0.2.2](https://github.com/osodevops/kafka-connect-salesforce-oss/compare/v0.2.1...v0.2.2) (2026-10-27)


### Bug Fixes

* **source:** render ReplayId as Salesforce's numeric replay position ([#38](https://github.com/osodevops/kafka-connect-salesforce-oss/issues/38)) ([d26dac8](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/d26dac8038f290b7998e02734af4a15623378338))
* **source:** wait for Salesforce's keepalive before snapshotting instead of timing out ([#39](https://github.com/osodevops/kafka-connect-salesforce-oss/issues/39)) ([505c259](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/505c2597fd7a94abe4328b34485ac198a3716e5c))


### Documentation

* Platform Events are available from 0.2.1 ([#36](https://github.com/osodevops/kafka-connect-salesforce-oss/issues/36)) ([86e33f2](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/86e33f28abc1fdc0ac577f267e18d441097ba8d4))
* Platform Events walkthrough with records from a real org ([#40](https://github.com/osodevops/kafka-connect-salesforce-oss/issues/40)) ([ed5181f](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/ed5181f2a7371c0a7183242701f2ea14746c77dd))

## [0.2.1](https://github.com/osodevops/kafka-connect-salesforce-oss/compare/v0.2.0...v0.2.1) (2026-10-27)


### Bug Fixes

* **build:** publish to Maven Central with central-publishing-maven-plugin 0.12.0 ([#33](https://github.com/osodevops/kafka-connect-salesforce-oss/issues/33)) ([300fce3](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/300fce3cdb5fb3d93e03abc40fd305002b7bc4b7))

## [0.2.0](https://github.com/osodevops/kafka-connect-salesforce-oss/compare/v0.1.0...v0.2.0) (2026-10-27)


### Features

* **migration:** translate Confluent Platform Event source configs and Confluent Cloud class names ([#31](https://github.com/osodevops/kafka-connect-salesforce-oss/issues/31)) ([aea984b](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/aea984b49303f180ba056194c8f838dabf405561))
* **source:** consume Platform Events over the Pub/Sub API ([#30](https://github.com/osodevops/kafka-connect-salesforce-oss/issues/30)) ([82555c5](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/82555c5eb71b7ba1ebfa788a93abf419af849ba0))


### Bug Fixes

* **source:** recover from Pub/Sub stream errors instead of failing the task ([#29](https://github.com/osodevops/kafka-connect-salesforce-oss/issues/29)) ([1534a07](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/1534a079896d690d4e4ea9e32d6ce3009ce0d26d))


### Documentation

* Enterprise Support page and navigation ([#26](https://github.com/osodevops/kafka-connect-salesforce-oss/issues/26)) ([74f877d](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/74f877d47e57164e2d1085cb7a76bddca609768b))
* generated per-property configuration reference + README config examples ([7fb4075](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/7fb4075cf5ce4fac736d2e2abf91ec48b388c605))
* publish the support policy and align the security policy ([#25](https://github.com/osodevops/kafka-connect-salesforce-oss/issues/25)) ([888eadc](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/888eadcb659369b0949c7cb62c38fd0dd5c63bcf))

## [0.1.0](https://github.com/osodevops/kafka-connect-salesforce-oss/compare/v0.0.1...v0.1.0) (2026-07-08)


### Features

* Confluent migration tooling, verifiable cutover evidence, and guide ([bc3ab46](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/bc3ab46159e739e6e8f21a46d7de37814b018dd8))
* initial open-source Salesforce Kafka connector suite ([6f38d2e](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/6f38d2ee0b7bb107ebd958569cec320b1ebcb2a3))


### Documentation

* add contributing, commercial support, and OSO footer to README ([482b954](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/482b954bf65f2e31e8e2665f17b2a8b28f27a432))


### Miscellaneous Chores

* prepare first public release ([e35cae3](https://github.com/osodevops/kafka-connect-salesforce-oss/commit/e35cae303776950afe000b00ce4433592117ff38))

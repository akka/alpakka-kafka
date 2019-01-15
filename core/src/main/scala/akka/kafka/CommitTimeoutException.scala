/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import java.util.concurrent.TimeoutException

/**
 * Calls to `commitJavadsl` and `commitScaladsl` will be failed with this exception if
 * Kafka doesn't respond within `commit-timeout`
 */
class CommitTimeoutException(msg: String) extends TimeoutException(msg) {}

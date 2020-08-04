/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka

import java.util.concurrent.TimeoutException

/**
 * Commits will be failed with this exception if Kafka doesn't respond within `commit-timeout`
 */
class CommitTimeoutException(msg: String) extends TimeoutException(msg) {}

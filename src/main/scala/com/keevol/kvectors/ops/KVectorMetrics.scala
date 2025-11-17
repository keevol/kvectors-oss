package com.keevol.kvectors.ops

/**
 * <pre>
 * ██╗  ██╗ ███████╗ ███████╗ ██╗   ██╗  ██████╗  ██╗
 * ██║ ██╔╝ ██╔════╝ ██╔════╝ ██║   ██║ ██╔═══██╗ ██║
 * █████╔╝  █████╗   █████╗   ██║   ██║ ██║   ██║ ██║
 * ██╔═██╗  ██╔══╝   ██╔══╝   ╚██╗ ██╔╝ ██║   ██║ ██║
 * ██║  ██╗ ███████╗ ███████╗  ╚████╔╝  ╚██████╔╝ ███████╗
 * ╚═╝  ╚═╝ ╚══════╝ ╚══════╝   ╚═══╝    ╚═════╝  ╚══════╝
 * </pre>
 * <p>
 * KEEp eVOLution!
 * <p>
 *
 * @author fq@keevol.cn
 * @since 2017.5.12
 * <p>
 * Copyright 2017 © 杭州福强科技有限公司版权所有 (<a href="https://www.keevol.cn">keevol.cn</a>)
 */

import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{Counter, Gauge, Histogram, Meter, MetricRegistry, Timer}
import com.keevol.goodies.lifecycle.Shutdowns
import io.opentelemetry.api.{GlobalOpenTelemetry, OpenTelemetry}

/**
 * https://metrics.dropwizard.io/4.2.0/getting-started.html
 *
 * Although micrometer and open-telemetry is the future, but I like this library more ;)
 *
 * The node can run standalone, whether the ops facilities are ready or not.
 *
 * That's, if metrics are needed, ops can pull them as needed. (instead of pushing to ops facilities which will make the node running with external dependencies)
 *
 * Furthermore, we use opentelemetry javaagent to start our program, so a GlobalOpenTelemetry ref will be available, but this is optional.
 */
object KVectorMetrics {

  val otel: OpenTelemetry = GlobalOpenTelemetry.get() // only invoke .get once as javadoc suggested.

  // https://www.javadoc.io/doc/io.dropwizard.metrics/metrics-core/latest/com/codahale/metrics/package-summary.html
  val registry: MetricRegistry = new MetricRegistry()
  // export metrics via jmx as default way
  private val jmxReporter = JmxReporter.forRegistry(registry).build();
  Shutdowns.add(() => jmxReporter.stop())
  Thread.ofVirtual().start(() => {
    jmxReporter.start()
  })

  def meter(name: String): Meter = registry.meter(name)

  def histogram(name: String): Histogram = registry.histogram(name)

  def gauge[T](name: String): Gauge[T] = registry.gauge(name)

  def counter(name: String): Counter = registry.counter(name)

  def timer(name: String): Timer = registry.timer(name)
}
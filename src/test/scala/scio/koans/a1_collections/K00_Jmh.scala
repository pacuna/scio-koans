package scio.koans.a1_collections

import scio.koans.shared._
import org.openjdk.jmh.annotations._

/**
 * A `JmhKoan` tests function performance with `sbt-jmh` micro-benchmark.
 *
 * https://github.com/ktoso/sbt-jmh
 */
class K00_Jmh extends JmhKoan {

  @Benchmark def baseline: Int = {
    Thread.sleep(10)
    (1 to 100).sum
  }

  @Benchmark def v1: Int = {
    (1 to 100).sum
  }

  // Add as many alternatives as you like
  // @Benchmark def v2: Int = ???

  // Verify that all benchmark results are the same as `baseline`
  verifyResults()

  // Verify that all benchmark scores are faster than `baseline`
  verifySpeedup(Speedup.Times(500))
}

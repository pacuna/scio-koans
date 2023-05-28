package scio.koans.a2_coders

import scio.koans.shared._

/**
 * Fix the non-deterministic coder exception.
 */
class K02_LatLon1 extends PipelineKoan {

  import K02_LatLon1._

  val events: Seq[(LatLon, String)] = Seq(
    (LatLon(3450, -1000), "a"),
    (LatLon(6780, 1230), "b"),
    (LatLon(6780, 1230), "c"),
    (LatLon(-4500, 314), "d")
  )

  val expected: Seq[(LatLon, Set[String])] = Seq(
    (LatLon(3450, -1000), Set("a")),
    (LatLon(6780, 1230), Set("b", "c")),
    (LatLon(-4500, 314), Set("d"))
  )

  "Snippet" should "work" in {
    runWithContext { sc =>
      val output = sc
        .parallelize(events)
        // `*ByKey` transforms compare keys using encoded bytes and must be deterministic
        .groupByKey
        .mapValues(_.toSet)

      output should containInAnyOrder(expected)
    }
  }
}

object K02_LatLon1 {
  // Latitude and longitude are degrees bounded by [-90, 90] and [-180, 180] respectively.
  // 1 degree = 60 minutes
  // 1 minute = 60 seconds
  // https://en.wikipedia.org/wiki/Decimal_degrees#Precision
  // Hint: `Double` encoding is not deterministic but `Int` is
  case class LatLon(lat: Int, lon: Int)
}

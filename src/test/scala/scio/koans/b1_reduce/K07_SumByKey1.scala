package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import scio.koans.shared._

/**
 * Replace `reduceByKey` with `sumByKey`.
 */
class K07_SumByKey1 extends TransformKoan {

  type InT = SCollection[(String, Int)]
  type OutT = (SCollection[(String, Int)], SCollection[(String, Int)], SCollection[(String, Int)])

  val input: Seq[(String, Int)] = Seq(
    ("a", 1),
    ("a", 1),
    ("a", 1),
    ("a", 2),
    ("a", 2),
    ("a", 3),
    ("b", 4),
    ("b", 5),
    ("c", 6)
  )
  val expectedMin: Seq[(String, Int)] = Seq(("a", 1), ("b", 4), ("c", 6))
  val expectedMax: Seq[(String, Int)] = Seq(("a", 3), ("b", 5), ("c", 6))
  val expectedDistinctCount: Seq[(String, Int)] = Seq(("a", 3), ("b", 2), ("c", 1))

  prepare(_.parallelize(input))
  verify { case (min, max, distinctCount) =>
    min should containInAnyOrder(expectedMin)
    max should containInAnyOrder(expectedMax)
    distinctCount should containInAnyOrder(expectedDistinctCount)
  }

  baseline { input =>
    val min: SCollection[(String, Int)] = input.reduceByKey(math.min)
    val max: SCollection[(String, Int)] = input.reduceByKey(math.max)
    val distinctCount: SCollection[(String, Int)] =
      input.mapValues(Set(_)).reduceByKey(_ ++ _).mapValues(_.size)
    (min, max, distinctCount)
  }

  test("v1") { input =>
    // `Min[T]` and implicit `Semigroup[Min[Int]` from Algebird
    val min: SCollection[(String, Int)] = input.mapValues(Min(_)).sumByKey.mapValues(_.get)

    // FIXME: implement these with `sumByKey`
    // Hint: Algebird also provides implicit `Semigroup[Max[Int]]` and `Semigroup[Set[Int]]`
    val max: SCollection[(String, Int)] = input.mapValues(Max(_)).sumByKey.mapValues(_.get)
    val distinctCount: SCollection[(String, Int)] = input.mapValues(Set(_)).sumByKey.mapValues(_.size)

    (min, max, distinctCount)
  }
}

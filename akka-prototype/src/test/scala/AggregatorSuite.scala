import edu.berkeley.cs.amplab.aggregationsketches.aggregators.{CountAggregator, BloomFilterRouter}
import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

class AggregatorSuite extends FlatSpec with ShouldMatchers {

  "A BloomFilterRouter" should "route to onMiss for the first element sent to it" in {
      val onMatch = new CountAggregator
      val onMiss = new CountAggregator
      val router = new BloomFilterRouter[Int, Int](100, 0.01f)(onMatch, onMiss)
      router.send((1, 1))
      onMatch.getCount should equal (0)
      onMiss.getCount should equal (1)
      router.send((1, 1))
      onMatch.getCount should equal (1)
      onMiss.getCount should equal (1)
  }

  it should "route to onMatch for true positives" in {
    val onMatch = new CountAggregator
    val onMiss = new CountAggregator
    val router = new BloomFilterRouter[Int, Int](100, 0.01f)(onMatch, onMiss)
    router.send((1, 1))
    router.send((1, 1))
    router.send((1, 1))
    onMatch.getCount should equal (2)
  }

}

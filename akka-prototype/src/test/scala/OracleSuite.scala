import edu.berkeley.cs.amplab.aggregationsketches.aggregators.Oracles
import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import scala.util.Random

class OracleSuite extends FlatSpec with ShouldMatchers {

  "Complete Lookahead Oracle" should "aggregate perfectly on a clustered stream" in {
    val items = Seq(1, 1, 1, 3, 3, 3, 2, 2, 2)
    Oracles.completeLookahead(items.toStream, 1) should equal (items.toSet.size)
  }

  it should "aggregate perfectly given as much buffer space as keys" in {
    val items = Array.fill(200)(Random.nextInt(50))
    val numKeys = items.toSet.size
    Oracles.completeLookahead(items.toStream, numKeys) should equal (numKeys)
  }


  "Complete Frequency Distribution Oracle" should "make suboptimal decisions on fully clustered streams" in {
    val items = Seq(1, 1, 1, 1, 1, 3, 3, 3, 2, 2)
    // Here, 1 is the most popular key, so it only appears once in the output,
    // while the other 5 inputs are never aggregated:
    Oracles.completeFrequencyDistribution(items.toStream, 1) should equal (6)
  }

  it should "aggregate perfectly given as much buffer space as keys" in {
    val items = Array.fill(200)(Random.nextInt(50))
    val numKeys = items.toSet.size
    Oracles.completeFrequencyDistribution(items.toStream, numKeys) should equal (numKeys)
  }
}

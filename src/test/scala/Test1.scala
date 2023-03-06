
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite

class SparkWordCountTest extends AnyFunSuite with SharedSparkContext    {
  test("testing case 1 Only CSV") {
    val inputPath="src/test/resources/dataset1"
    implicit val sparkContext=sc  //sc is automatically available.
    val rdd = CodingMain.process(sc,"local",inputPath)
    val resultMap=rdd.collectAsMap()
    assert(resultMap("1") == "2")
    assert(resultMap("2") == "4")

  }

  test("testing case 2 Only TSV") {
    val inputPath = "src/test/resources/dataset2"
    implicit val sparkContext = sc //sc is automatically available.
    val rdd = CodingMain.process(sc, "local", inputPath)
    val resultMap = rdd.collectAsMap()
    assert(resultMap("1") == "2")
    assert(resultMap("2") == "4")

  }

  test("testing case CSV+ TSV") {
    val inputPath = "src/test/resources/dataset3"
    implicit val sparkContext = sc //sc is automatically available.
    val rdd= CodingMain.process(sc, "local", inputPath)
    val resultMap = rdd.collectAsMap()
    assert(resultMap("1") == "2")
    assert(resultMap("2") == "4")
  }

  test("testing case multiple Unique key,value") {
    val inputPath = "src/test/resources/dataset4"
    implicit val sparkContext = sc //sc is automatically available.
    val rdd = CodingMain.process(sc, "local", inputPath)
    val resultMap = rdd.collectAsMap()

    assert(resultMap.size==0)

  }

  test("testing case multiple files") {
    val inputPath = "src/test/resources/dataset5"
    implicit val sparkContext = sc //sc is automatically available.
    val rdd = CodingMain.process(sc, "local", inputPath)
    val resultMap = rdd.collectAsMap()

    assert(resultMap("1") == "2")
    assert(resultMap("2") == "4")

  }


}

import java.io.{FileInputStream, UnsupportedEncodingException}

class AsciiGrid2WKTProcessorTest {
  private var testRunner = null

  @Before def init(): Unit = {
    testRunner = TestRunners.newTestRunner(classOf[Nothing])
  }

  @Test def testProcessor(): Unit = {
    try
      testRunner.enqueue(new FileInputStream(new Nothing("src/test/resources/asciiGridFolder/asciigrid1.asc")))
    catch {
      case e: Nothing =>
        e.printStackTrace
    }
    testRunner.run
    testRunner.assertValid
    val successFiles = testRunner.getFlowFilesForRelationship(LinkProcessor.REL_SUCCESS)
    for (mockFile <- successFiles) {
      try
        System.out.println("FILE:" + new String(mockFile.toByteArray, "UTF-8"))
      catch {
        case e: UnsupportedEncodingException =>
          e.printStackTrace()
      }
    }
  }
}
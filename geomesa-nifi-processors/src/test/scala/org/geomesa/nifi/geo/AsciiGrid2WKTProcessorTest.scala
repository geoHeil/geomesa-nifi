//import java.io.{File, FileInputStream, UnsupportedEncodingException}
//
//import org.apache.nifi.util.TestRunners
//import org.junit.{Before, Test}
//
//class AsciiGrid2WKTProcessorTest {
//  private var testRunner = null
//
//  @Before def init(): Unit = {
//    testRunner = TestRunners.newTestRunner(classOf[AsciiGrid2WKTProcessor])
//  }
//
//  @Test def testProcessor(): Unit = {
//    try
//      testRunner.enqueue(new FileInputStream(new File("src/test/resources/asciiGridFolder/asciigrid1.asc")))
//    catch {
//      case e: Nothing =>
//        e.printStackTrace
//    }
//    testRunner.run
//    testRunner.assertValid
//    val successFiles = testRunner.getFlowFilesForRelationship(LinkProcessor.REL_SUCCESS)
//    for (mockFile <- successFiles) {
//      try
//        System.out.println("FILE:" + new String(mockFile.toByteArray, "UTF-8"))
//      catch {
//        case e: UnsupportedEncodingException =>
//          e.printStackTrace()
//      }
//    }
//  }
//}
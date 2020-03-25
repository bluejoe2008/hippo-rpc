package hippo

import java.io.{File, FileInputStream}

import io.netty.buffer.Unpooled
import org.apache.commons.io.IOUtils
import org.grapheco.commons.util.Profiler
import org.grapheco.commons.util.Profiler._
import org.grapheco.hippo.util.ByteBufferInputStream
import org.grapheco.hippo.{HippoClient, HippoClientFactory, HippoServer}
import org.junit.{After, Assert, Before, Test}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class HippoRpcTest {
  Profiler.enableTiming = true
  var server1: HippoServer = null
  var server2: HippoServer = null
  var client: HippoClient = null;

  @Before
  def setup(): Unit = {
    server1 = HippoRpcServerForTest.createServer(1224)
    server2 = HippoRpcServerForTest.createServer(1225)
    client = HippoClientFactory.create("test", Map()).createClient("localhost", 1224)
  }

  @After
  def after(): Unit = {
    server1.close()
    server2.close()

    client.close()
  }

  @Test
  def testRpc(): Unit = {
    Await.result(client.askWithStream[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)

    val res1 = timing(true) {
      Await.result(client.askWithStream[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)
    }

    Assert.assertEquals("HELLO", res1.str);
  }

  @Test
  def testPutFile(): Unit = {
    val res = timing(true, 10) {
      Await.result(client.askWithStream[PutFileResponse](PutFileRequest(new File("./testdata/inputs/9999999").length().toInt), {
        val buf = Unpooled.buffer(1024)
        val fos = new FileInputStream(new File("./testdata/inputs/9999999"));
        buf.writeBytes(fos.getChannel, new File("./testdata/inputs/9999999").length().toInt)
        fos.close()
        buf
      }), Duration.Inf)
    }

    Assert.assertEquals(new File("./testdata/inputs/9999999").length(), res.written)
  }

  @Test
  def testPutFileAsync(): Unit = {
    val futures = (1 to 10).map {
      x =>
        client.askWithStream[PutFileResponse](PutFileRequest(new File("./testdata/inputs/9999999").length().toInt), {
          val buf = Unpooled.buffer(1024)
          val fos = new FileInputStream(new File("./testdata/inputs/9999999"));
          buf.writeBytes(fos.getChannel, new File("./testdata/inputs/9999999").length().toInt)
          fos.close()
          buf
        })
    }

    val res = futures.map(x => Await.result(x, Duration("4s"))).head
    Assert.assertEquals(new File("./testdata/inputs/9999999").length(), res.written)
  }

  @Test
  def testPutFileWithForward(): Unit = {
    val res = timing(true, 10) {
      //1224->1225
      Await.result(client.askWithStream[PutFileWithForwardResponse](PutFileWithForwardRequest(
        new File("./testdata/inputs/9999999").length().toInt, 1225), {
        val buf = Unpooled.buffer(1024)
        val fos = new FileInputStream(new File("./testdata/inputs/9999999"));
        buf.writeBytes(fos.getChannel, new File("./testdata/inputs/9999999").length().toInt)
        fos.close()
        buf
      }), Duration.Inf)
    }

    Assert.assertArrayEquals(Array(1224, 1225), res.nodes)
    Assert.assertEquals(new File("./testdata/inputs/9999999").length(), res.written)
  }

  @Test
  def testPutFileWithForwardAsync(): Unit = {
    val futures = (1 to 10).map {
      x =>
        //1224->1225
        client.askWithStream[PutFileWithForwardResponse](PutFileWithForwardRequest(
          new File("./testdata/inputs/9999999").length().toInt, 1225), {
          val buf = Unpooled.buffer(1024)
          val fos = new FileInputStream(new File("./testdata/inputs/9999999"));
          buf.writeBytes(fos.getChannel, new File("./testdata/inputs/9999999").length().toInt)
          fos.close()
          buf
        })
    }

    val res = futures.map(x => Await.result(x, Duration.Inf)).head
    Assert.assertArrayEquals(Array(1224, 1225), res.nodes)
    Assert.assertEquals(new File("./testdata/inputs/9999999").length(), res.written)
  }

  @Test
  def testGetChunkedStream(): Unit = {
    Await.result(client.askWithStream[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)

    val results = timing(true, 10) {
      client.getChunkedStream[String](GetManyResultsRequest(100, 10, "hello"), Duration.Inf)
    }.toArray

    Assert.assertEquals(results(0), "hello-1")
    Assert.assertEquals(results(99), "hello-100")
    Assert.assertEquals(100 * 10, results.length)

    val results2 = timing(true, 10) {
      client.getChunkedStream[String](GetBufferedResultsRequest(100), Duration.Inf).toArray
    }

    Assert.assertEquals(results2(0), "hello-1")
    Assert.assertEquals(results2(99), "hello-100")
    Assert.assertEquals(100, results2.length)
  }

  @Test
  def testGetStream(): Unit = {
    Await.result(client.askWithStream[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)

    val bytes = timing(true, 10) {
      Await.result(client.ask(ReadFileRequest("./testdata/inputs/9999999"), (buf) => {
        val bs = new Array[Byte](buf.remaining())
        buf.get(bs)
        bs
      }), Duration("4s"))
    }

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))),
      bytes
    )

    val bytes2 = timing(true, 10) {
      Await.result(client.ask(ReadFileRequest("./testdata/inputs/9999999"), (buf) => {
        IOUtils.toByteArray(new ByteBufferInputStream(buf))
      }), Duration("4s"))
    }

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))),
      bytes2
    )

    timing(true, 10) {
      val is = client.getInputStream(ReadFileRequest("./testdata/inputs/9999999"), Duration.Inf);
      IOUtils.toByteArray(is)
    }

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/999"))),
      IOUtils.toByteArray(client.getInputStream(ReadFileRequest("./testdata/inputs/999"), Duration.Inf))
    );

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/999"))),
      IOUtils.toByteArray(client.getChunkedInputStream(ReadFileRequest("./testdata/inputs/999"), Duration.Inf))
    );

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))),
      IOUtils.toByteArray(client.getInputStream(ReadFileRequest("./testdata/inputs/9999999"), Duration.Inf))
    );

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))),
      IOUtils.toByteArray(client.getChunkedInputStream(ReadFileRequest("./testdata/inputs/9999999"), Duration.Inf))
    );

    for (size <- Array(999, 9999, 99999, 999999, 9999999, 10000099)) {
      println("=================================")

      println(s"getInputStream(): size=$size")
      timing(true, 10) {
        IOUtils.toByteArray(client.getInputStream(ReadFileRequest(s"./testdata/inputs/$size"), Duration.Inf))
      }

      println(s"getChunkedInputStream(): size=$size")
      timing(true, 10) {
        IOUtils.toByteArray(client.getChunkedInputStream(ReadFileRequest(s"./testdata/inputs/$size"), Duration.Inf))
      }

      println("=================================")
    }

    import scala.concurrent.ExecutionContext.Implicits.global

    //378ms
    timing(true) {
      val futures = (1 to 5).map { _ =>
        Future {
          IOUtils.toByteArray(client.getInputStream(ReadFileRequest("./testdata/inputs/9999999"), Duration.Inf))
        }
      }

      futures.foreach(Await.result(_, Duration.Inf))
    }
  }
}

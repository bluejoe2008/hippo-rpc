package hippo

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import hippo.HippoRpcServerForTest.logger
import io.netty.buffer.{ByteBuf, Unpooled}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoEndpointRef, HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc._
import org.apache.commons.io.IOUtils
import org.grapheco.commons.util.Logging
import org.grapheco.commons.util.Profiler.timing
import org.grapheco.hippo.util.ByteBufferInputStream
import org.grapheco.hippo.{ChunkedStream, CompleteStream, HippoRpcHandler, ReceiveContext}
import org.junit.{After, Assert, Before, Test}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class HippoRpcEnvFactoryTest {
  var serverRpcEnv1:HippoRpcEnv = null
  var serverRpcEnv2:HippoRpcEnv = null
  val serverConfig1 = RpcEnvServerConfig(new RpcConf(), "hippo-server1", "localhost", 5566)
  val serverConfig2 = RpcEnvServerConfig(new RpcConf(), "hippo-server2", "localhost", 5567)

  var clientRpcEnv:HippoRpcEnv = null
  var endpointRef:HippoEndpointRef = null


  @Before
  def setup(): Unit ={

    val clientConfig = RpcEnvClientConfig(new RpcConf(), "client")
    serverRpcEnv1 = HippoRpcEnvFactory.create(serverConfig1)
    serverRpcEnv2 = HippoRpcEnvFactory.create(serverConfig2)

    val endpoint1 = new FileRpcEndPoint(serverRpcEnv1)
    val endpoint2 = new FileRpcEndPoint(serverRpcEnv2)

    serverRpcEnv1.setupEndpoint("server1", endpoint1)
    serverRpcEnv1.setRpcHandler(endpoint1)
    serverRpcEnv2.setupEndpoint("server2", endpoint2)
    serverRpcEnv2.setRpcHandler(endpoint2)

    clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    endpointRef = clientRpcEnv.setupEndpointRef(RpcAddress(serverConfig1.bindAddress,serverConfig1.port), "server1")
  }

  @After
  def after(): Unit ={
    serverRpcEnv1.shutdown()
    serverRpcEnv2.shutdown()
  }

  @Test
  def testRpc(): Unit = {
    Await.result(endpointRef.askWithStream[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)


    val res1 = timing(true) {
      Await.result(endpointRef.askWithStream[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)
    }

    Assert.assertEquals("HELLO", res1.str);
  }

  @Test
  def testPutFile(): Unit ={
    val res = timing(true, 10) {
      Await.result(endpointRef.askWithStream[PutFileResponse](PutFileRequest(new File("./testdata/inputs/9999999").length().toInt), {
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
        endpointRef.askWithStream[PutFileResponse](PutFileRequest(new File("./testdata/inputs/9999999").length().toInt), {
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
  def testPutFileWithForward(): Unit ={
    val res = timing(true, 10) {
      Await.result(endpointRef.askWithStream[PutFileWithForwardResponse](PutFileWithForwardRequest(
        new File("./testdata/inputs/9999999").length().toInt, serverConfig2.port), {
        val buf = Unpooled.buffer(1024)
        val fos = new FileInputStream(new File("./testdata/inputs/9999999"));
        buf.writeBytes(fos.getChannel, new File("./testdata/inputs/9999999").length().toInt)
        fos.close()
        buf
      }), Duration.Inf)
    }

    Assert.assertArrayEquals(Array(serverConfig1.port, serverConfig2.port), res.nodes)
    Assert.assertEquals(new File("./testdata/inputs/9999999").length(), res.written)
  }

  @Test
  def testPutFileWithForwardAsync(): Unit = {
    val futures = (1 to 10).map {
      x =>
        endpointRef.askWithStream[PutFileWithForwardResponse](PutFileWithForwardRequest(
          new File("./testdata/inputs/9999999").length().toInt, serverConfig2.port), {
          val buf = Unpooled.buffer(1024)
          val fos = new FileInputStream(new File("./testdata/inputs/9999999"));
          buf.writeBytes(fos.getChannel, new File("./testdata/inputs/9999999").length().toInt)
          fos.close()
          buf
        })
    }
    val res = futures.map(x => Await.result(x, Duration.Inf)).head
    Assert.assertArrayEquals(Array(serverConfig1.port, serverConfig2.port), res.nodes)
    Assert.assertEquals(new File("./testdata/inputs/9999999").length(), res.written)
  }

  @Test
  def testGetChunkedStream(): Unit ={
    Await.result(endpointRef.askWithStream[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)

    val results = timing(true, 10) {
      endpointRef.getChunkedStream[String](GetManyResultsRequest(100, 10, "hello"), Duration.Inf)
    }.toArray

    Assert.assertEquals(results(0), "hello-1")
    Assert.assertEquals(results(99), "hello-100")
    Assert.assertEquals(100 * 10, results.length)

    val results2 = timing(true, 10) {
      endpointRef.getChunkedStream[String](GetBufferedResultsRequest(100), Duration.Inf).toArray
    }

    Assert.assertEquals(results2(0), "hello-1")
    Assert.assertEquals(results2(99), "hello-100")
    Assert.assertEquals(100, results2.length)
  }

  @Test
  def testGetStream(): Unit = {
    Await.result(endpointRef.askWithStream[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)

    val bytes = timing(true, 10) {
      Await.result(endpointRef.ask(ReadFileRequest("./testdata/inputs/9999999"), (buf) => {
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
      Await.result(endpointRef.ask(ReadFileRequest("./testdata/inputs/9999999"), (buf) => {
        IOUtils.toByteArray(new ByteBufferInputStream(buf))
      }), Duration("4s"))
    }

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))),
      bytes2
    )

    timing(true, 10) {
      val is = endpointRef.getInputStream(ReadFileRequest("./testdata/inputs/9999999"), Duration.Inf);
      IOUtils.toByteArray(is)
    }

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/999"))),
      IOUtils.toByteArray(endpointRef.getInputStream(ReadFileRequest("./testdata/inputs/999"), Duration.Inf))
    );

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/999"))),
      IOUtils.toByteArray(endpointRef.getChunkedInputStream(ReadFileRequest("./testdata/inputs/999"), Duration.Inf))
    );

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))),
      IOUtils.toByteArray(endpointRef.getInputStream(ReadFileRequest("./testdata/inputs/9999999"), Duration.Inf))
    );

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))),
      IOUtils.toByteArray(endpointRef.getChunkedInputStream(ReadFileRequest("./testdata/inputs/9999999"), Duration.Inf))
    );

    for (size <- Array(999, 9999, 99999, 999999, 9999999, 10000099)) {
      println("=================================")

      println(s"getInputStream(): size=$size")
      timing(true, 10) {
        IOUtils.toByteArray(endpointRef.getInputStream(ReadFileRequest(s"./testdata/inputs/$size"), Duration.Inf))
      }

      println(s"getChunkedInputStream(): size=$size")
      timing(true, 10) {
        IOUtils.toByteArray(endpointRef.getChunkedInputStream(ReadFileRequest(s"./testdata/inputs/$size"), Duration.Inf))
      }

      println("=================================")
    }

    import scala.concurrent.ExecutionContext.Implicits.global

    //378ms
    timing(true) {
      val futures = (1 to 5).map { _ =>
        Future {
          IOUtils.toByteArray(endpointRef.getInputStream(ReadFileRequest("./testdata/inputs/9999999"), Duration.Inf))
        }
      }

      futures.foreach(Await.result(_, Duration.Inf))
    }
  }
}

class FileRpcEndPoint(override val rpcEnv: HippoRpcEnv) extends RpcEndpoint with HippoRpcHandler with Logging{

  override def receiveWithStream(extraInput: ByteBuffer, ctx: ReceiveContext): PartialFunction[Any, Unit] = {
    case SayHelloRequest(msg) =>
      ctx.reply(SayHelloResponse(msg.toUpperCase()))

    case PutFileRequest(totalLength) =>
      ctx.reply(PutFileResponse(extraInput.remaining()))

    case ReadFileRequest(path) =>
      val buf = Unpooled.buffer()
      val fis = new FileInputStream(new File(path))
      buf.writeBytes(fis.getChannel, new File(path).length().toInt)
      ctx.replyBuffer(buf)

    case PutFileWithForwardRequest(totalLength, port2) => {
      val clientForwardEnv = HippoRpcEnvFactory.create(RpcEnvClientConfig(new RpcConf(), "client2"))
      val endpointRef = clientForwardEnv.setupEndpointRef(RpcAddress("localhost", port2), "server2")

      val future = endpointRef.askWithStream[PutFileResponse](PutFileRequest(totalLength),
        Unpooled.wrappedBuffer(extraInput.duplicate()))

      Await.result(future, Duration.Inf);

      ctx.reply(PutFileWithForwardResponse(totalLength, Array(this.rpcEnv.address.port) ++ Array(port2)))
    }
  }
  override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    case GetManyResultsRequest(times, chunkSize, msg) =>
      ChunkedStream.grouped(chunkSize, (1 to times * chunkSize).map(x => s"hello-${x}"))

    case GetBufferedResultsRequest(total) =>
      ChunkedStream.pooled[String](10, (pool) => {
        for (i <- 1 to total) {
          pool.push(s"hello-$i");
          Thread.sleep(1);
        }
      })

    case ReadFileRequest(path) =>
      new ChunkedStream() {
        val fis = new FileInputStream(new File(path))
        val length = new File(path).length()
        var count = 0;

        override def hasNext(): Boolean = {
          count < length
        }

        def nextChunk(buf: ByteBuf): Unit = {
          val written =
            timing(false) {
              buf.writeBytes(fis, 1024 * 1024 * 10)
            }

          count += written
        }

        override def close(): Unit = {
          fis.close()
        }
      }
  }
  override def openCompleteStream(): PartialFunction[Any, CompleteStream] = {
    case ReadFileRequest(path) =>
      val fis = new FileInputStream(new File(path))
      val buf = Unpooled.buffer()
      buf.writeBytes(fis.getChannel, new File(path).length().toInt)

      CompleteStream.fromByteBuffer(buf);
  }
}

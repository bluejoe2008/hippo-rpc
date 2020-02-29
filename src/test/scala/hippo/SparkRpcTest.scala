package hippo

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

import org.grapheco.hippo.util.ByteBufferUtils._
import org.grapheco.commons.util.Profiler
import org.grapheco.commons.util.Profiler._
import io.netty.buffer.Unpooled
import org.apache.commons.io.IOUtils
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{ChunkReceivedCallback, RpcResponseCallback, StreamCallback, TransportClient}
import org.apache.spark.network.server.{NoOpRpcHandler, TransportServer}
import org.apache.spark.network.util.{MapConfigProvider, TransportConf}
import org.junit.{After, Assert, Before, Test}

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2020/2/14.
  */
class SparkRpcTest {

  Profiler.enableTiming = true
  val confProvider = new MapConfigProvider(JavaConversions.mapAsJavaMap(Map()))

  val clientFactory = {
    val conf: TransportConf = new TransportConf("test", confProvider)
    val context: TransportContext = new TransportContext(conf, new NoOpRpcHandler())
    context.createClientFactory
  }

  class MyChunkReceivedCallback extends ChunkReceivedCallback {
    val latch = new CountDownLatch(1);
    val out = new ByteArrayOutputStream();

    override def onFailure(chunkIndex: Int, e: Throwable): Unit = {
      latch.countDown()
    }

    override def onSuccess(chunkIndex: Int, buf: ManagedBuffer): Unit = {
      IOUtils.copy(buf.createInputStream(), out);
      latch.countDown()
    }

    def getBytes() = {
      latch.await()
      out.toByteArray
    }
  }

  class MyStreamCallback extends StreamCallback {
    val out = new ByteArrayOutputStream();
    val latch = new CountDownLatch(1);
    var count = 0;

    override def onData(streamId: String, buf: ByteBuffer): Unit = {
      val tmp: Array[Byte] = new Array[Byte](buf.remaining)
      buf.get(tmp)
      out.write(tmp)

      //println(s"onData: streamId=$streamId, length=${buf.position()}, index=$count");
      count += 1
    }

    override def onComplete(streamId: String): Unit = {
      out.close()
      latch.countDown()
    }

    override def onFailure(streamId: String, cause: Throwable): Unit = {
      latch.countDown()
      throw cause;
    }

    def getBytes() = {
      latch.await()
      out.toByteArray
    }
  }

  var server: TransportServer = null
  var client: TransportClient = null

  @Before
  def setup(): Unit = {
    server = SparkRpcServerForTest.createServer()
    client = clientFactory.createClient("localhost", server.getPort);
  }

  @After
  def teardown() = {
    server.close()
    client.close()
  }

  @Test
  def test1() {

    client.send(Unpooled.buffer().writeByte(1.toByte).writeString("no reply").writeLong(999).nioBuffer());
    val cd = new CountDownLatch(1);
    client.sendRpc(
      Unpooled.buffer().writeByte(1.toByte).writeString("hello").writeLong(888).nioBuffer(), new RpcResponseCallback() {
        override def onFailure(e: Throwable): Unit = {
          cd.countDown();
        }

        override def onSuccess(response: ByteBuffer): Unit = {
          val answer = response.readString() //Unpooled.wrappedBuffer(response).readString();
          println(answer)
          Assert.assertEquals("HELLO", answer)
          cd.countDown();
        }
      })

    cd.await()
  }

  @Test
  def testSendLargeFiles() {
    //send large files
    val res = timing(true) {
      client.sendRpcSync(ByteBuffer.allocate(9999999).put(2.toByte).flip().asInstanceOf[ByteBuffer], 1000000)
    }

    Assert.assertEquals(100, res.getInt);
  }

  @Test
  def testGetChunksParallelly() {
    //get chunks parallely
    timing(true) {
      val mcs = ArrayBuffer[MyChunkReceivedCallback]()
      for (i <- 1 to 9) {
        val mc = new MyChunkReceivedCallback()
        mcs += mc
        client.fetchChunk(0, i, mc);
      }

      mcs.foreach(_.getBytes())
    }
  }

  @Test
  def testGetChunksSerially() {
    //get chunks one by one
    timing(true) {
      for (i <- 1 to 9) {
        val mc = new MyChunkReceivedCallback()
        client.fetchChunk(0, i, mc);
        Assert.assertEquals(1024 * 1024, mc.getBytes().length);
      }
    }
  }

  @Test
  def testGetLargeStream() {
    val out = new MyStreamCallback();
    timing(true) {
      client.stream("large", out);
      out.getBytes();
    }

    Assert.assertEquals(9999999, out.getBytes().length)
  }

  @Test
  def testGetTinyStream() {
    val out = new MyStreamCallback();
    timing(true) {
      client.stream("tiny", out);
      out.getBytes();
    }

    Assert.assertEquals(1024, out.getBytes().length)
  }
}


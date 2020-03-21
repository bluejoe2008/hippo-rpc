package hippo

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import io.netty.buffer.{ByteBuf, Unpooled}
import org.grapheco.commons.util.Logging
import org.grapheco.commons.util.Profiler._
import org.grapheco.hippo._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2020/2/22.
  */
case class SayHelloRequest(str: String) {

}

case class SayHelloResponse(str: String) {

}

case class ReadFileRequest(path: String) {

}

case class ReadFileRequestWithHead(path: String) {

}

case class PutFileRequest(totalLength: Int) {

}

case class PutFileWithForwardRequest(totalLength: Int, port2: Int) {

}

case class PutFileWithForwardResponse(written: Int, nodes: Array[Int]) {

}

case class PutFileResponse(written: Int) {

}

case class GetManyResultsRequest(times: Int, chunkSize: Int, msg: String) {

}

case class GetBufferedResultsRequest(total: Int) {

}

case class ReadFileResponseHead(path: String, totalLength: Int) {

}

object HippoRpcServerForTest extends Logging {
  def createServer(port: Int) = HippoServer.create("test", Map(), new HippoRpcHandler() {

    override def receiveWithStream(extraInput: ByteBuffer, ctx: ReceiveContext): PartialFunction[Any, Unit] = {
      case SayHelloRequest(msg) =>
        ctx.reply(SayHelloResponse(msg.toUpperCase()))

      case PutFileRequest(totalLength) =>
        logger.debug(s"port-${port} received request: PutFileRequest")
        ctx.reply(PutFileResponse(extraInput.remaining()))
        logger.debug(s"port-${port} sent response: PutFileResponse")

      case PutFileWithForwardRequest(totalLength: Int, port2: Int) =>
        logger.debug(s"port-${port} received request: PutFileWithForwardRequest")

        //create new client
        val clientForward = HippoClientFactory.create("test", Map()).createClient("localhost", port2)
        val future = clientForward.ask[PutFileResponse](PutFileRequest(totalLength),
          Unpooled.wrappedBuffer(extraInput.duplicate()))

        logger.debug(s"port-${port} sent request to port-${port2}: PutFileRequest")

        Await.result(future, Duration.Inf);
        logger.debug(s"port-${port} received response from port-${port2}: PutFileResponse")

        ctx.reply(PutFileWithForwardResponse(extraInput.remaining(), Array(port) ++ Array(port2)))
        logger.debug(s"port-${port} sent response: PutFileWithForwardResponse")
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

      case ReadFileRequestWithHead(path) =>
        val fis = new FileInputStream(new File(path))
        val buf = Unpooled.buffer()
        buf.writeBytes(fis.getChannel, new File(path).length().toInt)
        val head = ReadFileResponseHead(path, new File(path).length().toInt)
        CompleteStream.fromByteBuffer(head, buf);
    }
  }, port)
}
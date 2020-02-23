package cn.bluejoe.hippo

import java.io.{File, InputStream}
import java.nio.ByteBuffer
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Predicate

import cn.bluejoe.util.ByteBufferUtils._
import cn.bluejoe.util.Profiler._
import cn.bluejoe.util.{ByteBufferInputStream, Logging, StreamUtils}
import io.netty.buffer.{ByteBuf, ByteBufInputStream, Unpooled}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NettyManagedBuffer}
import org.apache.spark.network.client._
import org.apache.spark.network.server.{NoOpRpcHandler, RpcHandler, StreamManager, TransportServer}
import org.apache.spark.network.util.{MapConfigProvider, TransportConf}

import scala.collection.{JavaConversions, mutable}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Created by bluejoe on 2020/2/17.
  *
  * Hippo Transport Library enhances spark-commons with easy stream management & handling
  *
  *                    ,.I ....
  *                  ... ZO.. .. M  .
  *                  ...=.       .,,.
  *                 .,D           ..?...O.
  *        ..=MD~,.. .,           .O  . O
  *     ..,            +I.        . .,N,  ,$N,,...
  *     O.                   ..    .~.+.      . N, .
  *    7.,, .                8. ..   ...         ,O.
  *    I.DMM,.                .M     .O           ,D
  *    ...MZ .                 ~.   ....          ..N..    :
  *    ?                     .I.    ,..             ..     ,
  *    +.       ,MM=       ..Z.   .,.               .MDMN~$
  *    .I.      .MMD     ..M . .. =..                :. . ..
  *    .,M      ....   .Z. .   +=. .                 ..
  *       ~M~  ... 7D...   .=~.      . .              .
  *        ..$Z... ...+MO..          .M               .
  *                     .M. ,.       .I   .?.        ..
  *                     .~ .. Z=I7.. .7.  .ZM~+N..   ..
  *                     .O   D   . , .M ...M   . .  .: .
  *                     . NNN.I....O.... .. M:. .M,=8..
  *                      ....,...,.  ..   ...   ..
  *
  * HippoServer enhances TransportServer with stream manager(open, streaming fetch, close)
  * HippoClient enhances TransportClient with stream request and result boxing (as Stream[T])
  *
  */
trait ReceiveContext {
  def reply[T](response: T, extra: ((ByteBuf) => Unit)*);
}

case class OpenStreamRequest(streamRequest: Any) {

}

case class OpenStreamResponse(streamId: Long, hasMoreChunks: Boolean) {

}

case class CloseStreamRequest(streamId: Long) {

}

trait ChunkedStream {
  def hasNext(): Boolean;

  def nextChunk(buf: ByteBuf): Unit;

  def close(): Unit
}

trait ChunkedMessageStream[T] extends ChunkedStream {
  override def hasNext(): Boolean;

  def nextChunk(): Iterable[T];

  override def nextChunk(buf: ByteBuf) = {
    buf.writeObject(nextChunk())
  }

  override def close(): Unit
}

object ChunkedStream {
  val executor = Executors.newSingleThreadExecutor();

  def grouped[T](batchSize: Int, iterable: Iterable[T]) = new GroupedMessageStream[T](batchSize, iterable);

  def pooled[T](poolSize: Int, producer: (MessagePool[T]) => Unit)(implicit m: Manifest[T]) =
    new PooledMessageStream[T](executor, poolSize, producer);
}

class GroupedMessageStream[T](batchSize: Int, iterable: Iterable[T]) extends ChunkedMessageStream[T] {
  val it = iterable.iterator.grouped(batchSize)

  override def hasNext() = it.hasNext;

  def nextChunk(): Iterable[T] = it.next();

  override def close(): Unit = {
  }
}

trait MessagePool[T] {
  def push(value: T);
}

class PooledMessageStream[T](executor: ExecutorService, bufferSize: Int, producer: (MessagePool[T]) => Unit)
                            (implicit m: Manifest[T]) extends ChunkedMessageStream[T] {
  val buffer = new ArrayBlockingQueue[Any](bufferSize);
  val END_OF_STREAM = new Object();

  val future = executor.submit(new Runnable {
    override def run(): Unit = {
      producer(new MessagePool[T]() {
        def push(value: T) = buffer.put(value)
      })

      buffer.put(END_OF_STREAM);
    }
  })

  override def hasNext(): Boolean = !(future.isDone && buffer.isEmpty)

  def nextChunk(): Iterable[T] = {
    val first = buffer.take()
    if (first == END_OF_STREAM) {
      Iterable.empty[T]
    }
    else {
      val list = new java.util.ArrayList[Any]();
      buffer.drainTo(list)
      list.add(0, first)
      list.removeIf(new Predicate[Any]() {
        override def test(t: Any) = t == END_OF_STREAM
      })

      JavaConversions.collectionAsScalaIterable(list).map(_.asInstanceOf[T])
    }
  }

  override def close(): Unit = future.cancel(true)
}

trait CompleteStream {
  def createManagedBuffer(): ManagedBuffer;
}

object CompleteStream {
  def fromFile(conf: TransportConf, file: File, offset: Long, length: Long): CompleteStream = new CompleteStream() {
    override def createManagedBuffer(): ManagedBuffer =
      new FileSegmentManagedBuffer(conf, file, offset, length);
  }

  def fromFile(conf: TransportConf, file: File): CompleteStream = {
    fromFile(conf, file, 0, file.length().toInt);
  }

  def fromByteBuf(produce: (ByteBuf) => Unit): CompleteStream = new CompleteStream() {
    override def createManagedBuffer(): ManagedBuffer = {
      val buf = Unpooled.buffer()
      produce(buf)
      new NettyManagedBuffer(buf)
    }
  }
}

trait HippoRpcHandler {
  def openCompleteStream(): PartialFunction[Any, CompleteStream] = {
    throw new UnsupportedOperationException();
  }

  def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    throw new UnsupportedOperationException();
  }

  def receiveWithStream(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
    throw new UnsupportedOperationException();
  }
}

class HippoStreamManagerAdapter(var handler: HippoRpcHandler) extends StreamManager {
  val streamIdGen = new AtomicLong(System.currentTimeMillis());
  val streams = mutable.Map[Long, ChunkedStream]();

  override def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
    if (logger.isTraceEnabled)
      logger.trace(s"get chunk: streamId=$streamId, chunkIndex=$chunkIndex")

    //1-2ms
    timing(false) {
      val buf = Unpooled.buffer(1024)
      val stream = streams(streamId)
      _writeNextChunk(buf, streamId, chunkIndex, stream)

      new NettyManagedBuffer(buf)
    }
  }

  override def openStream(streamId: String): ManagedBuffer = {
    val request = StreamUtils.deserializeObject(StreamUtils.base64.decode(streamId))
    handler.openCompleteStream()(request).createManagedBuffer();
  }

  private def _writeNextChunk(buf: ByteBuf, streamId: Long, chunkIndex: Int, stream: ChunkedStream) {
    buf.writeLong(streamId).writeInt(0).writeByte(1)

    //write next chunk
    if (stream.hasNext()) {
      stream.nextChunk(buf)
    }

    if (!stream.hasNext()) {
      buf.setByte(8 + 4, 0)
      stream.close()
      streams.remove(streamId)
    }
  }

  def handleOpenStreamRequest(streamRequest: Any, callback: RpcResponseCallback) {
    val streamId: Long = streamIdGen.getAndIncrement();
    val stream = handler.openChunkedStream()(streamRequest)
    val output = Unpooled.buffer(1024);
    output.writeObject(OpenStreamResponse(streamId, stream.hasNext()));
    if (stream.hasNext()) {
      streams(streamId) = stream
    }

    callback.onSuccess(output.nioBuffer())
  }

  def handleCloseStreamRequest(streamId: Long, callback: RpcResponseCallback): Unit = {
    streams(streamId).close
    streams -= streamId
  }

  def handleRequestWithStream(streamRequest: Any, extra: ByteBuffer, callback: RpcResponseCallback): Unit = {
    val ctx = new ReceiveContext {
      override def reply[T](response: T, extra: ((ByteBuf) => Unit)*) = {
        replyBuffer((buf: ByteBuf) => {
          buf.writeObject(response)
          extra.foreach(_.apply(buf))
        })
      }

      def replyBuffer(writeResponse: ((ByteBuf) => Unit)) = {
        val output = Unpooled.buffer(1024);
        writeResponse.apply(output)
        callback.onSuccess(output.nioBuffer())
      }
    }

    handler.receiveWithStream(extra, ctx)(streamRequest)
  }
}

object HippoServer extends Logging {
  //WEIRLD: this makes next Upooled.buffer() call run fast
  Unpooled.buffer(1)

  def create(module: String, rpcHandler: HippoRpcHandler, port: Int = 0, host: String = null): HippoServer = {
    val configProvider = new MapConfigProvider(JavaConversions.mapAsJavaMap(Map()))
    val conf: TransportConf = new TransportConf(module, configProvider)

    val handler: RpcHandler = new RpcHandler() {
      val streamManagerAdapter = new HippoStreamManagerAdapter(rpcHandler);

      override def getStreamManager: StreamManager = streamManagerAdapter

      override def receive(client: TransportClient, input: ByteBuffer, callback: RpcResponseCallback) {
        try {
          val message = input.readObject();
          message match {
            case OpenStreamRequest(streamRequest) =>
              streamManagerAdapter.handleOpenStreamRequest(streamRequest, callback)

            case CloseStreamRequest(streamId) =>
              streamManagerAdapter.handleCloseStreamRequest(streamId, callback)

            case _ => {
              streamManagerAdapter.handleRequestWithStream(message, input, callback)
            }
          }
        }
        catch {
          case e: Throwable => callback.onFailure(e)
        }
      }
    }

    val context: TransportContext = new TransportContext(conf, handler)
    new HippoServer(context.createServer(host, port, new util.ArrayList()), conf)
  }
}

class HippoServer(server: TransportServer, val conf: TransportConf) {
  def getPort() = server.getPort()

  def close() = server.close()
}

object HippoClient extends Logging {
  //WEIRLD: this makes next Upooled.buffer() call run fast
  Unpooled.buffer(1)

  val clientFactoryMap = mutable.Map[String, TransportClientFactory]();
  val executionContext: ExecutionContext = ExecutionContext.global

  def getClientFactory(module: String) = {
    clientFactoryMap.getOrElseUpdate(module, {
      val configProvider = new MapConfigProvider(JavaConversions.mapAsJavaMap(Map()))
      val conf: TransportConf = new TransportConf(module, configProvider)
      val context: TransportContext = new TransportContext(conf, new NoOpRpcHandler())
      context.createClientFactory
    }
    )
  }

  def create(module: String, remoteHost: String, remotePort: Int): HippoClient = {
    new HippoClient(getClientFactory(module).createClient(remoteHost, remotePort))
  }
}

trait HippoStreamingClient {
  def getChunkedStream[T](request: Any)(implicit m: Manifest[T]): Stream[T]

  def getInputStream(request: Any): InputStream

  def getChunkedInputStream(request: Any): InputStream
}

trait HippoRpcClient {
  def ask[T](message: Any, extra: ((ByteBuf) => Unit)*)(implicit m: Manifest[T]): Future[T]
}

class HippoClient(client: TransportClient) extends HippoStreamingClient with HippoRpcClient with Logging {
  def close() = client.close()

  def ask[T](message: Any, extra: ((ByteBuf) => Unit)*)(implicit m: Manifest[T]): Future[T] = {
    _sendAndReceive({ buf =>
      buf.writeObject(message)
      extra.foreach(_.apply(buf))
    }, _.readObject().asInstanceOf[T])
  }

  def getInputStream(request: Any): InputStream = {
    _getInputStream(StreamUtils.base64.encodeAsString(
      StreamUtils.serializeObject(request)))
  }

  override def getChunkedInputStream(request: Any): InputStream = {
    //12ms
    val iter: Iterator[InputStream] = timing(false) {
      _getChunkedStream[InputStream](request, (buf: ByteBuffer) =>
        new ByteBufferInputStream(buf)).iterator
    }

    //1ms
    timing(false) {
      StreamUtils.concatChunks {
        if (iter.hasNext) {
          Some(iter.next)
        }
        else {
          None
        }
      }
    }
  }

  override def getChunkedStream[T](request: Any)(implicit m: Manifest[T]): Stream[T] = {
    val stream = _getChunkedStream(request, { buf =>
      if (buf.hasRemaining) {
        buf.readObject().asInstanceOf[Iterable[T]]
      }
      else {
        Iterable.empty[T]
      }
    })

    stream.flatMap(_.toIterable)
  }

  private case class ChunkResponse[T](streamId: Long, chunkIndex: Int, hasNext: Boolean, chunk: T) {

  }

  private class MyChunkReceivedCallback[T](consumeResponse: (ByteBuffer) => T) extends ChunkReceivedCallback {
    val latch = new CountDownLatch(1);

    var res: ChunkResponse[T] = _
    var err: Throwable = null

    override def onFailure(chunkIndex: Int, e: Throwable): Unit = {
      err = e;
      latch.countDown();
    }

    override def onSuccess(chunkIndex: Int, buffer: ManagedBuffer): Unit = {
      try {
        val buf = buffer.nioByteBuffer()
        res = _readChunk(buf, consumeResponse)
      }
      catch {
        case e: Throwable =>
          err = e;
      }

      latch.countDown();
    }

    def await(): ChunkResponse[T] = {
      latch.await()
      if (err != null)
        throw err;

      res
    }
  }

  private class MyRpcResponseCallback[T](consumeResponse: (ByteBuffer) => T) extends RpcResponseCallback {
    val latch = new CountDownLatch(1);

    var res: Any = null
    var err: Throwable = null

    override def onFailure(e: Throwable): Unit = {
      err = e
      latch.countDown();
    }

    override def onSuccess(response: ByteBuffer): Unit = {
      try {
        res = consumeResponse(response)
      }
      catch {
        case e: Throwable => err = e
      }

      latch.countDown();
    }

    def await(): T = {
      latch.await()
      if (err != null)
        throw err;

      res.asInstanceOf[T]
    }
  }

  private def _getChunkedStream[T](request: Any, consumeResponse: (ByteBuffer) => T)(implicit m: Manifest[T]): Stream[T] = {
    //send start stream request
    //2ms
    val OpenStreamResponse(streamId, hasMoreChunks) =
      Await.result(ask[OpenStreamResponse](OpenStreamRequest(request)), Duration.Inf);

    if (!hasMoreChunks) {
      Stream.empty[T]
    }
    else {
      _buildStream(streamId, 0, consumeResponse)
    }
  }

  private def _readChunk[T](buf: ByteBuffer, consumeResponse: (ByteBuffer) => T): ChunkResponse[T] = {
    ChunkResponse[T](buf.getLong(),
      buf.getInt(),
      buf.get() != 0,
      consumeResponse(buf))
  }

  private def _buildStream[T](streamId: Long, chunkIndex: Int, consumeResponse: (ByteBuffer) => T): Stream[T] = {
    if (logger.isTraceEnabled)
      logger.trace(s"build stream: streamId=$streamId, chunkIndex=$chunkIndex")

    val callback = new MyChunkReceivedCallback[T](consumeResponse);
    val ChunkResponse(_, _, hasMoreChunks, values) = timing(false) {
      client.fetchChunk(streamId, chunkIndex, callback)
      callback.await()
    }

    Stream.cons(values,
      if (hasMoreChunks) {
        _buildStream(streamId, chunkIndex + 1, consumeResponse)
      }
      else {
        Stream.empty
      })
  }

  private def _getInputStream(streamId: String): InputStream = {
    val queue = new ArrayBlockingQueue[AnyRef](1);
    val END_OF_STREAM = new Object

    client.stream(streamId, new StreamCallback {
      override def onData(streamId: String, buf: ByteBuffer): Unit = {
        queue.put(Unpooled.copiedBuffer(buf));
      }

      override def onComplete(streamId: String): Unit = {
        queue.put(END_OF_STREAM)
      }

      override def onFailure(streamId: String, cause: Throwable): Unit = {
        throw cause;
      }
    })

    StreamUtils.concatChunks {
      val buffer = queue.take()
      if (buffer == END_OF_STREAM)
        None
      else {
        Some(new ByteBufInputStream(buffer.asInstanceOf[ByteBuf]))
      }
    }
  }

  private def _sendAndReceive[T](produceRequest: (ByteBuf) => Unit, consumeResponse: (ByteBuffer) => T)(implicit m: Manifest[T]): Future[T] = {
    val buf = Unpooled.buffer(1024)
    produceRequest(buf)
    val callback = new MyRpcResponseCallback[T](consumeResponse);
    client.sendRpc(buf.nioBuffer, callback)
    implicit val ec: ExecutionContext = HippoClient.executionContext
    Future {
      callback.await()
    }
  }
}
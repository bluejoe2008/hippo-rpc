package hippo

import java.nio.ByteBuffer
import java.util
import cn.bluejoe.util.ByteBufferUtils._
import io.netty.buffer.Unpooled
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.{NioManagedBuffer, ManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{StreamManager, RpcHandler}
import org.apache.spark.network.util.{TransportConf, MapConfigProvider}

import scala.collection.JavaConversions

/**
  * Created by bluejoe on 2020/2/23.
  */
object SparkRpcServerForTest {
  val server = {
    val handler: RpcHandler = new RpcHandler() {
      override def receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback) {
        if (false) {
          val bb = Unpooled.wrappedBuffer(message)

          val res = bb.readByte() match {
            case 1 => Unpooled.buffer().writeString(bb.readString().toUpperCase).nioBuffer()
            case 2 => Unpooled.buffer().writeInt(100).nioBuffer()
          }

          callback.onSuccess(res)
        }
        else {
          val res = message.get() match {
            case 1 => Unpooled.buffer().writeString(message.readString().toUpperCase).nioBuffer()
            case 2 => Unpooled.buffer().writeInt(100).nioBuffer()
          }

          callback.onSuccess(res)
        }
      }

      override def receive(client: TransportClient, message: ByteBuffer) {
        println(message.get(), message.readString(), message.getLong);
      }

      override def getStreamManager: StreamManager = {
        new StreamManager() {
          override def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
            //println(s"received chunk request: $chunkIndex");
            new NioManagedBuffer(ByteBuffer.allocate(1024 * 1024))
          }

          override def openStream(streamId: String): ManagedBuffer = {
            streamId match {
              case "large" => new NioManagedBuffer(ByteBuffer.allocate(9999999));
              case "tiny" => new NioManagedBuffer(ByteBuffer.allocate(1024));
            }
          }
        }
      }
    }

    val confProvider = new MapConfigProvider(JavaConversions.mapAsJavaMap(Map()))
    val conf: TransportConf = new TransportConf("test", confProvider)
    val context: TransportContext = new TransportContext(conf, handler)
    context.createServer("localhost", 0, new util.ArrayList())
  }
}

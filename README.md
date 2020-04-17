# hippo-rpc

Hippo Transport Library enhances spark-commons with easy stream management & handling

[![GitHub issues](https://img.shields.io/github/issues/bluejoe2008/hippo-rpc.svg)](https://github.com/bluejoe2008/hippo-rpc/issues)
[![GitHub forks](https://img.shields.io/github/forks/bluejoe2008/hippo-rpc.svg)](https://github.com/bluejoe2008/hippo-rpc/network)
[![GitHub stars](https://img.shields.io/github/stars/bluejoe2008/hippo-rpc.svg)](https://github.com/bluejoe2008/hippo-rpc/stargazers)
[![GitHub license](https://img.shields.io/github/license/bluejoe2008/hippo-rpc.svg)](https://github.com/bluejoe2008/hippo-rpc/blob/master/LICENSE)


                    ,.I ....
                  ... ZO.. .. M  .
                  ...=.       .,,.
                 .,D           ..?...O.
        ..=MD~,.. .,           .O  . O
     ..,            +I.        . .,N,  ,$N,,...
     O.                   ..    .~.+.      . N, .
    7.,, .                8. ..   ...         ,O.
    I.DMM,.                .M     .O           ,D
    ...MZ .                 ~.   ....          ..N..    :
    ?                     .I.    ,..             ..     ,
    +.       ,MM=       ..Z.   .,.               .MDMN~$
    .I.      .MMD     ..M . .. =..                :. . ..
    .,M      ....   .Z. .   +=. .                 ..
       ~M~  ... 7D...   .=~.      . .              .
        ..$Z... ...+MO..          .M               .
                     .M. ,.       .I   .?.        ..
                     .~ .. Z=I7.. .7.  .ZM~+N..   ..
                     .O   D   . , .M ...M   . .  .: .
                     . NNN.I....O.... .. M:. .M,=8..
                      ....,...,.  ..   ...   ..

## build & install

```
mvn clean compile install
```

## using hippo-rpc

add repository in `pom.xml`:

```
    <dependency>
        <groupId>org.grapheco</groupId>
        <artifactId>hippo-rpc</artifactId>
        <version>0.1.1</version>
    </dependency>
```

 `HippoServer` enhances TransportServer with stream manager(open, streaming fetch, close)
 ```
   val server = HippoServer.create("test", new HippoRpcHandler() {
   ...
   }, 1224)

 ```
 `HippoClient` enhances TransportClient with stream request and result boxing (as Stream[T])
 ```
   val client = HippoClient.create("test", "localhost", 1224)
 ```

more examples, see <https://github.com/bluejoe2008/hippo-rpc/blob/master/src/test/scala/hippo/HippoRpcTest.scala>

## methods of HippoClient

* `ask[T](message: Any, consumeResponse: (ByteBuffer) => T): Future[T]`
asks for an response, `message` as request, use `consumeResponse` to parse response message
* `askWithBuffer[T](message: Any, extra: ByteBuf*): Future[T]`
asks for an response, sends `message` and `extra` as request
* `getInputStream(request: Any, waitStreamTimeout: Duration): InputStream`
gets for an `InputStream`, e.g, stream of a remote file
* `getChunkedStream[T](request: Any, waitStreamTimeout: Duration): Stream[T]`
gets results as a `Stream`, e.g, results of a SQL execution
* `getChunkedInputStream(request: Any, waitStreamTimeout: Duration): InputStream`
gets for an `InputStream` chunk by chunk, e.g, stream of a remote file

## using hippo-rpc with spark-rpc

`spark-rpc`, or `kraps-rpc` uses a messaging mechanism to improve the performance of RPC calling.

`HippoRpcEnvFactory` is provided to enable RPC calls on `kraps-rpc`, and stream handling on `hippo-rpc`.

`HippoRpcEnv` enhances `NettyRpcEnv` with stream handling functions, besides RPC messaging
 usage of `HippoRpcEnv` is like that of `NettyRpcEnv`:

```
   rpcEnv = HippoRpcEnvFactory.create(config)
   val endpoint: RpcEndpoint = new FileRpcEndpoint(rpcEnv)
   rpcEnv.setupEndpoint("endpoint-name", endpoint)
   rpcEnv.setRpcHandler(new MyRpcHandler())
   ...
   ...
   val endPointRef = rpcEnv.setupEndpointRef(RpcAddress(...), "...");
```

to provide a customized `HippoRpcHandler`, a set of methods will be implemented:
* `openCompleteStream`: provides a stream for given request
* `openChunkedStream`: provides a chunkable stream for given request
* `receiveWithStream`: defines how to respond on received request buffer

more examples, see <https://github.com/bluejoe2008/hippo-rpc/blob/master/src/test/scala/hippo/HippoRpcEnvFactoryTest.scala>

## methods of HippoEndpointRef

* `ask[T](message: Any): Future[T]`
asks for an response, `message` as request
* `ask[T](message: Any, timeout: RpcTimeout): Future[T]`
asks for an response, with max timeout limit
* `ask[T](message: Any, timeout: Duration): Future[T]`
asks for an response, with max timeout limit
* `send(message: Any): Unit`
sends a message
* `ask[T](message: Any, consumeResponse: (ByteBuffer) => T): Future[T]`
asks for an response, `message` as request, use `consumeResponse` to parse response message
* `askWithBuffer[T](message: Any, extra: ByteBuf*): Future[T]`
asks for an response, sends `message` and `extra` as request
* `getInputStream(request: Any, waitStreamTimeout: Duration): InputStream`
gets for an `InputStream`, e.g, stream of a remote file
* `getChunkedStream[T](request: Any, waitStreamTimeout: Duration): Stream[T]`
gets results as a `Stream`, e.g, results of a SQL execution
* `getChunkedInputStream(request: Any, waitStreamTimeout: Duration): InputStream`
gets for an `InputStream` chunk by chunk, e.g, stream of a remote file

## dependencies

* `spark-commons`: spark common library, https://github.com/apache/spark
* `kraps-rpc`: A RPC framework leveraging Spark RPC module, https://github.com/neoremind/kraps-rpc

## licensing

`hippo-rpc` is licensed under the BSD 2-Clause "Simplified" License.

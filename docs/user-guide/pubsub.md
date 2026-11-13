## Publish/Subscribe

Lettuce provides support for Publish/Subscribe on Redis Standalone and
Redis Cluster connections. The connection is notified on
message/subscribed/unsubscribed events after subscribing to channels or
patterns. [Synchronous](connecting-redis.md#basic-usage), [asynchronous](async-api.md)
and [reactive](reactive-api.md) APIs are provided to interact with Redis
Publish/Subscribe features.

### Subscribing

A connection can notify multiple listeners that implement
`RedisPubSubListener` (Lettuce provides a `RedisPubSubAdapter` for
convenience). All listener registrations are kept within the
`StatefulRedisPubSubConnection`/`StatefulRedisClusterConnection`.

``` java
StatefulRedisPubSubConnection<String, String> connection = client.connectPubSub()
connection.addListener(new RedisPubSubListener<String, String>() { ... })

RedisPubSubCommands<String, String> sync = connection.sync();
sync.subscribe("channel");

// application flow continues
```

!!! NOTE
    Don’t issue blocking calls (includes synchronous API calls to Lettuce)
    from inside of Pub/Sub callbacks as this would block the EventLoop. If
    you need to fetch data from Redis from inside a callback, please use
    the asynchronous API.

``` java
StatefulRedisPubSubConnection<String, String> connection = client.connectPubSub()
connection.addListener(new RedisPubSubListener<String, String>() { ... })

RedisPubSubAsyncCommands<String, String> async = connection.async();
RedisFuture<Void> future = async.subscribe("channel");

// application flow continues
```

### Reactive API

The reactive API provides hot `Observable`s to listen on
`ChannelMessage`s and `PatternMessage`s. The `Observable`s receive all
inbound messages. You can do filtering using the observable chain if you
need to filter out the interesting ones, The `Observable` stops
triggering events when the subscriber unsubscribes from it.

``` java
StatefulRedisPubSubConnection<String, String> connection = client.connectPubSub()

RedisPubSubReactiveCommands<String, String> reactive = connection.reactive();
reactive.subscribe("channel").subscribe();

reactive.observeChannels().doOnNext(patternMessage -> {...}).subscribe()

// application flow continues
```

### Kotlin coroutines

Pub/Sub connections provide a coroutine API through
`io.lettuce.core.pubsub.coroutines`. This API requires the same coroutine
dependencies as the [Kotlin API](kotlin-api.md) and opt-in to
`ExperimentalLettuceCoroutinesApi`.

```kotlin
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.api.coroutines
import io.lettuce.core.pubsub.coroutines
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.first

@OptIn(ExperimentalLettuceCoroutinesApi::class)
suspend fun receiveMessage(client: RedisClient) = coroutineScope {
    client.connectPubSub().use { subscription ->
        client.connect().use { publisher ->
            val commands = subscription.coroutines()
            val received = async(start = CoroutineStart.UNDISPATCHED) {
                commands.observeChannels().first()
            }

            commands.subscribe("events")
            publisher.coroutines().publish("events", "hello")
            println(received.await().message)
            commands.unsubscribe("events")
        }
    }
}
```

Start collecting before registering subscriptions so that the message listener
is ready when messages arrive. The subscription functions suspend until Redis
acknowledges the registration; they do not return the messages themselves.

Use `psubscribe` with `observePatterns` for pattern subscriptions, and
`ssubscribe` with `observeShardChannels` for shard subscriptions (Redis 7.0+).
On Redis Cluster, shard channels in a single subscription command must share a
hash slot. The basic Pub/Sub API also accepts cluster Pub/Sub connections;
cluster node-selection operations remain available through the Java APIs.

Each collector owns a separate listener. Cancelling a collector removes that
listener, but does not unsubscribe other collectors or close the connection.
Use `unsubscribe`, `punsubscribe`, or `sunsubscribe` to unregister server
subscriptions. Use a separate publishing connection as shown above, since
RESP2 restricts the commands allowed on a subscribed connection.

Message flows use the reactive API's unbounded `BUFFER` overflow strategy by
default. Pass another `FluxSink.OverflowStrategy`, such as `ERROR`, `DROP`, or
`LATEST`, to control behavior when messages arrive faster than they are consumed.
Calling `buffer` on the Flow alone does not bound the reactive buffer.

### Redis Cluster

Redis Cluster support Publish/Subscribe but requires some attention in
general. User-space Pub/Sub messages (Calling `PUBLISH`) are broadcasted
across the whole cluster regardless of subscriptions to particular
channels/patterns. This behavior allows connecting to an arbitrary
cluster node and registering a subscription. The client isn’t required
to connect to the node where messages were published.

A cluster-aware Pub/Sub connection is provided by
`RedisClusterClient.connectPubSub()` allowing to listen for cluster
reconfiguration and reconnect if the topology changes.

``` java
StatefulRedisClusterPubSubConnection<String, String> connection = clusterClient.connectPubSub()
connection.addListener(new RedisPubSubListener<String, String>() { ... })

RedisPubSubCommands<String, String> sync = connection.sync();
sync.subscribe("channel");
```

Redis Cluster also makes a distinction between user-space and key-space
messages. Key-space notifications (Pub/Sub messages for key-activity)
stay node-local and are not broadcasted across the Redis Cluster. A
notification about, e.g. an expiring key, stays local to the node on
which the key expired.

Clients that are interested in keyspace notifications must subscribe to
the appropriate node (or nodes) to receive these notifications. You can
either use `RedisClient.connectPubSub()` to establish Pub/Sub
connections to the individual nodes or use `RedisClusterClient`'s
message propagation and NodeSelection API to get a managed set of
connections.

``` java
StatefulRedisClusterPubSubConnection<String, String> connection = clusterClient.connectPubSub()
connection.addListener(new RedisClusterPubSubListener<String, String>() { ... })
connection.setNodeMessagePropagation(true);

RedisPubSubCommands<String, String> sync = connection.sync();
sync.masters().commands().subscribe("__keyspace@0__:*");
```

There are two things to pay special attention to:

1.  Replication: Keys replicated to replica nodes, especially
    considering expiry, generate keyspace events on all nodes holding
    the key. If a key expires and it is replicated, it will expire on
    the master and all replicas. Each Redis server will emit keyspace
    events. Subscribing to non-master nodes, therefore, will let your
    application see multiple events of the same type for the same key
    because of Redis distributed nature.

2.  Topology Changes: Subscriptions are issued either by using the
    NodeSelection API or by calling `subscribe(…)` on the individual
    cluster node connections. Subscription registrations are not
    propagated to new nodes that are added on a topology change.

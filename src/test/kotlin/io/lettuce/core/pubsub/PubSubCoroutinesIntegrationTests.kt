package io.lettuce.core.pubsub

import io.lettuce.TestTags
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.BaseRedisCoroutinesCommands
import io.lettuce.core.cluster.ClusterTestSettings
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.coroutines
import io.lettuce.core.pubsub.coroutines
import io.lettuce.test.settings.TestSettings
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import java.io.Closeable

@Tag(TestTags.INTEGRATION_TEST)
@OptIn(ExperimentalLettuceCoroutinesApi::class)
class PubSubCoroutinesIntegrationTests {
    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun channelSubscriptionsAndCollectorsHaveSeparateLifetimes(cluster: Boolean) = withConnections(cluster) { connection, publisher ->
        val commands = connection.coroutines()
        val received = async(start = CoroutineStart.UNDISPATCHED) { commands.observeChannels().take(2).toList() }
        commands.subscribe("lettuce-2775-one", "lettuce-2775-two")
        assertPublishCount(1L, publisher.publish("lettuce-2775-one", "first"), cluster)
        assertPublishCount(1L, publisher.publish("lettuce-2775-two", "second"), cluster)
        assertEquals(listOf("first", "second"), received.await().map { it.message })
        val afterCollection = async(start = CoroutineStart.UNDISPATCHED) { commands.observeChannels().first() }
        assertPublishCount(1L, publisher.publish("lettuce-2775-one", "after collection"), cluster)
        assertEquals("after collection", afterCollection.await().message)
        UnsubscribeNotifications(connection).use { notifications ->
            commands.unsubscribe("lettuce-2775-one")
            assertEquals("lettuce-2775-one" to 1L, notifications.events.receive())
            assertPublishCount(0L, publisher.publish("lettuce-2775-one", "after unsubscribe"), cluster)
            val remaining = async(start = CoroutineStart.UNDISPATCHED) { commands.observeChannels().first() }
            assertPublishCount(1L, publisher.publish("lettuce-2775-two", "still subscribed"), cluster)
            assertEquals("still subscribed", remaining.await().message)
            commands.unsubscribe()
            assertEquals("lettuce-2775-two" to 0L, notifications.events.receive())
            assertPublishCount(0L, publisher.publish("lettuce-2775-two", "after unsubscribe all"), cluster)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun patternSubscriptionsPreserveTheMatchingPattern(cluster: Boolean) = withConnections(cluster) { connection, publisher ->
        val commands = connection.coroutines()
        val received = async(start = CoroutineStart.UNDISPATCHED) { commands.observePatterns().take(2).toList() }
        commands.psubscribe("lettuce-2775-one-*", "lettuce-2775-two-*")
        assertPublishCount(1L, publisher.publish("lettuce-2775-one-channel", "first"), cluster)
        assertPublishCount(1L, publisher.publish("lettuce-2775-two-channel", "second"), cluster)
        val messages = received.await()
        assertEquals(listOf("lettuce-2775-one-*", "lettuce-2775-two-*"), messages.map { it.pattern })
        assertEquals(listOf("lettuce-2775-one-channel", "lettuce-2775-two-channel"), messages.map { it.channel })
        assertEquals(listOf("first", "second"), messages.map { it.message })
        val afterCollection = async(start = CoroutineStart.UNDISPATCHED) { commands.observePatterns().first() }
        publisher.publish("lettuce-2775-one-channel", "after collection")
        assertEquals("after collection", afterCollection.await().message)
        UnsubscribeNotifications(connection).use { notifications ->
            commands.punsubscribe("lettuce-2775-one-*")
            assertEquals("lettuce-2775-one-*" to 1L, notifications.events.receive())
            assertPublishCount(0L, publisher.publish("lettuce-2775-one-channel", "after unsubscribe"), cluster)
            val remaining = async(start = CoroutineStart.UNDISPATCHED) { commands.observePatterns().first() }
            assertPublishCount(1L, publisher.publish("lettuce-2775-two-channel", "still subscribed"), cluster)
            val message = remaining.await()
            assertEquals("lettuce-2775-two-*", message.pattern)
            assertEquals("still subscribed", message.message)
            commands.punsubscribe()
            assertEquals("lettuce-2775-two-*" to 0L, notifications.events.receive())
            assertPublishCount(0L, publisher.publish("lettuce-2775-two-channel", "after unsubscribe all"), cluster)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun shardSubscriptionsReceiveShardMessages(cluster: Boolean) = withConnections(cluster) { connection, publisher ->
        val commands = connection.coroutines()
        val received = async(start = CoroutineStart.UNDISPATCHED) { commands.observeShardChannels().take(2).toList() }
        commands.ssubscribe("{lettuce-2775}:one", "{lettuce-2775}:two")
        assertEquals(1L, publisher.spublish("{lettuce-2775}:one", "first"))
        assertEquals(1L, publisher.spublish("{lettuce-2775}:two", "second"))
        assertEquals(listOf("first", "second"), received.await().map { it.message })
        commands.sunsubscribe("{lettuce-2775}:one")
        assertEquals(0L, publisher.spublish("{lettuce-2775}:one", "after unsubscribe"))
        assertEquals(1L, publisher.spublish("{lettuce-2775}:two", "still subscribed"))
        commands.sunsubscribe("{lettuce-2775}:two")
        assertEquals(0L, publisher.spublish("{lettuce-2775}:two", "after unsubscribe all"))
    }

    private class UnsubscribeNotifications(
        private val connection: StatefulRedisPubSubConnection<String, String>
    ) : RedisPubSubAdapter<String, String>(), Closeable {
        val events = Channel<Pair<String, Long>>(Channel.UNLIMITED)

        init {
            connection.addListener(this)
        }

        override fun unsubscribed(channel: String, count: Long) {
            events.trySend(channel to count)
        }

        override fun punsubscribed(pattern: String, count: Long) {
            events.trySend(pattern to count)
        }

        override fun close() {
            connection.removeListener(this)
            events.cancel()
        }
    }

    private fun assertPublishCount(expected: Long, actual: Long?, cluster: Boolean) {
        // PUBLISH counts subscribers on the publishing node, not across the cluster.
        if (!cluster) {
            assertEquals(expected, actual)
        }
    }

    private fun withConnections(
        cluster: Boolean,
        block: suspend CoroutineScope.(StatefulRedisPubSubConnection<String, String>, BaseRedisCoroutinesCommands<String, String>) -> Unit
    ) = runBlocking {
        withTimeout(20000) {
            if (cluster) {
                val client = RedisClusterClient.create(RedisURI.create(ClusterTestSettings.host, ClusterTestSettings.port1))
                try {
                    client.connect().use { publisher ->
                        client.connectPubSub().use { connection -> block(connection, publisher.coroutines()) }
                    }
                } finally {
                    client.shutdown()
                }
            } else {
                val client = RedisClient.create(RedisURI.create(TestSettings.host(), TestSettings.port()))
                try {
                    client.connect().use { publisher ->
                        client.connectPubSub().use { connection -> block(connection, publisher.coroutines()) }
                    }
                } finally {
                    client.shutdown()
                }
            }
        }
    }
}

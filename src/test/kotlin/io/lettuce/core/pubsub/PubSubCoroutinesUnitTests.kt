package io.lettuce.core.pubsub

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisCommandExecutionException
import io.lettuce.core.pubsub.api.coroutines.RedisPubSubCoroutinesCommandsImpl
import io.lettuce.core.pubsub.api.reactive.ChannelMessage
import io.lettuce.core.pubsub.api.reactive.PatternMessage
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import reactor.core.publisher.Mono
import reactor.core.Exceptions
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

@OptIn(ExperimentalLettuceCoroutinesApi::class)
@Timeout(10)
class PubSubCoroutinesUnitTests {
    @Suppress("UNCHECKED_CAST")
    private val connection = Mockito.mock(StatefulRedisPubSubConnection::class.java) as StatefulRedisPubSubConnection<String, String>
    @Suppress("UNCHECKED_CAST")
    private val reactive = Mockito.mock(RedisPubSubReactiveCommands::class.java) as RedisPubSubReactiveCommands<String, String>
    private val commands = RedisPubSubCoroutinesCommandsImpl(connection, reactive)

    @Test
    fun subscriptionCommandsCompleteWithoutAValue() = runBlocking<Unit> {
        Mockito.`when`(reactive.subscribe("one", "two")).thenReturn(Mono.empty())
        Mockito.`when`(reactive.unsubscribe()).thenReturn(Mono.empty())
        Mockito.`when`(reactive.psubscribe("one*", "two*")).thenReturn(Mono.empty())
        Mockito.`when`(reactive.punsubscribe()).thenReturn(Mono.empty())
        Mockito.`when`(reactive.ssubscribe("{slot}:one", "{slot}:two")).thenReturn(Mono.empty())
        Mockito.`when`(reactive.sunsubscribe("{slot}:one", "{slot}:two")).thenReturn(Mono.empty())
        Mockito.`when`(reactive.ping()).thenReturn(Mono.just("PONG"))

        commands.subscribe("one", "two")
        commands.unsubscribe()
        commands.psubscribe("one*", "two*")
        commands.punsubscribe()
        commands.ssubscribe("{slot}:one", "{slot}:two")
        commands.sunsubscribe("{slot}:one", "{slot}:two")
        assertEquals("PONG", commands.ping())

        verify(reactive).subscribe("one", "two")
        verify(reactive).unsubscribe()
        verify(reactive).psubscribe("one*", "two*")
        verify(reactive).punsubscribe()
        verify(reactive).ssubscribe("{slot}:one", "{slot}:two")
        verify(reactive).sunsubscribe("{slot}:one", "{slot}:two")
    }

    @Test
    fun subscriptionErrorsArePropagated() = runBlocking {
        val error = RedisCommandExecutionException("subscription failed")
        Mockito.`when`(reactive.subscribe("channel")).thenReturn(Mono.error(error))
        val thrown = assertFailsWith<RedisCommandExecutionException> { commands.subscribe("channel") }
        assertEquals(error.message, thrown.message)
    }

    @Test
    fun cancellingASubscriptionCommandCancelsItsPublisher() = runBlocking {
        val cancelled = AtomicBoolean()
        Mockito.`when`(reactive.subscribe("channel")).thenReturn(Mono.never<Void>().doOnCancel { cancelled.set(true) })
        val job = launch(start = CoroutineStart.UNDISPATCHED) { commands.subscribe("channel") }
        job.cancelAndJoin()
        assertTrue(cancelled.get())
    }

    @Test
    fun channelAndPatternFlowsUseTheRequestedOverflowStrategy() = runBlocking {
        val channelMessage = ChannelMessage("channel", "value")
        val patternMessage = PatternMessage("pattern*", "channel", "value")
        Mockito.`when`(reactive.observeChannels(FluxSink.OverflowStrategy.LATEST)).thenReturn(Flux.just(channelMessage))
        Mockito.`when`(reactive.observePatterns(FluxSink.OverflowStrategy.ERROR)).thenReturn(Flux.just(patternMessage))
        assertSame(channelMessage, commands.observeChannels(FluxSink.OverflowStrategy.LATEST).first())
        assertSame(patternMessage, commands.observePatterns(FluxSink.OverflowStrategy.ERROR).first())
    }

    @Test
    fun shardCollectorsHaveIndependentListeners() = runBlocking {
        val listeners = trackListeners()
        val flow = commands.observeShardChannels()
        assertTrue(listeners.isEmpty())
        val first = async(start = CoroutineStart.UNDISPATCHED) { flow.first() }
        val second = async(start = CoroutineStart.UNDISPATCHED) { flow.first() }
        assertEquals(2, listeners.size)

        first.cancelAndJoin()
        assertEquals(1, listeners.size)
        listeners.single().smessage("channel", "value")
        val message = second.await()
        assertEquals("channel", message.channel)
        assertEquals("value", message.message)
        assertTrue(listeners.isEmpty())
        verifyNoInteractions(reactive)
    }

    @Test
    fun shardOverflowErrorsRemoveTheListener() = runBlocking {
        withTimeout(5000) {
            val listeners = trackListeners()
            val started = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val result = async(start = CoroutineStart.UNDISPATCHED) {
                runCatching {
                    commands.observeShardChannels(FluxSink.OverflowStrategy.ERROR).buffer(0).collect {
                        started.complete(Unit)
                        release.await()
                    }
                }.exceptionOrNull()
            }
            listeners.single().smessage("channel", "first")
            started.await()
            repeat(256) {
                listeners.toList().forEach { listener -> listener.smessage("channel", "overflow") }
            }
            release.complete(Unit)
            val error = result.await()
            assertTrue(error != null && Exceptions.isOverflow(error))
            assertTrue(listeners.isEmpty())
        }
    }

    @Test
    fun failingShardCollectorRemovesItsListener() = runBlocking {
        val listeners = trackListeners()
        val error = IllegalStateException("collector failed")
        val received = async(start = CoroutineStart.UNDISPATCHED) {
            assertSame(error, assertFailsWith<IllegalStateException> {
                commands.observeShardChannels().first { throw error }
            })
        }
        listeners.single().smessage("channel", "value")
        received.await()
        assertTrue(listeners.isEmpty())
        verifyNoInteractions(reactive)
    }

    private fun trackListeners(): MutableList<RedisPubSubListener<String, String>> {
        val listeners = CopyOnWriteArrayList<RedisPubSubListener<String, String>>()
        doAnswer {
            listeners.add(it.getArgument(0))
            null
        }.`when`(connection).addListener(any<RedisPubSubListener<String, String>>())
        doAnswer {
            listeners.remove(it.getArgument<RedisPubSubListener<String, String>>(0))
            null
        }.`when`(connection).removeListener(any<RedisPubSubListener<String, String>>())
        return listeners
    }
}

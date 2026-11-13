/*
 * Copyright (c) 2026-Present, Redis Ltd. All rights reserved.
 * SPDX-License-Identifier: MIT
 */
package io.lettuce.core.pubsub.api.coroutines

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.coroutines.RedisCoroutinesCommandsImpl
import io.lettuce.core.pubsub.RedisPubSubAdapter
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.pubsub.api.reactive.ChannelMessage
import io.lettuce.core.pubsub.api.reactive.PatternMessage
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink

@ExperimentalLettuceCoroutinesApi
internal class RedisPubSubCoroutinesCommandsImpl<K : Any, V : Any>(
    private val connection: StatefulRedisPubSubConnection<K, V>,
    private val pubSubOps: RedisPubSubReactiveCommands<K, V>
) : RedisCoroutinesCommandsImpl<K, V>(pubSubOps), RedisPubSubCoroutinesCommands<K, V> {

    override suspend fun subscribe(vararg channels: K) {
        pubSubOps.subscribe(*channels).awaitFirstOrNull()
    }

    override suspend fun unsubscribe(vararg channels: K) {
        pubSubOps.unsubscribe(*channels).awaitFirstOrNull()
    }

    override suspend fun psubscribe(vararg patterns: K) {
        pubSubOps.psubscribe(*patterns).awaitFirstOrNull()
    }

    override suspend fun punsubscribe(vararg patterns: K) {
        pubSubOps.punsubscribe(*patterns).awaitFirstOrNull()
    }

    override suspend fun ssubscribe(vararg shardChannels: K) {
        pubSubOps.ssubscribe(*shardChannels).awaitFirstOrNull()
    }

    override suspend fun sunsubscribe(vararg shardChannels: K) {
        pubSubOps.sunsubscribe(*shardChannels).awaitFirstOrNull()
    }

    override fun observeChannels(overflowStrategy: FluxSink.OverflowStrategy): Flow<ChannelMessage<K, V>> =
        pubSubOps.observeChannels(overflowStrategy).asFlow()

    override fun observePatterns(overflowStrategy: FluxSink.OverflowStrategy): Flow<PatternMessage<K, V>> =
        pubSubOps.observePatterns(overflowStrategy).asFlow()

    override fun observeShardChannels(overflowStrategy: FluxSink.OverflowStrategy): Flow<ChannelMessage<K, V>> =
        Flux.create<ChannelMessage<K, V>>({ sink ->
            val listener = object : RedisPubSubAdapter<K, V>() {
                override fun smessage(channel: K, message: V) {
                    sink.next(ChannelMessage(channel, message))
                }
            }
            connection.addListener(listener)
            sink.onDispose { connection.removeListener(listener) }
        }, overflowStrategy).asFlow()
}

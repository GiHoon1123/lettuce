/*
 * Copyright (c) 2026-Present, Redis Ltd. All rights reserved.
 * SPDX-License-Identifier: MIT
 */
package io.lettuce.core.pubsub.api.coroutines

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.lettuce.core.pubsub.api.reactive.ChannelMessage
import io.lettuce.core.pubsub.api.reactive.PatternMessage
import kotlinx.coroutines.flow.Flow
import reactor.core.publisher.FluxSink

/**
 * Coroutine commands and message flows for a Redis Pub/Sub connection.
 *
 * Register subscriptions before publishing messages. Collect message flows before issuing subscriptions to avoid
 * missing messages. Cancelling a collector removes its listener without unsubscribing or closing the connection.
 * By default, message flows buffer without a limit. Use ERROR, DROP or LATEST to avoid unbounded buffering.
 *
 * @param K key type
 * @param V value type
 * @since 8.0
 */
@ExperimentalLettuceCoroutinesApi
interface RedisPubSubCoroutinesCommands<K : Any, V : Any> : RedisCoroutinesCommands<K, V> {

    /**
     * Listen for messages published to the given channels. Returns when the subscriptions are registered.
     *
     * @param channels the channels, must not be empty
     * @since 8.0
     */
    suspend fun subscribe(vararg channels: K)

    /**
     * Stop listening to the given channels. Returns when the subscriptions are unregistered.
     *
     * @param channels the channels, or none to unsubscribe from all channels
     * @since 8.0
     */
    suspend fun unsubscribe(vararg channels: K)

    /**
     * Listen for messages published to channels matching the given patterns. Returns when the subscriptions are registered.
     *
     * @param patterns the patterns, must not be empty
     * @since 8.0
     */
    suspend fun psubscribe(vararg patterns: K)

    /**
     * Stop listening to channels matching the given patterns. Returns when the subscriptions are unregistered.
     *
     * @param patterns the patterns, or none to unsubscribe from all patterns
     * @since 8.0
     */
    suspend fun punsubscribe(vararg patterns: K)

    /**
     * Listen for messages published to the given shard channels. Requires Redis 7.0 or newer.
     * Returns when the subscriptions are registered.
     *
     * @param shardChannels the shard channels, must not be empty and must share a slot on Redis Cluster
     * @since 8.0
     */
    suspend fun ssubscribe(vararg shardChannels: K)

    /**
     * Stop listening to the given shard channels. Requires Redis 7.0 or newer.
     * Returns when the subscriptions are unregistered.
     *
     * @param shardChannels the shard channels, must not be empty and must share a slot on Redis Cluster
     * @since 8.0
     */
    suspend fun sunsubscribe(vararg shardChannels: K)

    /**
     * Observe messages received through channel subscriptions. Does not register server subscriptions.
     *
     * @param overflowStrategy how to handle messages when the collector cannot keep up
     * @return a flow of channel messages; each collector has its own listener
     * @since 8.0
     */
    fun observeChannels(overflowStrategy: FluxSink.OverflowStrategy = FluxSink.OverflowStrategy.BUFFER): Flow<ChannelMessage<K, V>>

    /**
     * Observe messages received through pattern subscriptions. Does not register server subscriptions.
     *
     * @param overflowStrategy how to handle messages when the collector cannot keep up
     * @return a flow of messages including the matching pattern; each collector has its own listener
     * @since 8.0
     */
    fun observePatterns(overflowStrategy: FluxSink.OverflowStrategy = FluxSink.OverflowStrategy.BUFFER): Flow<PatternMessage<K, V>>

    /**
     * Observe messages received through shard channel subscriptions. Does not register server subscriptions.
     *
     * @param overflowStrategy how to handle messages when the collector cannot keep up
     * @return a flow of shard channel messages; each collector has its own listener
     * @since 8.0
     */
    fun observeShardChannels(overflowStrategy: FluxSink.OverflowStrategy = FluxSink.OverflowStrategy.BUFFER): Flow<ChannelMessage<K, V>>
}

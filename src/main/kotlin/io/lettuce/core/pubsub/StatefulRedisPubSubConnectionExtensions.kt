/*
 * Copyright (c) 2026-Present, Redis Ltd. All rights reserved.
 * SPDX-License-Identifier: MIT
 */
package io.lettuce.core.pubsub

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection
import io.lettuce.core.cluster.pubsub.api.reactive.RedisClusterPubSubReactiveCommands
import io.lettuce.core.pubsub.api.coroutines.RedisPubSubCoroutinesCommands
import io.lettuce.core.pubsub.api.coroutines.RedisPubSubCoroutinesCommandsImpl
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands

/**
 * Create a coroutine API for this Pub/Sub connection. Does not create or close connections or register subscriptions.
 *
 * @param K key type
 * @param V value type
 * @return coroutine commands and message flows bound to this connection
 * @since 8.0
 */
@ExperimentalLettuceCoroutinesApi
fun <K : Any, V : Any> StatefulRedisPubSubConnection<K, V>.coroutines(): RedisPubSubCoroutinesCommands<K, V> {
    val reactive = if (this is StatefulRedisClusterPubSubConnection<K, V>) {
        commands(RedisClusterPubSubReactiveCommands.factory<K, V>())
    } else {
        commands(RedisPubSubReactiveCommands.factory<K, V>())
    }
    return RedisPubSubCoroutinesCommandsImpl(this, reactive)
}

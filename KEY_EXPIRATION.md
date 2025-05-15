# Redis Key Expiration Behavior

This document explains how Redis handles key expiration, how to configure it, and best practices for working with expired keys.

## How Redis Expires Keys

Redis handles key expiration through two separate mechanisms:

### 1. Passive Expiration
When a client attempts to access a key (via GET, SET, etc.), Redis checks if the key has expired. If it has, Redis removes the key before processing the command, effectively returning a "key not found" result.

### 2. Active Expiration
Redis runs a background task that periodically samples a small set of keys with expiration times to determine if they should be deleted. This process:
- Samples 20 keys from the set of keys with expiration times
- Deletes all sampled keys that have expired
- If more than 25% of the sampled keys were expired, repeats the process immediately
- Otherwise, waits until the next cycle

## Key Expiration Notification Timing

Important considerations regarding when expiration events are triggered:

- Notifications are sent **after** Redis actually removes the expired key, not exactly when the TTL reaches zero
- There can be a delay between when a key's TTL expires and when the notification is triggered
- Under high load, there might be significant delays in receiving expiration events
- The sampling nature of the active expiration means not all keys are checked continuously

## Configuring Key Expiration Behavior

While you cannot configure the exact moment when Redis starts the key sampling process (as it's an ongoing background task), you can adjust these parameters to control its behavior:

### 1. `hz` Configuration
Controls how frequently Redis runs background tasks, including expired key sampling:

```
hz 10  # Default: runs background tasks ~10 times per second
```

- Range: 1-500
- Higher values make Redis check expired keys more frequently but increase CPU usage
- Configuration options:
  - In redis.conf: `hz 50`
  - At runtime: `CONFIG SET hz 50`

### 2. `active-expire-effort` Configuration (Redis 6.0+)
Controls how aggressively Redis samples expired keys:

```
active-expire-effort 1  # Default: minimal effort (1-10 scale)
```

- Range: 1-10
- Higher values allocate more CPU resources to expired key cleanup
- Configuration options:
  - In redis.conf: `active-expire-effort 5`
  - At runtime: `CONFIG SET active-expire-effort 5`

### 3. Memory Policy Impact
When using memory policies, expired key behavior can be affected:

```
maxmemory-policy volatile-ttl
```

- `volatile-ttl`: Redis will prioritize evicting keys with shorter TTLs first
- This impacts keys that haven't yet expired but are approaching expiration

## Keyspace Notifications for Expired Keys

To receive notifications when keys expire:

1. Enable keyspace notifications:
```
notify-keyspace-events Ex
```
- `E`: Enable keyspace events
- `x`: Notify on expired events

2. Subscribe to the expiration channel:
```
PSUBSCRIBE __keyevent@*__:expired
```

Example in Java:
```java
jedis.psubscribe(new JedisPubSub() {
    @Override
    public void onPMessage(String pattern, String channel, String message) {
        System.out.println("Key expired: " + message);
        // Handle expiration
    }
}, "__keyevent@*__:expired");
```

## Best Practices for Key Expiration

1. **Don't rely on exact expiration timing**: Design your system to handle delayed or missed expirations
2. **Use appropriate TTL values**: Set TTLs significantly higher than your processing window to account for delays
3. **Consider dual tracking**: For critical expirations, maintain a separate sorted set with scores as expiration times
4. **Monitor Redis metrics**: Watch `expired_keys` and `evicted_keys` metrics to understand expiration patterns
5. **Tune `hz` parameter**: Increase for more responsive expirations, decrease to reduce CPU load
6. **Plan for missed notifications**: Have a recovery mechanism for when notifications aren't received

## Limitations and Considerations

- **No guarantee of immediate expiration**: Keys might persist briefly past their expiration time
- **Network issues**: Keyspace notifications might be missed if connections drop
- **Cluster considerations**: In clustered environments, subscribe to notifications on all nodes
- **Performance impact**: High expiration rates can impact Redis performance
- **Storage overhead**: Each key with an expiration time requires additional memory

## Expiration Events in Distributed Environments

When running multiple instances of an application that listen for Redis expiration events, special considerations are needed:

### Broadcasting Behavior

- Redis keyspace notifications use a pub/sub model
- **All subscribed instances** receive the same expiration event
- There is no built-in mechanism to route an event to just one consumer
- Each application instance listening to `__keyevent@*__:expired` will receive notification for every expired key

### Handling Duplicate Processing

To ensure an expiration event is processed only once in a multi-instance environment:

1. **Distributed Lock Approach**
   ```java
   // When expiration event is received
   String lockKey = "processing:" + expiredKey;
   try {
       // Try to acquire lock with short TTL
       boolean acquired = jedis.set(lockKey, instanceId, SetParams.setParams().nx().ex(5));
       if (acquired) {
           // This instance won the lock, process the event
           processExpiredKey(expiredKey);
       }
   } finally {
       // Only delete the lock if this instance created it
       jedis.eval(
           "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
           1, lockKey, instanceId);
   }
   ```

2. **Leader Election**
   - Designate only one instance to listen for expiration events
   - Use Redis Sentinel, ZooKeeper, or etcd for leader election
   - If the leader fails, another instance takes over as the listener

3. **Idempotent Processing**
   - Design handlers to be idempotent (multiple executions produce same result)
   - Use transaction IDs or record processed events to avoid duplication

4. **Work Queue Pattern**
   ```java
   // Instead of processing directly, add to a reliable queue
   jedis.lpush("expired_keys_queue", expiredKey);
   
   // In a separate consumer process (using reliable queue pattern)
   while (true) {
       // Atomically move item from source to processing list
       String key = jedis.brpoplpush("expired_keys_queue", "expired_keys_processing", 0);
       try {
           processExpiredKey(key);
           // Remove from processing list after successful processing
           jedis.lrem("expired_keys_processing", 1, key);
       } catch (Exception e) {
           // Error handling - item remains in processing list for recovery
       }
   }
   ```

### Considerations for Distributed Processing

- **Exactly-once processing** is challenging in distributed systems
- **Network partition** can lead to multiple instances believing they are the leader
- **Recovery mechanisms** are needed for failed processing attempts
- **Processing latency** increases when using distributed locks or queues
- In **Redis Cluster**, you must subscribe to events on all nodes where keys might expire

## Further Resources

- [Redis Documentation on Expiration](https://redis.io/commands/expire)
- [Redis Keyspace Notifications](https://redis.io/topics/notifications)
- [Redis Configuration Reference](https://redis.io/topics/config) 
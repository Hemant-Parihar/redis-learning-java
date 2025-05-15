package com.example.redis.config;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Handles Redis key expiration events using keyspace notifications.
 * This class provides a way to execute callbacks when keys expire in Redis.
 */
public class RedisExpirationHandler {
    private static final Logger logger = LoggerFactory.getLogger(RedisExpirationHandler.class);
    private final JedisPool jedisPool;
    private final ExecutorService executorService;
    private Consumer<String> expirationCallback;
    private volatile boolean isRunning = false;

    /**
     * Creates a new Redis expiration handler.
     */
    public RedisExpirationHandler() {
        this.jedisPool = DatabaseConfig.getJedisPool();
        this.executorService = Executors.newSingleThreadExecutor();
    }

    /**
     * Sets the callback to execute when a key expires.
     * 
     * @param callback A consumer that processes the expired key name
     * @return This handler instance for method chaining
     */
    public RedisExpirationHandler onKeyExpire(Consumer<String> callback) {
        this.expirationCallback = callback;
        return this;
    }

    /**
     * Enables keyspace notifications for key expiration events in Redis.
     */
    private void enableKeyspaceNotifications() {
        try (Jedis jedis = jedisPool.getResource()) {
            // Configure Redis to send notifications for expired events
            // Ex = Keyspace events for expired keys
            String notifyOption = jedis.configGet("notify-keyspace-events").get(1);
            
            // If keyspace notifications aren't properly configured, update the configuration
            if (!notifyOption.contains("E") || !notifyOption.contains("x")) {
                if (notifyOption.equals("")) {
                    notifyOption = "Ex";
                } else {
                    notifyOption = notifyOption + "Ex";
                }
                jedis.configSet("notify-keyspace-events", notifyOption);
                logger.info("Redis keyspace notifications enabled: {}", notifyOption);
            }
        } catch (Exception e) {
            logger.error("Failed to enable Redis keyspace notifications", e);
            throw new RuntimeException("Could not configure Redis for expiration events", e);
        }
    }

    /**
     * Starts listening for key expiration events.
     */
    public void start() {
        if (isRunning) {
            logger.warn("Expiration handler is already running");
            return;
        }

        if (expirationCallback == null) {
            throw new IllegalStateException("No expiration callback configured. Call onKeyExpire() first.");
        }

        enableKeyspaceNotifications();
        isRunning = true;

        // Start listening for expiration events in a separate thread
        executorService.submit(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                logger.info("Starting to listen for Redis key expiration events");
                
                // Subscribe to the keyspace notification channel for expired events
                // The channel pattern is: __keyevent@<db>__:expired
                jedis.psubscribe(new JedisPubSub() {
                    @Override
                    public void onPMessage(String pattern, String channel, String message) {
                        logger.debug("Received expiration for key: {}", message);
                        try {
                            // Call the user-provided callback with the expired key
                            expirationCallback.accept(message);
                        } catch (Exception e) {
                            logger.error("Error in expiration callback for key: {}", message, e);
                        }
                    }
                }, "__keyevent@*__:expired");
            } catch (Exception e) {
                if (isRunning) {
                    logger.error("Error in Redis expiration listener", e);
                } else {
                    logger.info("Redis expiration listener stopped");
                }
            }
        });
    }

    /**
     * Stops listening for key expiration events.
     */
    public void stop() {
        isRunning = false;
        executorService.shutdownNow();
        logger.info("Redis expiration handler stopped");
    }

    /**
     * Sets a key with a specified expiration time (TTL).
     * 
     * @param key The key to set
     * @param value The value to set
     * @param ttlSeconds The time-to-live in seconds
     */
    public void setWithExpiration(String key, String value, int ttlSeconds) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(key, ttlSeconds, value);
            logger.debug("Set key {} with expiration of {} seconds", key, ttlSeconds);
        } catch (Exception e) {
            logger.error("Failed to set key with expiration", e);
            throw new RuntimeException("Could not set key with expiration", e);
        }
    }
} 
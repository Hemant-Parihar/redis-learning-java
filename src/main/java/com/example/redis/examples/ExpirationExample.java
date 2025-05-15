package com.example.redis.examples;

import com.example.redis.config.RedisExpirationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating how to handle Redis key expirations using keyspace notifications.
 */
public class ExpirationExample {
    private static final Logger logger = LoggerFactory.getLogger(ExpirationExample.class);

    public static void main(String[] args) throws InterruptedException {
        logger.info("Starting Redis expiration handling example");

        // Create and configure the expiration handler
        RedisExpirationHandler expirationHandler = new RedisExpirationHandler();
        
        // Define what happens when a key expires
        expirationHandler.onKeyExpire(expiredKey -> {
            logger.info("Key expired: {}", expiredKey);
            
            // Here you can add your business logic to handle expired keys
            if (expiredKey.startsWith("session:")) {
                logger.info("User session expired, cleaning up resources for {}", expiredKey);
                // Perform session cleanup logic
            } else if (expiredKey.startsWith("cache:")) {
                logger.info("Cache entry expired: {}", expiredKey);
                // Handle cache expiration
            } else if (expiredKey.startsWith("lock:")) {
                logger.info("Lock released: {}", expiredKey);
                // Handle lock expiration
            }
        });
        
        // Start listening for expiration events
        expirationHandler.start();
        
        // Set some keys with different TTLs to demonstrate expiration
        expirationHandler.setWithExpiration("session:user123", "sessiondata", 5);
        expirationHandler.setWithExpiration("cache:product456", "cachedata", 7);
        expirationHandler.setWithExpiration("lock:resource789", "lockdata", 10);
        
        logger.info("Keys set with TTL. Waiting for expirations...");
        
        // Wait long enough for all keys to expire
        Thread.sleep(12000);
        
        // Clean up resources
        expirationHandler.stop();
        
        logger.info("Example completed");
    }
} 
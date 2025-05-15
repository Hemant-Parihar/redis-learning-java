package com.example.redis.dao;

import com.example.redis.config.DatabaseConfig;
import com.example.redis.model.Product;
import com.example.redis.model.User;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Data Access Object for Redis operations.
 * This class provides methods for various Redis data structure operations
 * and demonstrates common Redis use cases.
 */
public class RedisDao {
    
    // Redis key prefixes for different data types
    private static final String USER_KEY_PREFIX = "user:";
    private static final String PRODUCT_KEY_PREFIX = "product:";
    private static final String USER_IDX_PREFIX = "idx:user:";
    private static final String PAGE_VISIT_KEY = "page_visits";
    private static final String STOCK_KEY_PREFIX = "stock:";
    
    // JedisPool instance for connection pooling
    private final JedisPool jedisPool;
    
    /**
     * Constructor that initializes the Redis connection pool
     */
    public RedisDao() {
        this.jedisPool = DatabaseConfig.getJedisPool();
    }
    
    /**
     * Save a user to Redis using String data type.
     * This demonstrates basic Redis String operations.
     * 
     * @param user The user to save
     * @return The ID of the saved user
     */
    public Long saveUser(User user) {
        try (Jedis jedis = jedisPool.getResource()) {
            // If the user doesn't have an ID, generate one
            if (user.getId() == null) {
                // Check if the user already exists by username to avoid duplicates
                String existingUserId = jedis.get(USER_IDX_PREFIX + user.getUsername());
                if (existingUserId != null) {
                    throw new IllegalArgumentException("User with username " + user.getUsername() + " already exists");
                }
                // Use Redis to generate a unique ID (INCR command)
                Long id = jedis.incr("next_user_id");
                user.setId(id);
            }
            
            // Construct the Redis key for this user
            String userKey = USER_KEY_PREFIX + user.getId();
            
            // Store the user as a string value
            jedis.set(userKey, user.toRedisString());
            
            // Create an index by username for fast lookups
            jedis.set(USER_IDX_PREFIX + user.getUsername(), user.getId().toString());
            
            return user.getId();
        }
    }
    
    /**
     * Get a user from Redis by ID.
     * This demonstrates retrieving a Redis String.
     * 
     * @param id The user ID
     * @return The User object or null if not found
     */
    public User getUserById(Long id) {
        try (Jedis jedis = jedisPool.getResource()) {
            String userKey = USER_KEY_PREFIX + id;
            String userData = jedis.get(userKey);
            
            if (userData == null) {
                return null;
            }
            
            return User.fromRedisString(userData);
        }
    }
    
    /**
     * Get a user from Redis by username.
     * This demonstrates using a secondary index in Redis.
     * 
     * @param username The username to look up
     * @return The User object or null if not found
     */
    public User getUserByUsername(String username) {
        try (Jedis jedis = jedisPool.getResource()) {
            // First, get the ID using the username index
            String userId = jedis.get(USER_IDX_PREFIX + username);
            
            if (userId == null) {
                return null;
            }
            
            // Then, get the user using the ID
            return getUserById(Long.parseLong(userId));
        }
    }
    
    /**
     * Save a product to Redis using Hash data type.
     * This demonstrates Redis Hash operations which are good for objects with fields.
     * 
     * @param product The product to save
     * @return The ID of the saved product
     */
    public Long saveProduct(Product product) {
        try (Jedis jedis = jedisPool.getResource()) {
            // If the product doesn't have an ID, generate one
            if (product.getId() == null) {
                // Use Redis to generate a unique ID
                Long id = jedis.incr("next_product_id");
                product.setId(id);
            }
            
            String productKey = PRODUCT_KEY_PREFIX + product.getId();
            
            // Create a hash map of field-value pairs
            Map<String, String> productHash = new HashMap<>();
            productHash.put("id", product.getId().toString());
            productHash.put("name", product.getName());
            productHash.put("price", product.getPrice().toString());
            productHash.put("stock", product.getStock().toString());
            productHash.put("createdAt", product.getCreatedAt().toString());
            
            // Save as a Redis hash
            jedis.hset(productKey, productHash);
            
            // Also update the stock in a separate key for stock management
            jedis.set(STOCK_KEY_PREFIX + product.getId(), product.getStock().toString());
            
            return product.getId();
        }
    }
    
    /**
     * Get a product from Redis by ID.
     * This demonstrates retrieving a Redis Hash.
     * 
     * @param id The product ID
     * @return The Product object or null if not found
     */
    public Product getProductById(Long id) {
        try (Jedis jedis = jedisPool.getResource()) {
            String productKey = PRODUCT_KEY_PREFIX + id;
            Map<String, String> productHash = jedis.hgetAll(productKey);
            
            if (productHash.isEmpty()) {
                return null;
            }
            
            // Create a Product object from the hash
            Product product = new Product();
            product.setId(Long.parseLong(productHash.get("id")));
            product.setName(productHash.get("name"));
            product.setPrice(java.math.BigDecimal.valueOf(Double.parseDouble(productHash.get("price"))));
            product.setStock(Integer.parseInt(productHash.get("stock")));
            product.setCreatedAt(java.time.LocalDateTime.parse(productHash.get("createdAt")));
            
            return product;
        }
    }
    
    /**
     * Update product stock using Redis atomic operations.
     * This demonstrates Redis's atomic operations for concurrency control.
     * 
     * @param productId The product ID
     * @param newStockValue The new stock value
     * @return true if update was successful, false if product not found
     */
    public boolean updateProductStock(Long productId, int newStockValue) {
        try (Jedis jedis = jedisPool.getResource()) {
            String productKey = PRODUCT_KEY_PREFIX + productId;
            
            // Check if product exists
            if (!jedis.exists(productKey)) {
                return false;
            }
            
            // Update the stock field in the product hash
            jedis.hset(productKey, "stock", String.valueOf(newStockValue));
            
            // Also update the separate stock key
            jedis.set(STOCK_KEY_PREFIX + productId, String.valueOf(newStockValue));
            
            return true;
        }
    }
    
    /**
     * Attempt to decrement product stock atomically with optimistic locking.
     * This demonstrates Redis transactions for atomic operations.
     * 
     * @param productId The product ID
     * @param amount The amount to decrement
     * @return true if decrement was successful, false if insufficient stock or product not found
     */
    public boolean decrementProductStock(Long productId, int amount) {
        try (Jedis jedis = jedisPool.getResource()) {
            String stockKey = STOCK_KEY_PREFIX + productId;
            String productKey = PRODUCT_KEY_PREFIX + productId;
            
            // Start watching the stock key for changes
            jedis.watch(stockKey);
            
            // Get current stock
            String currentStockStr = jedis.get(stockKey);
            if (currentStockStr == null) {
                return false;  // Product not found
            }
            
            int currentStock = Integer.parseInt(currentStockStr);
            if (currentStock < amount) {
                jedis.unwatch();  // Release the watch
                return false;  // Insufficient stock
            }
            
            // Start a transaction
            redis.clients.jedis.Transaction tx = jedis.multi();
            
            // Decrement the stock
            int newStock = currentStock - amount;
            tx.set(stockKey, String.valueOf(newStock));
            tx.hset(productKey, "stock", String.valueOf(newStock));
            
            // Execute the transaction
            tx.exec();
            
            return true;
        } catch (Exception e) {
            // Transaction failed (someone else modified the stock simultaneously)
            return false;
        }
    }
    
    /**
     * Increment a page visit counter.
     * This demonstrates using Redis for real-time analytics.
     * 
     * @param url The page URL
     * @return The new visit count
     */
    public Long incrementPageVisit(String url) {
        try (Jedis jedis = jedisPool.getResource()) {
            // Increment the counter for this URL in the hash
            Long count = jedis.hincrBy(PAGE_VISIT_KEY, url, 1);
            
            // Update the last visit time
            jedis.hset(PAGE_VISIT_KEY + ":lastvisit", url, java.time.LocalDateTime.now().toString());
            
            return count;
        }
    }
    
    /**
     * Get all page visit counts.
     * This demonstrates retrieving all fields from a Redis hash.
     * 
     * @return Map of URL to visit count
     */
    public Map<String, Long> getAllPageVisits() {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> rawVisits = jedis.hgetAll(PAGE_VISIT_KEY);
            Map<String, Long> visits = new HashMap<>();
            
            for (Map.Entry<String, String> entry : rawVisits.entrySet()) {
                visits.put(entry.getKey(), Long.parseLong(entry.getValue()));
            }
            
            return visits;
        }
    }
    
    /**
     * Add a product to a user's wishlist using Redis Sets.
     * This demonstrates Redis Set operations.
     * 
     * @param userId The user ID
     * @param productId The product ID to add to wishlist
     */
    public void addToWishlist(Long userId, Long productId) {
        try (Jedis jedis = jedisPool.getResource()) {
            String wishlistKey = "wishlist:" + userId;
            jedis.sadd(wishlistKey, productId.toString());
        }
    }
    
    /**
     * Remove a product from a user's wishlist.
     * 
     * @param userId The user ID
     * @param productId The product ID to remove from wishlist
     */
    public void removeFromWishlist(Long userId, Long productId) {
        try (Jedis jedis = jedisPool.getResource()) {
            String wishlistKey = "wishlist:" + userId;
            jedis.srem(wishlistKey, productId.toString());
        }
    }
    
    /**
     * Get a user's wishlist.
     * 
     * @param userId The user ID
     * @return List of product IDs in the wishlist
     */
    public List<String> getWishlist(Long userId) {
        try (Jedis jedis = jedisPool.getResource()) {
            String wishlistKey = "wishlist:" + userId;
            return new ArrayList<>(jedis.smembers(wishlistKey));
        }
    }
    
    /**
     * Add a product to a sorted ranking by sales.
     * This demonstrates Redis Sorted Set operations.
     * 
     * @param productId The product ID
     * @param salesCount The number of sales (score)
     */
    public void addToSalesRanking(Long productId, long salesCount) {
        try (Jedis jedis = jedisPool.getResource()) {
            String rankingKey = "ranking:sales";
            jedis.zadd(rankingKey, salesCount, productId.toString());
        }
    }
    
    /**
     * Increment a product's sales count in the ranking.
     * 
     * @param productId The product ID
     * @param incrementBy The amount to increment by
     * @return The new sales count
     */
    public Double incrementSalesCount(Long productId, long incrementBy) {
        try (Jedis jedis = jedisPool.getResource()) {
            String rankingKey = "ranking:sales";
            Double newScore = jedis.zincrby(rankingKey, incrementBy, productId.toString());
            return newScore;
        }
    }
    
    /**
     * Get the top N products by sales.
     * 
     * @param count The number of top products to get
     * @return List of product IDs sorted by sales count
     */
    public List<String> getTopProductsBySales(int count) {
        try (Jedis jedis = jedisPool.getResource()) {
            String rankingKey = "ranking:sales";
            return new ArrayList<>(jedis.zrevrange(rankingKey, 0, count - 1));
        }
    }
    
    /**
     * Store session data for a user.
     * This demonstrates using Redis for session storage.
     * 
     * @param sessionId The session ID
     * @param data Map of session data
     * @param expirySeconds Time to live in seconds
     */
    public void storeSessionData(String sessionId, Map<String, String> data, int expirySeconds) {
        try (Jedis jedis = jedisPool.getResource()) {
            String sessionKey = "session:" + sessionId;
            
            // Store session data as a hash
            jedis.hset(sessionKey, data);
            
            // Set expiry time
            jedis.expire(sessionKey, expirySeconds);
        }
    }
    
    /**
     * Get session data.
     * 
     * @param sessionId The session ID
     * @return Map of session data
     */
    public Map<String, String> getSessionData(String sessionId) {
        try (Jedis jedis = jedisPool.getResource()) {
            String sessionKey = "session:" + sessionId;
            return jedis.hgetAll(sessionKey);
        }
    }
    
    /**
     * Save user activity to a Redis list (used as a queue or for recent activity).
     * This demonstrates Redis List operations.
     * 
     * @param userId The user ID
     * @param activity The activity description
     * @param maxActivities Maximum number of activities to keep
     */
    public void saveUserActivity(Long userId, String activity, int maxActivities) {
        try (Jedis jedis = jedisPool.getResource()) {
            String activityKey = "activity:" + userId;
            
            // Add the activity to the front of the list with timestamp
            String activityWithTimestamp = System.currentTimeMillis() + ":" + activity;
            jedis.lpush(activityKey, activityWithTimestamp);
            
            // Trim the list to keep only the most recent activities
            jedis.ltrim(activityKey, 0, maxActivities - 1);
        }
    }
    
    /**
     * Get recent user activities.
     * 
     * @param userId The user ID
     * @param count Number of recent activities to retrieve
     * @return List of recent activities
     */
    public List<String> getUserActivities(Long userId, int count) {
        try (Jedis jedis = jedisPool.getResource()) {
            String activityKey = "activity:" + userId;
            return jedis.lrange(activityKey, 0, count - 1);
        }
    }
    
    /**
     * Add user to a leaderboard with their score.
     * This demonstrates another use of Redis Sorted Sets.
     * 
     * @param userId The user ID
     * @param score The user's score
     */
    public void addToLeaderboard(Long userId, double score) {
        try (Jedis jedis = jedisPool.getResource()) {
            String leaderboardKey = "leaderboard";
            jedis.zadd(leaderboardKey, score, userId.toString());
        }
    }
    
    /**
     * Get user rank in the leaderboard (0-based).
     * 
     * @param userId The user ID
     * @return The user's rank or null if not on leaderboard
     */
    public Long getUserRank(Long userId) {
        try (Jedis jedis = jedisPool.getResource()) {
            String leaderboardKey = "leaderboard";
            Long rank = jedis.zrevrank(leaderboardKey, userId.toString());
            return rank;
        }
    }
    
    /**
     * Demonstrate Redis pipelining for better performance when sending multiple commands.
     * This executes all commands in a single network roundtrip.
     * 
     * @param userIds List of user IDs to fetch
     * @return List of users
     */
    public List<User> getUsersByIdsPipelined(List<Long> userIds) {
        List<User> users = new ArrayList<>();
        
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline pipeline = jedis.pipelined();
            
            // Send all get commands in the pipeline
            List<Response<String>> responses = new ArrayList<>();
            for (Long id : userIds) {
                String userKey = USER_KEY_PREFIX + id;
                responses.add(pipeline.get(userKey));
            }
            
            // Execute the pipeline
            pipeline.sync();
            
            // Process the responses
            for (Response<String> response : responses) {
                String userData = response.get();
                if (userData != null) {
                    users.add(User.fromRedisString(userData));
                }
            }
        }
        
        return users;
    }
    
    /**
     * Example of using Redis Scan for safely iterating over keys.
     * This is better than KEYS for production environments.
     * 
     * @param pattern Key pattern to match
     * @return List of matching keys
     */
    public List<String> scanKeys(String pattern) {
        List<String> keys = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            ScanParams params = new ScanParams().match(pattern).count(100);
            String cursor = "0";
            
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, params);
                keys.addAll(scanResult.getResult());
                cursor = scanResult.getCursor();
            } while (!cursor.equals("0"));
        }
        
        return keys;
    }
    
    /**
     * Delete all keys matching a pattern.
     * 
     * @param pattern Key pattern to match
     * @return Number of keys deleted
     */
    public long deleteKeysByPattern(String pattern) {
        long deletedCount = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            List<String> keys = scanKeys(pattern);
            
            if (!keys.isEmpty()) {
                deletedCount = jedis.del(keys.toArray(new String[0]));
            }
        }
        
        return deletedCount;
    }
} 
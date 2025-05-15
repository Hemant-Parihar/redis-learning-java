package com.example.redis;

import com.example.redis.config.DatabaseConfig;
import com.example.redis.dao.RedisDao;
import com.example.redis.model.Product;
import com.example.redis.model.User;
import com.example.redis.performance.PerformanceComparison;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

/**
 * Main application class for Redis learning examples.
 * This class demonstrates various Redis operations and includes a performance comparison
 * with PostgreSQL.
 */
public class RedisLearningApplication {
    
    private static final Logger logger = LoggerFactory.getLogger(RedisLearningApplication.class);
    private static final Scanner scanner = new Scanner(System.in);
    private static final RedisDao redisDao = new RedisDao();
    
    /**
     * Main method to run the application.
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        try {
            // Display a welcome message and menu
            System.out.println("===== Redis Learning Application =====");
            System.out.println("This application demonstrates Redis operations and compares its performance with PostgreSQL.");
            
            boolean running = true;
            while (running) {
                displayMenu();
                int choice = getIntInput("Enter your choice: ");
                
                switch (choice) {
                    case 1:
                        basicRedisExamples();
                        break;
                    case 2:
                        dataTypesExample();
                        break;
                    case 3:
                        crudOperationsExample();
                        break;
                    case 4:
                        runPerformanceComparison();
                        break;
                    case 5:
                        redisPubSubExample();
                        break;
                    case 6:
                        cachingExample();
                        break;
                    case 0:
                        running = false;
                        System.out.println("Closing connections and exiting...");
                        DatabaseConfig.closeConnections();
                        break;
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
                
                if (running) {
                    System.out.println("\nPress Enter to continue...");
                    scanner.nextLine();
                }
            }
        } catch (Exception e) {
            logger.error("Error in application", e);
        } finally {
            scanner.close();
        }
    }
    
    /**
     * Display the application menu.
     */
    private static void displayMenu() {
        System.out.println("\n===== MENU =====");
        System.out.println("1. Basic Redis Examples");
        System.out.println("2. Redis Data Types Examples");
        System.out.println("3. CRUD Operations Example");
        System.out.println("4. Performance Comparison with PostgreSQL");
        System.out.println("5. Redis Pub/Sub Example");
        System.out.println("6. Caching Example");
        System.out.println("0. Exit");
    }
    
    /**
     * Get an integer input from the user.
     * 
     * @param prompt The prompt to display
     * @return The entered integer
     */
    private static int getIntInput(String prompt) {
        while (true) {
            try {
                System.out.print(prompt);
                String input = scanner.nextLine();
                return Integer.parseInt(input);
            } catch (NumberFormatException e) {
                System.out.println("Please enter a valid number.");
            }
        }
    }
    
    /**
     * Example 1: Basic Redis operations.
     * This demonstrates basic Redis commands like SET, GET, etc.
     */
    private static void basicRedisExamples() {
        System.out.println("\n===== Basic Redis Examples =====");
        
        try (Jedis jedis = DatabaseConfig.getJedisPool().getResource()) {
            // Basic operations
            System.out.println("\n1. String operations:");
            jedis.set("greeting", "Hello, Redis!");
            System.out.println("SET greeting 'Hello, Redis!'");
            
            String greeting = jedis.get("greeting");
            System.out.println("GET greeting: " + greeting);
            
            // Expiration
            System.out.println("\n2. Key expiration:");
            jedis.set("temp_key", "This will expire soon");
            jedis.expire("temp_key", 10);  // Expire in 10 seconds
            System.out.println("SET temp_key 'This will expire soon'");
            System.out.println("EXPIRE temp_key 10 (seconds)");
            
            Long ttl = jedis.ttl("temp_key");
            System.out.println("TTL temp_key: " + ttl + " seconds");
            
            // Incrementing
            System.out.println("\n3. Incrementing numbers:");
            jedis.set("counter", "0");
            System.out.println("SET counter 0");
            
            Long newValue = jedis.incr("counter");
            System.out.println("INCR counter: " + newValue);
            
            newValue = jedis.incrBy("counter", 5);
            System.out.println("INCRBY counter 5: " + newValue);
            
            // Checking if a key exists
            System.out.println("\n4. Key existence check:");
            boolean exists = jedis.exists("greeting");
            System.out.println("EXISTS greeting: " + exists);
            
            // Deleting a key
            System.out.println("\n5. Key deletion:");
            Long deleted = jedis.del("counter");
            System.out.println("DEL counter: " + deleted + " (number of keys deleted)");
            
            // Verify deletion
            exists = jedis.exists("counter");
            System.out.println("EXISTS counter: " + exists);
        }
    }
    
    /**
     * Example 2: Redis data types.
     * This demonstrates the different Redis data types.
     */
    private static void dataTypesExample() {
        System.out.println("\n===== Redis Data Types Examples =====");
        
        try (Jedis jedis = DatabaseConfig.getJedisPool().getResource()) {
            // Clear any existing data for this demo
            jedis.flushDB();
            
            // 1. Strings
            System.out.println("\n1. String data type:");
            jedis.set("user:name", "John Doe");
            jedis.set("user:email", "john@example.com");
            jedis.set("user:visits", "1");
            jedis.incr("user:visits");
            
            System.out.println("GET user:name: " + jedis.get("user:name"));
            System.out.println("GET user:email: " + jedis.get("user:email"));
            System.out.println("GET user:visits: " + jedis.get("user:visits"));
            
            // 2. Lists
            System.out.println("\n2. List data type:");
            jedis.lpush("recent_users", "user:1001", "user:1002", "user:1003");
            jedis.rpush("recent_users", "user:1004");  // Add to the right side
            
            List<String> recentUsers = jedis.lrange("recent_users", 0, -1);
            System.out.println("LRANGE recent_users 0 -1: " + recentUsers);
            
            String poppedUser = jedis.lpop("recent_users");  // Pop from the left side
            System.out.println("LPOP recent_users: " + poppedUser);
            
            Long listSize = jedis.llen("recent_users");
            System.out.println("LLEN recent_users: " + listSize);
            
            // 3. Sets
            System.out.println("\n3. Set data type:");
            jedis.sadd("tags", "redis", "database", "nosql", "key-value");
            jedis.sadd("user:1001:tags", "redis", "java", "spring");
            
            Set<String> tags = jedis.smembers("tags");
            System.out.println("SMEMBERS tags: " + tags);
            
            boolean isMember = jedis.sismember("tags", "redis");
            System.out.println("SISMEMBER tags redis: " + isMember);
            
            // Intersection between two sets
            Set<String> commonTags = jedis.sinter("tags", "user:1001:tags");
            System.out.println("SINTER tags user:1001:tags: " + commonTags);
            
            // 4. Hashes
            System.out.println("\n4. Hash data type:");
            jedis.hset("user:1001", "name", "John Doe");
            jedis.hset("user:1001", "email", "john@example.com");
            jedis.hset("user:1001", "age", "30");
            
            String userName = jedis.hget("user:1001", "name");
            System.out.println("HGET user:1001 name: " + userName);
            
            Map<String, String> userInfo = jedis.hgetAll("user:1001");
            System.out.println("HGETALL user:1001: " + userInfo);
            
            // 5. Sorted Sets
            System.out.println("\n5. Sorted Set data type:");
            jedis.zadd("leaderboard", 100, "player:1");
            jedis.zadd("leaderboard", 75, "player:2");
            jedis.zadd("leaderboard", 150, "player:3");
            jedis.zadd("leaderboard", 50, "player:4");
            
            // Get top 3 players
            List<String> topPlayers = new ArrayList<>(jedis.zrevrange("leaderboard", 0, 2));
            System.out.println("ZREVRANGE leaderboard 0 2 (top 3 players): " + topPlayers);
            
            // Get player's rank (0-based)
            Long rank = jedis.zrevrank("leaderboard", "player:2");
            System.out.println("ZREVRANK leaderboard player:2: " + rank);
            
            // Increment a player's score
            Double newScore = jedis.zincrby("leaderboard", 25, "player:2");
            System.out.println("ZINCRBY leaderboard 25 player:2: " + newScore);
            
            // 6. Bit operations
            System.out.println("\n6. Bitmaps (special case of string):");
            // Track user logins for a week
            jedis.setbit("user:1001:logins", 0, true);  // Sunday
            jedis.setbit("user:1001:logins", 2, true);  // Tuesday
            jedis.setbit("user:1001:logins", 4, true);  // Thursday
            
            boolean loggedInTuesday = jedis.getbit("user:1001:logins", 2);
            System.out.println("GETBIT user:1001:logins 2 (Tuesday): " + loggedInTuesday);
            
            Long loginCount = jedis.bitcount("user:1001:logins");
            System.out.println("BITCOUNT user:1001:logins (login days): " + loginCount);
            
            // 7. HyperLogLog
            System.out.println("\n7. HyperLogLog (for counting unique elements):");
            jedis.pfadd("daily_visitors", "user:1", "user:2", "user:3");
            jedis.pfadd("daily_visitors", "user:2", "user:4");
            
            Long uniqueVisitors = jedis.pfcount("daily_visitors");
            System.out.println("PFCOUNT daily_visitors (unique visitors): " + uniqueVisitors);
        }
    }
    
    /**
     * Example 3: CRUD operations with Redis.
     * This demonstrates Create, Read, Update, Delete operations using our DAO.
     */
    private static void crudOperationsExample() {
        System.out.println("\n===== CRUD Operations Example =====");
        
        // 1. Create users
        System.out.println("\n1. Creating users:");
        User user1 = new User("johndoe", "john@example.com");
        User user2 = new User("janedoe", "jane@example.com");
        
        Long user1Id = redisDao.saveUser(user1);
        Long user2Id = redisDao.saveUser(user2);
        
        System.out.println("Created user: " + user1.getUsername() + " with ID: " + user1Id);
        System.out.println("Created user: " + user2.getUsername() + " with ID: " + user2Id);
        
        // 2. Read users
        System.out.println("\n2. Reading users:");
        User retrievedUser1 = redisDao.getUserById(user1Id);
        User retrievedUserByUsername = redisDao.getUserByUsername("janedoe");
        
        System.out.println("Retrieved by ID: " + retrievedUser1);
        System.out.println("Retrieved by username: " + retrievedUserByUsername);
        
        // 3. Create products
        System.out.println("\n3. Creating products:");
        Product product1 = new Product("Laptop", new BigDecimal("999.99"), 10);
        Product product2 = new Product("Smartphone", new BigDecimal("499.99"), 20);
        
        Long product1Id = redisDao.saveProduct(product1);
        Long product2Id = redisDao.saveProduct(product2);
        
        System.out.println("Created product: " + product1.getName() + " with ID: " + product1Id);
        System.out.println("Created product: " + product2.getName() + " with ID: " + product2Id);
        
        // 4. Read products
        System.out.println("\n4. Reading products:");
        Product retrievedProduct1 = redisDao.getProductById(product1Id);
        System.out.println("Retrieved product: " + retrievedProduct1);
        
        // 5. Update product stock
        System.out.println("\n5. Updating product stock:");
        boolean updated = redisDao.updateProductStock(product1Id, 5);
        System.out.println("Updated stock: " + updated);
        
        Product updatedProduct = redisDao.getProductById(product1Id);
        System.out.println("Retrieved updated product: " + updatedProduct);
        
        // 6. Wishlist operations
        System.out.println("\n6. Wishlist operations:");
        redisDao.addToWishlist(user1Id, product1Id);
        redisDao.addToWishlist(user1Id, product2Id);
        
        List<String> wishlist = redisDao.getWishlist(user1Id);
        System.out.println("User " + user1Id + " wishlist: " + wishlist);
        
        redisDao.removeFromWishlist(user1Id, product2Id);
        wishlist = redisDao.getWishlist(user1Id);
        System.out.println("User " + user1Id + " wishlist after removal: " + wishlist);
        
        // 7. Page visit counter
        System.out.println("\n7. Page visit counters:");
        for (int i = 0; i < 5; i++) {
            redisDao.incrementPageVisit("https://example.com/home");
            redisDao.incrementPageVisit("https://example.com/products");
            redisDao.incrementPageVisit("https://example.com/about");
        }
        
        // Increment one page more times
        for (int i = 0; i < 3; i++) {
            redisDao.incrementPageVisit("https://example.com/home");
        }
        
        Map<String, Long> pageVisits = redisDao.getAllPageVisits();
        System.out.println("Page visits: " + pageVisits);
        
        // 8. Product sales ranking
        System.out.println("\n8. Product sales ranking:");
        redisDao.addToSalesRanking(product1Id, 100);
        redisDao.addToSalesRanking(product2Id, 50);
        
        // Increment sales
        Double newSales = redisDao.incrementSalesCount(product1Id, 25);
        System.out.println("New sales count for product " + product1Id + ": " + newSales);
        
        List<String> topProducts = redisDao.getTopProductsBySales(10);
        System.out.println("Top products by sales: " + topProducts);
        
        // 9. User activity tracking
        System.out.println("\n9. User activity tracking:");
        redisDao.saveUserActivity(user1Id, "Logged in", 10);
        redisDao.saveUserActivity(user1Id, "Viewed product " + product1Id, 10);
        redisDao.saveUserActivity(user1Id, "Added product " + product1Id + " to cart", 10);
        
        List<String> activities = redisDao.getUserActivities(user1Id, 10);
        System.out.println("User " + user1Id + " recent activities:");
        for (String activity : activities) {
            String[] parts = activity.split(":");
            System.out.println("  - " + parts[1] + " (at " + parts[0] + ")");
        }
        
        // 10. Session storage
        System.out.println("\n10. Session storage:");
        String sessionId = "sess_" + System.currentTimeMillis();
        Map<String, String> sessionData = new HashMap<>();
        sessionData.put("user_id", user1Id.toString());
        sessionData.put("logged_in", "true");
        sessionData.put("last_access", System.currentTimeMillis() + "");
        
        redisDao.storeSessionData(sessionId, sessionData, 3600);
        System.out.println("Stored session: " + sessionId);
        
        Map<String, String> retrievedSession = redisDao.getSessionData(sessionId);
        System.out.println("Retrieved session data: " + retrievedSession);
    }
    
    /**
     * Example 4: Performance comparison between Redis and PostgreSQL.
     */
    private static void runPerformanceComparison() {
        System.out.println("\n===== Performance Comparison =====");
        System.out.println("Running performance tests between Redis and PostgreSQL...");
        
        PerformanceComparison comparison = new PerformanceComparison();
        comparison.runAllBenchmarks();
    }
    
    /**
     * Example 5: Redis Pub/Sub.
     * This demonstrates Redis's publish/subscribe messaging capability.
     */
    private static void redisPubSubExample() {
        System.out.println("\n===== Redis Pub/Sub Example =====");
        
        // Create a channel name
        String channel = "notifications";
        
        // Create a subscriber in a separate thread
        Thread subscriberThread = new Thread(() -> {
            try (Jedis jedis = DatabaseConfig.getJedisPool().getResource()) {
                System.out.println("Subscriber is listening to channel: " + channel);
                
                // Subscribe to the channel
                jedis.subscribe(new redis.clients.jedis.JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        System.out.println("Received message: " + message + " from channel: " + channel);
                        
                        // Unsubscribe after receiving "EXIT" message
                        if (message.equals("EXIT")) {
                            unsubscribe();
                        }
                    }
                    
                    @Override
                    public void onSubscribe(String channel, int subscribedChannels) {
                        System.out.println("Subscribed to channel: " + channel);
                    }
                    
                    @Override
                    public void onUnsubscribe(String channel, int subscribedChannels) {
                        System.out.println("Unsubscribed from channel: " + channel);
                    }
                }, channel);
            }
        });
        
        // Start the subscriber
        subscriberThread.start();
        
        // Wait a bit to ensure subscriber is ready
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Now publish some messages
        try (Jedis jedis = DatabaseConfig.getJedisPool().getResource()) {
            for (int i = 1; i <= 3; i++) {
                String message = "Message #" + i;
                jedis.publish(channel, message);
                System.out.println("Published: " + message);
                
                // Pause between messages
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            // Send exit message to terminate subscriber
            jedis.publish(channel, "EXIT");
            System.out.println("Published: EXIT");
        }
        
        // Wait for subscriber to finish
        try {
            subscriberThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Example 6: Caching with Redis.
     * This demonstrates how to use Redis as a cache.
     */
    private static void cachingExample() {
        System.out.println("\n===== Redis Caching Example =====");
        
        // We'll simulate an expensive database operation and cache its results
        String cacheKey = "expensive_query_result";
        
        try (Jedis jedis = DatabaseConfig.getJedisPool().getResource()) {
            // Check if data is in cache
            String cachedResult = jedis.get(cacheKey);
            
            if (cachedResult != null) {
                System.out.println("Cache HIT! Retrieving data from Redis cache.");
                System.out.println("Data: " + cachedResult);
            } else {
                System.out.println("Cache MISS! Performing expensive operation...");
                
                // Simulate expensive database query
                try {
                    System.out.println("Executing expensive query (simulated delay)...");
                    Thread.sleep(3000); // Simulate 3 second delay
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                
                // Generate result
                String result = "Data from expensive operation at " + System.currentTimeMillis();
                System.out.println("Operation completed. Result: " + result);
                
                // Store in cache for future use with a TTL of 30 seconds
                jedis.setex(cacheKey, 30, result);
                System.out.println("Stored result in cache with 30 second TTL.");
            }
            
            // Demonstrate cache expiration
            System.out.println("\nCurrent TTL for cached data: " + jedis.ttl(cacheKey) + " seconds");
            
            System.out.println("\nWould you like to immediately clear the cache? (y/n)");
            String choice = scanner.nextLine().trim().toLowerCase();
            
            if (choice.equals("y")) {
                jedis.del(cacheKey);
                System.out.println("Cache cleared.");
            } else {
                System.out.println("Cache will expire automatically after TTL.");
            }
        }
    }
} 
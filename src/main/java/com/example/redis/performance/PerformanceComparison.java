package com.example.redis.performance;

import com.example.redis.dao.PostgresDao;
import com.example.redis.dao.RedisDao;
import com.example.redis.model.Product;
import com.example.redis.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Performance comparison between Redis and PostgreSQL.
 * This class runs various benchmark tests to compare the performance of similar operations.
 */
public class PerformanceComparison {
    
    private static final Logger logger = LoggerFactory.getLogger(PerformanceComparison.class);
    
    private final RedisDao redisDao;
    private final PostgresDao postgresDao;
    private final Random random = new Random();
    
    /**
     * Constructor that initializes the DAOs for Redis and PostgreSQL
     */
    public PerformanceComparison() {
        this.redisDao = new RedisDao();
        this.postgresDao = new PostgresDao();
    }
    
    /**
     * Run all benchmarks and output the results
     */
    public void runAllBenchmarks() {
        try {
            System.out.println("\n===== REDIS VS POSTGRESQL PERFORMANCE COMPARISON =====\n");
            
            // Test 1: Reading and writing a single user
            singleUserTest();
            
            // Test 2: Reading and writing multiple users in batch
            batchUserTest(1000);
            
            // Test 3: Reading and writing a product with consistent stock updates
            productStockTest(100);
            
            // Test 4: Increment counters (page visits)
            counterIncrementTest(1000);
            
            // Test 5: Get sorted data (leaderboard/ranking)
            sortedSetTest(100);
            
            // Test 6: Wishlist operations
            wishlistTest(10, 100);
            
            System.out.println("\n===== BENCHMARK COMPLETE =====\n");
        } catch (Exception e) {
            logger.error("Error running benchmarks", e);
        }
    }
    
    /**
     * Test 1: Benchmark creating, reading, and updating a single user
     * 
     * @throws SQLException If a database error occurs
     */
    private void singleUserTest() throws SQLException {
        System.out.println("\n----- Test 1: Single User Operations -----");
        
        // Create random user data
        String username = "user_" + UUID.randomUUID().toString().substring(0, 8);
        String email = username + "@example.com";
        User user = new User(username, email);
        
        // Test 1.1: Create a user
        System.out.println("\nCreating a single user:");
        
        // Redis Create
        long redisStartTime = System.nanoTime();
        Long redisUserId = redisDao.saveUser(user);
        long redisEndTime = System.nanoTime();
        long redisCreateTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // Reset user for PostgreSQL test
        user = new User(username, email);
        
        // PostgreSQL Create
        long pgStartTime = System.nanoTime();
        Long pgUserId = postgresDao.saveUser(user);
        long pgEndTime = System.nanoTime();
        long pgCreateTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        System.out.println("Redis: " + redisCreateTime + " μs");
        System.out.println("PostgreSQL: " + pgCreateTime + " μs");
        System.out.println("Difference: " + (pgCreateTime - redisCreateTime) + " μs (" + 
                           String.format("%.2fx", (double) pgCreateTime / redisCreateTime) + " faster with Redis)");
        
        // Test 1.2: Get a user by ID
        System.out.println("\nGetting a user by ID:");
        
        // Redis Get
        redisStartTime = System.nanoTime();
        User redisUser = redisDao.getUserById(redisUserId);
        redisEndTime = System.nanoTime();
        long redisGetTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Get
        pgStartTime = System.nanoTime();
        User pgUser = postgresDao.getUserById(pgUserId);
        pgEndTime = System.nanoTime();
        long pgGetTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        System.out.println("Redis: " + redisGetTime + " μs");
        System.out.println("PostgreSQL: " + pgGetTime + " μs");
        System.out.println("Difference: " + (pgGetTime - redisGetTime) + " μs (" + 
                           String.format("%.2fx", (double) pgGetTime / redisGetTime) + " faster with Redis)");
        
        // Test 1.3: Get a user by username (using secondary index)
        System.out.println("\nGetting a user by username (indexed lookup):");
        
        // Redis Get by username
        redisStartTime = System.nanoTime();
        User redisUserByUsername = redisDao.getUserByUsername(username);
        redisEndTime = System.nanoTime();
        long redisGetByUsernameTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Get by username
        pgStartTime = System.nanoTime();
        User pgUserByUsername = postgresDao.getUserByUsername(username);
        pgEndTime = System.nanoTime();
        long pgGetByUsernameTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        System.out.println("Redis: " + redisGetByUsernameTime + " μs");
        System.out.println("PostgreSQL: " + pgGetByUsernameTime + " μs");
        System.out.println("Difference: " + (pgGetByUsernameTime - redisGetByUsernameTime) + " μs (" + 
                           String.format("%.2fx", (double) pgGetByUsernameTime / redisGetByUsernameTime) + " faster with Redis)");
    }
    
    /**
     * Test 2: Benchmark batch operations with multiple users
     * 
     * @param count Number of users to create and read
     * @throws SQLException If a database error occurs
     */
    private void batchUserTest(int count) throws SQLException {
        System.out.println("\n----- Test 2: Batch User Operations (" + count + " users) -----");
        
        // Test 2.1: Create multiple users
        System.out.println("\nCreating " + count + " users:");
        
        List<User> users = new ArrayList<>();
        List<Long> redisUserIds = new ArrayList<>();
        List<Long> pgUserIds = new ArrayList<>();
        
        // Generate random users
        for (int i = 0; i < count; i++) {
            String username = "batch_user_" + UUID.randomUUID().toString().substring(0, 8);
            String email = username + "@example.com";
            users.add(new User(username, email));
        }
        
        // Redis Create Batch
        long redisStartTime = System.nanoTime();
        for (User user : users) {
            Long id = redisDao.saveUser(new User(user.getUsername(), user.getEmail()));
            redisUserIds.add(id);
        }
        long redisEndTime = System.nanoTime();
        long redisCreateTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Create Batch
        long pgStartTime = System.nanoTime();
        for (User user : users) {
            Long id = postgresDao.saveUser(new User(user.getUsername(), user.getEmail()));
            pgUserIds.add(id);
        }
        long pgEndTime = System.nanoTime();
        long pgCreateTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        System.out.println("Redis: " + redisCreateTime + " μs (avg: " + (redisCreateTime / count) + " μs per user)");
        System.out.println("PostgreSQL: " + pgCreateTime + " μs (avg: " + (pgCreateTime / count) + " μs per user)");
        System.out.println("Difference: " + (pgCreateTime - redisCreateTime) + " μs (" + 
                           String.format("%.2fx", (double) pgCreateTime / redisCreateTime) + " faster with Redis)");
        
        // Test 2.2: Batch read users
        System.out.println("\nReading " + count + " users by ID:");
        
        // Redis Get Batch (using pipelined operation for better performance)
        redisStartTime = System.nanoTime();
        List<User> redisUsers = redisDao.getUsersByIdsPipelined(redisUserIds);
        redisEndTime = System.nanoTime();
        long redisGetTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Get Batch
        pgStartTime = System.nanoTime();
        List<User> pgUsers = postgresDao.getUsersByIds(pgUserIds);
        pgEndTime = System.nanoTime();
        long pgGetTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        System.out.println("Redis (pipelined): " + redisGetTime + " μs (avg: " + (redisGetTime / count) + " μs per user)");
        System.out.println("PostgreSQL (IN query): " + pgGetTime + " μs (avg: " + (pgGetTime / count) + " μs per user)");
        System.out.println("Difference: " + (pgGetTime - redisGetTime) + " μs (" + 
                           String.format("%.2fx", (double) pgGetTime / redisGetTime) + " faster with Redis)");
    }
    
    /**
     * Test 3: Benchmark product stock operations with optimistic locking
     * 
     * @param iterations Number of stock update operations to perform
     * @throws SQLException If a database error occurs
     */
    private void productStockTest(int iterations) throws SQLException {
        System.out.println("\n----- Test 3: Product Stock Operations (" + iterations + " updates) -----");
        
        // Create a test product
        String productName = "Test Product " + UUID.randomUUID().toString().substring(0, 8);
        Product product = new Product(productName, new BigDecimal("99.99"), 1000);
        
        // Save to Redis
        Long redisProductId = redisDao.saveProduct(product);
        
        // Save to PostgreSQL
        product = new Product(productName, new BigDecimal("99.99"), 1000);
        Long pgProductId = postgresDao.saveProduct(product);
        
        // Test 3.1: Update stock value multiple times
        System.out.println("\nUpdating product stock " + iterations + " times:");
        
        // Redis Stock Updates
        long redisStartTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            int newStock = 1000 - i;
            redisDao.updateProductStock(redisProductId, newStock);
        }
        long redisEndTime = System.nanoTime();
        long redisUpdateTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Stock Updates
        long pgStartTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            int newStock = 1000 - i;
            postgresDao.updateProductStock(pgProductId, newStock);
        }
        long pgEndTime = System.nanoTime();
        long pgUpdateTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        System.out.println("Redis: " + redisUpdateTime + " μs (avg: " + (redisUpdateTime / iterations) + " μs per update)");
        System.out.println("PostgreSQL: " + pgUpdateTime + " μs (avg: " + (pgUpdateTime / iterations) + " μs per update)");
        System.out.println("Difference: " + (pgUpdateTime - redisUpdateTime) + " μs (" + 
                           String.format("%.2fx", (double) pgUpdateTime / redisUpdateTime) + " faster with Redis)");
        
        // Test 3.2: Decrement stock with optimistic locking (simulate concurrent access)
        System.out.println("\nDecrementing stock with optimistic locking " + iterations + " times:");
        
        // Reset stock to 1000
        redisDao.updateProductStock(redisProductId, 1000);
        postgresDao.updateProductStock(pgProductId, 1000);
        
        // Redis Decrement with Optimistic Locking
        redisStartTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            redisDao.decrementProductStock(redisProductId, 1);
        }
        redisEndTime = System.nanoTime();
        long redisDecrementTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Decrement with Transaction
        pgStartTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            postgresDao.decrementProductStock(pgProductId, 1);
        }
        pgEndTime = System.nanoTime();
        long pgDecrementTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        System.out.println("Redis (with WATCH/MULTI/EXEC): " + redisDecrementTime + " μs (avg: " + (redisDecrementTime / iterations) + " μs per decrement)");
        System.out.println("PostgreSQL (with transaction): " + pgDecrementTime + " μs (avg: " + (pgDecrementTime / iterations) + " μs per decrement)");
        System.out.println("Difference: " + (pgDecrementTime - redisDecrementTime) + " μs (" + 
                           String.format("%.2fx", (double) pgDecrementTime / redisDecrementTime) + " faster with Redis)");
    }
    
    /**
     * Test 4: Benchmark counter increment operations (page visits)
     * 
     * @param iterations Number of increment operations to perform
     * @throws SQLException If a database error occurs
     */
    private void counterIncrementTest(int iterations) throws SQLException {
        System.out.println("\n----- Test 4: Counter Increment Operations (" + iterations + " increments) -----");
        
        // Generate URLs for testing
        List<String> urls = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            urls.add("https://example.com/page" + i);
        }
        
        // Test 4.1: Increment counters
        System.out.println("\nIncrementing page visit counters " + iterations + " times:");
        
        // Redis Increment
        long redisStartTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            String url = urls.get(random.nextInt(urls.size()));
            redisDao.incrementPageVisit(url);
        }
        long redisEndTime = System.nanoTime();
        long redisIncrementTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Increment
        long pgStartTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            String url = urls.get(random.nextInt(urls.size()));
            postgresDao.incrementPageVisit(url);
        }
        long pgEndTime = System.nanoTime();
        long pgIncrementTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        System.out.println("Redis (HINCRBY): " + redisIncrementTime + " μs (avg: " + (redisIncrementTime / iterations) + " μs per increment)");
        System.out.println("PostgreSQL (INSERT ... ON CONFLICT): " + pgIncrementTime + " μs (avg: " + (pgIncrementTime / iterations) + " μs per increment)");
        System.out.println("Difference: " + (pgIncrementTime - redisIncrementTime) + " μs (" + 
                           String.format("%.2fx", (double) pgIncrementTime / redisIncrementTime) + " faster with Redis)");
        
        // Test 4.2: Retrieve all counters
        System.out.println("\nRetrieving all page visit counters:");
        
        // Redis Get All
        redisStartTime = System.nanoTime();
        Map<String, Long> redisVisits = redisDao.getAllPageVisits();
        redisEndTime = System.nanoTime();
        long redisGetAllTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Get All
        pgStartTime = System.nanoTime();
        Map<String, Long> pgVisits = postgresDao.getAllPageVisits();
        pgEndTime = System.nanoTime();
        long pgGetAllTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        System.out.println("Redis (HGETALL): " + redisGetAllTime + " μs");
        System.out.println("PostgreSQL (SELECT): " + pgGetAllTime + " μs");
        System.out.println("Difference: " + (pgGetAllTime - redisGetAllTime) + " μs (" + 
                           String.format("%.2fx", (double) pgGetAllTime / redisGetAllTime) + " faster with Redis)");
    }
    
    /**
     * Test 5: Benchmark sorted set operations (leaderboard/ranking)
     * 
     * @param count Number of products to rank
     * @throws SQLException If a database error occurs
     */
    private void sortedSetTest(int count) throws SQLException {
        System.out.println("\n----- Test 5: Sorted Set Operations (" + count + " products) -----");
        
        // Create products for testing
        List<Long> redisProductIds = new ArrayList<>();
        List<Long> pgProductIds = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            String name = "Product " + i;
            Product product = new Product(name, new BigDecimal("9.99"), 100);
            
            Long redisId = redisDao.saveProduct(product);
            redisProductIds.add(redisId);
            
            product = new Product(name, new BigDecimal("9.99"), 100);
            Long pgId = postgresDao.saveProduct(product);
            pgProductIds.add(pgId);
        }
        
        // Test 5.1: Add products to sales ranking
        System.out.println("\nAdding " + count + " products to sales ranking:");
        
        // Redis Add to Ranking
        long redisStartTime = System.nanoTime();
        for (int i = 0; i < count; i++) {
            Long productId = redisProductIds.get(i);
            long sales = random.nextInt(1000);
            redisDao.addToSalesRanking(productId, sales);
        }
        long redisEndTime = System.nanoTime();
        long redisAddTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Add to Ranking
        long pgStartTime = System.nanoTime();
        for (int i = 0; i < count; i++) {
            Long productId = pgProductIds.get(i);
            long sales = random.nextInt(1000);
            postgresDao.updateSalesRanking(productId, sales);
        }
        long pgEndTime = System.nanoTime();
        long pgAddTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        System.out.println("Redis (ZADD): " + redisAddTime + " μs (avg: " + (redisAddTime / count) + " μs per product)");
        System.out.println("PostgreSQL (INSERT/UPDATE): " + pgAddTime + " μs (avg: " + (pgAddTime / count) + " μs per product)");
        System.out.println("Difference: " + (pgAddTime - redisAddTime) + " μs (" + 
                           String.format("%.2fx", (double) pgAddTime / redisAddTime) + " faster with Redis)");
        
        // Test 5.2: Increment sales count
        System.out.println("\nIncrementing sales count for " + count + " products:");
        
        // Redis Increment
        redisStartTime = System.nanoTime();
        for (int i = 0; i < count; i++) {
            Long productId = redisProductIds.get(i);
            redisDao.incrementSalesCount(productId, 1);
        }
        redisEndTime = System.nanoTime();
        long redisIncrementTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Increment
        pgStartTime = System.nanoTime();
        for (int i = 0; i < count; i++) {
            Long productId = pgProductIds.get(i);
            postgresDao.incrementSalesCount(productId, 1);
        }
        pgEndTime = System.nanoTime();
        long pgIncrementTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        System.out.println("Redis (ZINCRBY): " + redisIncrementTime + " μs (avg: " + (redisIncrementTime / count) + " μs per increment)");
        System.out.println("PostgreSQL (UPDATE): " + pgIncrementTime + " μs (avg: " + (pgIncrementTime / count) + " μs per increment)");
        System.out.println("Difference: " + (pgIncrementTime - redisIncrementTime) + " μs (" + 
                           String.format("%.2fx", (double) pgIncrementTime / redisIncrementTime) + " faster with Redis)");
        
        // Test 5.3: Get top products
        System.out.println("\nGetting top 10 products by sales:");
        
        // Redis Get Top
        redisStartTime = System.nanoTime();
        redisDao.getTopProductsBySales(10);
        redisEndTime = System.nanoTime();
        long redisGetTopTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Get Top
        pgStartTime = System.nanoTime();
        postgresDao.getTopProductsBySales(10);
        pgEndTime = System.nanoTime();
        long pgGetTopTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        System.out.println("Redis (ZREVRANGE): " + redisGetTopTime + " μs");
        System.out.println("PostgreSQL (ORDER BY ... LIMIT): " + pgGetTopTime + " μs");
        System.out.println("Difference: " + (pgGetTopTime - redisGetTopTime) + " μs (" + 
                           String.format("%.2fx", (double) pgGetTopTime / redisGetTopTime) + " faster with Redis)");
    }
    
    /**
     * Test 6: Benchmark wishlist operations
     * 
     * @param userCount Number of users with wishlists
     * @param productCount Number of potential products for wishlists
     * @throws SQLException If a database error occurs
     */
    private void wishlistTest(int userCount, int productCount) throws SQLException {
        System.out.println("\n----- Test 6: Wishlist Operations (" + userCount + " users, " + productCount + " products) -----");
        
        // Create users and products for testing
        List<Long> redisUserIds = new ArrayList<>();
        List<Long> pgUserIds = new ArrayList<>();
        List<Long> redisProductIds = new ArrayList<>();
        List<Long> pgProductIds = new ArrayList<>();
        
        // Create users
        for (int i = 0; i < userCount; i++) {
            String username = "wishlist_user_" + i;
            User user = new User(username, username + "@example.com");
            
            Long redisId = redisDao.saveUser(user);
            redisUserIds.add(redisId);
            
            user = new User(username, username + "@example.com");
            Long pgId = postgresDao.saveUser(user);
            pgUserIds.add(pgId);
        }
        
        // Create products
        for (int i = 0; i < productCount; i++) {
            String name = "Wishlist Product " + i;
            Product product = new Product(name, new BigDecimal("19.99"), 50);
            
            Long redisId = redisDao.saveProduct(product);
            redisProductIds.add(redisId);
            
            product = new Product(name, new BigDecimal("19.99"), 50);
            Long pgId = postgresDao.saveProduct(product);
            pgProductIds.add(pgId);
        }
        
        // Test 6.1: Add products to wishlist
        System.out.println("\nAdding products to wishlists (10 products per user):");
        
        // Redis Add to Wishlist
        long redisStartTime = System.nanoTime();
        for (Long userId : redisUserIds) {
            for (int i = 0; i < 10; i++) {
                Long productId = redisProductIds.get(random.nextInt(productCount));
                redisDao.addToWishlist(userId, productId);
            }
        }
        long redisEndTime = System.nanoTime();
        long redisAddTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Add to Wishlist
        long pgStartTime = System.nanoTime();
        for (Long userId : pgUserIds) {
            for (int i = 0; i < 10; i++) {
                Long productId = pgProductIds.get(random.nextInt(productCount));
                postgresDao.addToWishlist(userId, productId);
            }
        }
        long pgEndTime = System.nanoTime();
        long pgAddTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        int totalOperations = userCount * 10;
        System.out.println("Redis (SADD): " + redisAddTime + " μs (avg: " + (redisAddTime / totalOperations) + " μs per add)");
        System.out.println("PostgreSQL (INSERT): " + pgAddTime + " μs (avg: " + (pgAddTime / totalOperations) + " μs per add)");
        System.out.println("Difference: " + (pgAddTime - redisAddTime) + " μs (" + 
                           String.format("%.2fx", (double) pgAddTime / redisAddTime) + " faster with Redis)");
        
        // Test 6.2: Get user wishlists
        System.out.println("\nGetting user wishlists:");
        
        // Redis Get Wishlist
        redisStartTime = System.nanoTime();
        for (Long userId : redisUserIds) {
            redisDao.getWishlist(userId);
        }
        redisEndTime = System.nanoTime();
        long redisGetTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Get Wishlist
        pgStartTime = System.nanoTime();
        for (Long userId : pgUserIds) {
            postgresDao.getWishlist(userId);
        }
        pgEndTime = System.nanoTime();
        long pgGetTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        System.out.println("Redis (SMEMBERS): " + redisGetTime + " μs (avg: " + (redisGetTime / userCount) + " μs per user)");
        System.out.println("PostgreSQL (SELECT): " + pgGetTime + " μs (avg: " + (pgGetTime / userCount) + " μs per user)");
        System.out.println("Difference: " + (pgGetTime - redisGetTime) + " μs (" + 
                           String.format("%.2fx", (double) pgGetTime / redisGetTime) + " faster with Redis)");
        
        // Test 6.3: Remove from wishlist
        System.out.println("\nRemoving products from wishlists (5 removals per user):");
        
        // Redis Remove from Wishlist
        redisStartTime = System.nanoTime();
        for (Long userId : redisUserIds) {
            for (int i = 0; i < 5; i++) {
                Long productId = redisProductIds.get(random.nextInt(productCount));
                redisDao.removeFromWishlist(userId, productId);
            }
        }
        redisEndTime = System.nanoTime();
        long redisRemoveTime = TimeUnit.NANOSECONDS.toMicros(redisEndTime - redisStartTime);
        
        // PostgreSQL Remove from Wishlist
        pgStartTime = System.nanoTime();
        for (Long userId : pgUserIds) {
            for (int i = 0; i < 5; i++) {
                Long productId = pgProductIds.get(random.nextInt(productCount));
                postgresDao.removeFromWishlist(userId, productId);
            }
        }
        pgEndTime = System.nanoTime();
        long pgRemoveTime = TimeUnit.NANOSECONDS.toMicros(pgEndTime - pgStartTime);
        
        totalOperations = userCount * 5;
        System.out.println("Redis (SREM): " + redisRemoveTime + " μs (avg: " + (redisRemoveTime / totalOperations) + " μs per remove)");
        System.out.println("PostgreSQL (DELETE): " + pgRemoveTime + " μs (avg: " + (pgRemoveTime / totalOperations) + " μs per remove)");
        System.out.println("Difference: " + (pgRemoveTime - redisRemoveTime) + " μs (" + 
                           String.format("%.2fx", (double) pgRemoveTime / redisRemoveTime) + " faster with Redis)");
    }
} 
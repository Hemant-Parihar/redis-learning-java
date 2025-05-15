package com.example.redis.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Configuration class to manage database connections for both Redis and PostgreSQL.
 * This class provides connection pools for both databases to ensure efficient resource usage.
 */
public class DatabaseConfig {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfig.class);
    
    // Redis configuration
    private static final String REDIS_HOST = System.getenv("REDIS_HOST") != null ? System.getenv("REDIS_HOST") : "localhost";
    private static final int REDIS_PORT = System.getenv("REDIS_PORT") != null ? Integer.parseInt(System.getenv("REDIS_PORT")) : 6379;
    private static final String REDIS_PASSWORD = System.getenv("REDIS_PASSWORD") != null ? System.getenv("REDIS_PASSWORD") : "redispass";
    
    // PostgreSQL configuration
    private static final String PG_HOST = System.getenv("PG_HOST") != null ? System.getenv("PG_HOST") : "localhost";
    private static final int PG_PORT = System.getenv("PG_PORT") != null ? Integer.parseInt(System.getenv("PG_PORT")) : 5432;
    private static final String PG_DATABASE = System.getenv("PG_DATABASE") != null ? System.getenv("PG_DATABASE") : "rediscomparison";
    private static final String PG_USER = System.getenv("PG_USER") != null ? System.getenv("PG_USER") : "postgres";
    private static final String PG_PASSWORD = System.getenv("PG_PASSWORD") != null ? System.getenv("PG_PASSWORD") : "postgrespass";
    
    // Connection pool instances
    private static JedisPool jedisPool;
    private static HikariDataSource hikariDataSource;
    
    /**
     * The Jedis connection pool manages a pool of Redis connections for efficient resource utilization.
     * Here's how it works:
     * 
     * 1. Pool Configuration:
     *    - MaxTotal (10): Maximum number of connections that can be created in the pool
     *    - MaxIdle (5): Maximum number of idle connections kept in the pool
     *    - MinIdle (1): Minimum number of idle connections to maintain in the pool
     * 
     * 2. Connection Testing:
     *    - TestOnBorrow: Validates connection before giving it to the application
     *    - TestOnReturn: Validates connection when returned to the pool
     *    - TestWhileIdle: Periodically tests idle connections to ensure they're still valid
     * 
     * 3. Connection Management:
     *    - When application requests a connection:
     *      a) If idle connection available -> reuse it
     *      b) If no idle connection and below maxTotal -> create new connection
     *      c) If at maxTotal -> wait for connection to be returned
     *    - Connections are automatically returned to pool after use
     *    
     * 4. Connection Parameters:
     *    - Host: Redis server location (default: localhost)
     *    - Port: Redis port (default: 6379)
     *    - Timeout: Connection timeout (2000ms)
     *    - Password: Redis authentication (if enabled)
     */

    /**
     * Get a Jedis connection pool for Redis.
     * The pool is created with sensible defaults for connection timeouts and pool sizes.
     *
     * @return A configured JedisPool instance
     */
    public static synchronized JedisPool getJedisPool() {
        if (jedisPool == null) {
            try {
                // Configure Jedis pool settings
                final JedisPoolConfig poolConfig = new JedisPoolConfig();
                poolConfig.setMaxTotal(10);                 // Maximum connections in pool
                poolConfig.setMaxIdle(5);                   // Maximum idle connections in pool
                poolConfig.setMinIdle(1);                   // Minimum idle connections in pool
                poolConfig.setTestOnBorrow(true);           // Test connection before borrowing from pool
                poolConfig.setTestOnReturn(true);           // Test connection before returning to pool
                poolConfig.setTestWhileIdle(true);          // Test idle connections
                
                // Create the Jedis connection pool
                jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, 2000, REDIS_PASSWORD);
                
                logger.info("Redis connection pool initialized successfully");
            } catch (Exception e) {
                logger.error("Failed to initialize Redis connection pool", e);
                throw new RuntimeException("Could not initialize Redis connection pool", e);
            }
        }
        return jedisPool;
    }
    
    /**
     * Get a HikariCP connection pool for PostgreSQL.
     * HikariCP is a high-performance JDBC connection pool.
     *
     * @return A configured HikariDataSource instance
     */
    public static synchronized DataSource getPostgresDataSource() {
        if (hikariDataSource == null) {
            try {
                // Configure HikariCP
                HikariConfig config = new HikariConfig();
                config.setJdbcUrl(String.format("jdbc:postgresql://%s:%d/%s", PG_HOST, PG_PORT, PG_DATABASE));
                config.setUsername(PG_USER);
                config.setPassword(PG_PASSWORD);
                config.setDriverClassName("org.postgresql.Driver");
                
                // Connection pool settings
                config.setMaximumPoolSize(10);              // Maximum size of the connection pool
                config.setMinimumIdle(2);                   // Minimum number of idle connections
                config.setIdleTimeout(30000);               // Maximum idle time for connection
                config.setConnectionTimeout(10000);         // Maximum wait time when getting a connection from the pool
                config.setAutoCommit(false);                // Don't auto-commit transactions
                
                // Create the HikariCP data source
                hikariDataSource = new HikariDataSource(config);
                
                // Initialize database schema if needed
                initializeDatabase();
                
                logger.info("PostgreSQL connection pool initialized successfully");
            } catch (Exception e) {
                logger.error("Failed to initialize PostgreSQL connection pool", e);
                throw new RuntimeException("Could not initialize PostgreSQL connection pool", e);
            }
        }
        return hikariDataSource;
    }
    
    /**
     * Initialize the PostgreSQL database schema by creating necessary tables.
     * This is called when the connection pool is first established.
     */
    private static void initializeDatabase() {
        try (Connection conn = hikariDataSource.getConnection()) {
            // Create tables needed for our examples
            String createTableSQL = 
                "CREATE TABLE IF NOT EXISTS users (" +
                "   id SERIAL PRIMARY KEY," +
                "   username VARCHAR(100) NOT NULL UNIQUE," +
                "   email VARCHAR(255) NOT NULL," +
                "   created_at TIMESTAMP NOT NULL DEFAULT NOW()" +
                ");" +
                
                "CREATE TABLE IF NOT EXISTS products (" +
                "   id SERIAL PRIMARY KEY," +
                "   name VARCHAR(255) NOT NULL," +
                "   price NUMERIC(10,2) NOT NULL," +
                "   stock INT NOT NULL DEFAULT 0," +
                "   created_at TIMESTAMP NOT NULL DEFAULT NOW()" +
                ");" +
                
                "CREATE TABLE IF NOT EXISTS page_visits (" +
                "   id SERIAL PRIMARY KEY," +
                "   url VARCHAR(255) NOT NULL UNIQUE," +
                "   visit_count INT NOT NULL DEFAULT 0," +
                "   last_visit TIMESTAMP NOT NULL DEFAULT NOW()" +
                ");";
            
            conn.createStatement().execute(createTableSQL);
            conn.commit();
            logger.info("Database schema initialized successfully");
        } catch (SQLException e) {
            logger.error("Error initializing database schema", e);
            throw new RuntimeException("Failed to initialize database schema", e);
        }
    }
    
    /**
     * Close all database connections and clean up resources.
     * Should be called when the application is shutting down.
     */
    public static void closeConnections() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
            logger.info("Redis connection pool closed");
        }
        
        if (hikariDataSource != null && !hikariDataSource.isClosed()) {
            hikariDataSource.close();
            logger.info("PostgreSQL connection pool closed");
        }
    }
} 
package com.example.redis.dao;

import com.example.redis.config.DatabaseConfig;
import com.example.redis.model.Product;
import com.example.redis.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Data Access Object for PostgreSQL operations.
 * This class provides methods for database operations equivalent to the Redis ones
 * to compare performance and usage patterns.
 */
public class PostgresDao {
    
    private static final Logger logger = LoggerFactory.getLogger(PostgresDao.class);
    private final DataSource dataSource;
    
    /**
     * Constructor that initializes the PostgreSQL connection pool
     */
    public PostgresDao() {
        this.dataSource = DatabaseConfig.getPostgresDataSource();
    }
    
    /**
     * Save a user to PostgreSQL.
     * 
     * @param user The user to save
     * @return The ID of the saved user
     * @throws SQLException If a database error occurs
     */
    public Long saveUser(User user) throws SQLException {
        String sql = "INSERT INTO users (username, email, created_at) VALUES (?, ?, ?) RETURNING id";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            
            pstmt.setString(1, user.getUsername());
            pstmt.setString(2, user.getEmail());
            pstmt.setTimestamp(3, Timestamp.valueOf(user.getCreatedAt() != null ? user.getCreatedAt() : LocalDateTime.now()));
            
            int affectedRows = pstmt.executeUpdate();
            
            if (affectedRows == 0) {
                throw new SQLException("Creating user failed, no rows affected.");
            }
            
            try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    Long id = generatedKeys.getLong(1);
                    user.setId(id);
                    conn.commit();
                    return id;
                } else {
                    conn.rollback();
                    throw new SQLException("Creating user failed, no ID obtained.");
                }
            }
        }
    }
    
    /**
     * Update an existing user in PostgreSQL.
     * 
     * @param user The user to update
     * @return true if successful, false otherwise
     * @throws SQLException If a database error occurs
     */
    public boolean updateUser(User user) throws SQLException {
        String sql = "UPDATE users SET username = ?, email = ? WHERE id = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, user.getUsername());
            pstmt.setString(2, user.getEmail());
            pstmt.setLong(3, user.getId());
            
            int affectedRows = pstmt.executeUpdate();
            
            if (affectedRows > 0) {
                conn.commit();
                return true;
            } else {
                conn.rollback();
                return false;
            }
        }
    }
    
    /**
     * Get a user from PostgreSQL by ID.
     * 
     * @param id The user ID
     * @return The User object or null if not found
     * @throws SQLException If a database error occurs
     */
    public User getUserById(Long id) throws SQLException {
        String sql = "SELECT id, username, email, created_at FROM users WHERE id = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, id);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new User(
                        rs.getLong("id"),
                        rs.getString("username"),
                        rs.getString("email"),
                        rs.getTimestamp("created_at").toLocalDateTime()
                    );
                }
                return null;
            }
        }
    }
    
    /**
     * Get a user from PostgreSQL by username.
     * 
     * @param username The username to look up
     * @return The User object or null if not found
     * @throws SQLException If a database error occurs
     */
    public User getUserByUsername(String username) throws SQLException {
        String sql = "SELECT id, username, email, created_at FROM users WHERE username = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, username);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new User(
                        rs.getLong("id"),
                        rs.getString("username"),
                        rs.getString("email"),
                        rs.getTimestamp("created_at").toLocalDateTime()
                    );
                }
                return null;
            }
        }
    }
    
    /**
     * Save a product to PostgreSQL.
     * 
     * @param product The product to save
     * @return The ID of the saved product
     * @throws SQLException If a database error occurs
     */
    public Long saveProduct(Product product) throws SQLException {
        String sql = "INSERT INTO products (name, price, stock, created_at) VALUES (?, ?, ?, ?) RETURNING id";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            
            pstmt.setString(1, product.getName());
            pstmt.setBigDecimal(2, product.getPrice());
            pstmt.setInt(3, product.getStock());
            pstmt.setTimestamp(4, Timestamp.valueOf(product.getCreatedAt() != null ? product.getCreatedAt() : LocalDateTime.now()));
            
            int affectedRows = pstmt.executeUpdate();
            
            if (affectedRows == 0) {
                throw new SQLException("Creating product failed, no rows affected.");
            }
            
            try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    Long id = generatedKeys.getLong(1);
                    product.setId(id);
                    conn.commit();
                    return id;
                } else {
                    conn.rollback();
                    throw new SQLException("Creating product failed, no ID obtained.");
                }
            }
        }
    }
    
    /**
     * Get a product from PostgreSQL by ID.
     * 
     * @param id The product ID
     * @return The Product object or null if not found
     * @throws SQLException If a database error occurs
     */
    public Product getProductById(Long id) throws SQLException {
        String sql = "SELECT id, name, price, stock, created_at FROM products WHERE id = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, id);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new Product(
                        rs.getLong("id"),
                        rs.getString("name"),
                        rs.getBigDecimal("price"),
                        rs.getInt("stock"),
                        rs.getTimestamp("created_at").toLocalDateTime()
                    );
                }
                return null;
            }
        }
    }
    
    /**
     * Update product stock in PostgreSQL.
     * 
     * @param productId The product ID
     * @param newStockValue The new stock value
     * @return true if update was successful, false if product not found
     * @throws SQLException If a database error occurs
     */
    public boolean updateProductStock(Long productId, int newStockValue) throws SQLException {
        String sql = "UPDATE products SET stock = ? WHERE id = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, newStockValue);
            pstmt.setLong(2, productId);
            
            int affectedRows = pstmt.executeUpdate();
            
            if (affectedRows > 0) {
                conn.commit();
                return true;
            } else {
                conn.rollback();
                return false;
            }
        }
    }
    
    /**
     * Attempt to decrement product stock with optimistic locking.
     * Uses a transaction with isolation level to prevent race conditions.
     * 
     * @param productId The product ID
     * @param amount The amount to decrement
     * @return true if decrement was successful, false if insufficient stock or product not found
     * @throws SQLException If a database error occurs
     */
    public boolean decrementProductStock(Long productId, int amount) throws SQLException {
        String selectSql = "SELECT stock FROM products WHERE id = ? FOR UPDATE";
        String updateSql = "UPDATE products SET stock = ? WHERE id = ?";
        
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            
            // Set transaction isolation level to ensure consistency
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            
            // Get current stock with lock
            try (PreparedStatement selectStmt = conn.prepareStatement(selectSql)) {
                selectStmt.setLong(1, productId);
                
                try (ResultSet rs = selectStmt.executeQuery()) {
                    if (!rs.next()) {
                        conn.rollback();
                        return false;  // Product not found
                    }
                    
                    int currentStock = rs.getInt("stock");
                    if (currentStock < amount) {
                        conn.rollback();
                        return false;  // Insufficient stock
                    }
                    
                    // Update the stock
                    try (PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {
                        updateStmt.setInt(1, currentStock - amount);
                        updateStmt.setLong(2, productId);
                        
                        updateStmt.executeUpdate();
                        conn.commit();
                        return true;
                    }
                }
            }
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    logger.error("Error rolling back transaction", ex);
                }
            }
            throw e;
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException e) {
                    logger.error("Error resetting connection state", e);
                }
            }
        }
    }
    
    /**
     * Increment a page visit counter in PostgreSQL.
     * 
     * @param url The page URL
     * @return The new visit count
     * @throws SQLException If a database error occurs
     */
    public Long incrementPageVisit(String url) throws SQLException {
        String upsertSql = 
            "INSERT INTO page_visits (url, visit_count, last_visit) " +
            "VALUES (?, 1, NOW()) " +
            "ON CONFLICT (url) " +
            "DO UPDATE SET visit_count = page_visits.visit_count + 1, " +
            "last_visit = NOW() " +
            "RETURNING visit_count";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(upsertSql)) {
            
            pstmt.setString(1, url);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    Long count = rs.getLong("visit_count");
                    conn.commit();
                    return count;
                } else {
                    conn.rollback();
                    throw new SQLException("Incrementing page visit failed, no count returned.");
                }
            }
        }
    }
    
    /**
     * Get all page visit counts from PostgreSQL.
     * 
     * @return Map of URL to visit count
     * @throws SQLException If a database error occurs
     */
    public Map<String, Long> getAllPageVisits() throws SQLException {
        String sql = "SELECT url, visit_count FROM page_visits";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            Map<String, Long> visits = new HashMap<>();
            
            while (rs.next()) {
                visits.put(rs.getString("url"), rs.getLong("visit_count"));
            }
            
            return visits;
        }
    }
    
    /**
     * Get multiple users by their IDs in a batch.
     * 
     * @param userIds List of user IDs to fetch
     * @return List of users
     * @throws SQLException If a database error occurs
     */
    public List<User> getUsersByIds(List<Long> userIds) throws SQLException {
        if (userIds == null || userIds.isEmpty()) {
            return new ArrayList<>();
        }
        
        // Create SQL IN clause placeholder strings
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < userIds.size(); i++) {
            if (i > 0) {
                placeholders.append(",");
            }
            placeholders.append("?");
        }
        
        String sql = "SELECT id, username, email, created_at FROM users WHERE id IN (" + placeholders + ")";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            // Set all the IDs as parameters
            for (int i = 0; i < userIds.size(); i++) {
                pstmt.setLong(i + 1, userIds.get(i));
            }
            
            try (ResultSet rs = pstmt.executeQuery()) {
                List<User> users = new ArrayList<>();
                
                while (rs.next()) {
                    users.add(new User(
                        rs.getLong("id"),
                        rs.getString("username"),
                        rs.getString("email"),
                        rs.getTimestamp("created_at").toLocalDateTime()
                    ));
                }
                
                return users;
            }
        }
    }
    
    /**
     * Add products to a user's wishlist.
     * This requires creating a separate wishlist table in PostgreSQL.
     * 
     * @param userId The user ID
     * @param productId The product ID to add
     * @throws SQLException If a database error occurs
     */
    public void addToWishlist(Long userId, Long productId) throws SQLException {
        // First check if we need to create the wishlist table
        createWishlistTableIfNotExists();
        
        String sql = "INSERT INTO wishlist (user_id, product_id, added_at) VALUES (?, ?, NOW()) ON CONFLICT DO NOTHING";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, userId);
            pstmt.setLong(2, productId);
            
            pstmt.executeUpdate();
            conn.commit();
        }
    }
    
    /**
     * Remove a product from a user's wishlist.
     * 
     * @param userId The user ID
     * @param productId The product ID to remove
     * @throws SQLException If a database error occurs
     */
    public void removeFromWishlist(Long userId, Long productId) throws SQLException {
        // First check if we need to create the wishlist table
        createWishlistTableIfNotExists();
        
        String sql = "DELETE FROM wishlist WHERE user_id = ? AND product_id = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, userId);
            pstmt.setLong(2, productId);
            
            pstmt.executeUpdate();
            conn.commit();
        }
    }
    
    /**
     * Get a user's wishlist.
     * 
     * @param userId The user ID
     * @return Set of product IDs in the wishlist
     * @throws SQLException If a database error occurs
     */
    public Set<String> getWishlist(Long userId) throws SQLException {
        // First check if we need to create the wishlist table
        createWishlistTableIfNotExists();
        
        String sql = "SELECT product_id FROM wishlist WHERE user_id = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, userId);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                Set<String> productIds = new HashSet<>();
                
                while (rs.next()) {
                    productIds.add(String.valueOf(rs.getLong("product_id")));
                }
                
                return productIds;
            }
        }
    }
    
    /**
     * Helper method to create the wishlist table if it doesn't exist.
     * 
     * @throws SQLException If a database error occurs
     */
    private void createWishlistTableIfNotExists() throws SQLException {
        String createTableSQL = 
            "CREATE TABLE IF NOT EXISTS wishlist (" +
            "   user_id BIGINT NOT NULL," +
            "   product_id BIGINT NOT NULL," +
            "   added_at TIMESTAMP NOT NULL DEFAULT NOW()," +
            "   PRIMARY KEY (user_id, product_id)," +
            "   CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE," +
            "   CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE" +
            ")";
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.execute(createTableSQL);
            conn.commit();
        }
    }
    
    /**
     * Create or update sales ranking for a product.
     * This requires creating a separate sales_ranking table in PostgreSQL.
     * 
     * @param productId The product ID
     * @param salesCount The sales count to set
     * @throws SQLException If a database error occurs
     */
    public void updateSalesRanking(Long productId, long salesCount) throws SQLException {
        // First check if we need to create the sales ranking table
        createSalesRankingTableIfNotExists();
        
        String sql = 
            "INSERT INTO sales_ranking (product_id, sales_count, last_updated) " +
            "VALUES (?, ?, NOW()) " +
            "ON CONFLICT (product_id) " +
            "DO UPDATE SET sales_count = ?, last_updated = NOW()";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, productId);
            pstmt.setLong(2, salesCount);
            pstmt.setLong(3, salesCount);
            
            pstmt.executeUpdate();
            conn.commit();
        }
    }
    
    /**
     * Increment a product's sales count in the ranking.
     * 
     * @param productId The product ID
     * @param incrementBy The amount to increment by
     * @return The new sales count
     * @throws SQLException If a database error occurs
     */
    public Long incrementSalesCount(Long productId, long incrementBy) throws SQLException {
        // First check if we need to create the sales ranking table
        createSalesRankingTableIfNotExists();
        
        String sql = 
            "INSERT INTO sales_ranking (product_id, sales_count, last_updated) " +
            "VALUES (?, ?, NOW()) " +
            "ON CONFLICT (product_id) " +
            "DO UPDATE SET sales_count = sales_ranking.sales_count + ?, last_updated = NOW() " +
            "RETURNING sales_count";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, productId);
            pstmt.setLong(2, incrementBy);
            pstmt.setLong(3, incrementBy);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    Long newCount = rs.getLong("sales_count");
                    conn.commit();
                    return newCount;
                } else {
                    conn.rollback();
                    throw new SQLException("Incrementing sales count failed, no count returned.");
                }
            }
        }
    }
    
    /**
     * Get the top N products by sales.
     * 
     * @param count The number of top products to get
     * @return List of product IDs ordered by sales count
     * @throws SQLException If a database error occurs
     */
    public List<String> getTopProductsBySales(int count) throws SQLException {
        // First check if we need to create the sales ranking table
        createSalesRankingTableIfNotExists();
        
        String sql = "SELECT product_id FROM sales_ranking ORDER BY sales_count DESC LIMIT ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, count);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                List<String> productIds = new ArrayList<>();
                
                while (rs.next()) {
                    productIds.add(String.valueOf(rs.getLong("product_id")));
                }
                
                return productIds;
            }
        }
    }
    
    /**
     * Helper method to create the sales ranking table if it doesn't exist.
     * 
     * @throws SQLException If a database error occurs
     */
    private void createSalesRankingTableIfNotExists() throws SQLException {
        String createTableSQL = 
            "CREATE TABLE IF NOT EXISTS sales_ranking (" +
            "   product_id BIGINT PRIMARY KEY," +
            "   sales_count BIGINT NOT NULL DEFAULT 0," +
            "   last_updated TIMESTAMP NOT NULL DEFAULT NOW()," +
            "   CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE" +
            ")";
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.execute(createTableSQL);
            conn.commit();
        }
    }
    
    /**
     * Save user activity in PostgreSQL.
     * 
     * @param userId The user ID
     * @param activity The activity description 
     * @throws SQLException If a database error occurs
     */
    public void saveUserActivity(Long userId, String activity) throws SQLException {
        // First check if we need to create the user activities table
        createUserActivitiesTableIfNotExists();
        
        String sql = "INSERT INTO user_activities (user_id, activity, created_at) VALUES (?, ?, NOW())";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, userId);
            pstmt.setString(2, activity);
            
            pstmt.executeUpdate();
            conn.commit();
        }
    }
    
    /**
     * Get recent user activities.
     * 
     * @param userId The user ID
     * @param count Number of recent activities to retrieve
     * @return List of user activities
     * @throws SQLException If a database error occurs
     */
    public List<String> getUserActivities(Long userId, int count) throws SQLException {
        // First check if we need to create the user activities table
        createUserActivitiesTableIfNotExists();
        
        String sql = "SELECT activity, created_at FROM user_activities WHERE user_id = ? ORDER BY created_at DESC LIMIT ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setLong(1, userId);
            pstmt.setInt(2, count);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                List<String> activities = new ArrayList<>();
                
                while (rs.next()) {
                    // Format activity with timestamp
                    activities.add(rs.getTimestamp("created_at").getTime() + ":" + rs.getString("activity"));
                }
                
                return activities;
            }
        }
    }
    
    /**
     * Helper method to create the user activities table if it doesn't exist.
     * 
     * @throws SQLException If a database error occurs
     */
    private void createUserActivitiesTableIfNotExists() throws SQLException {
        String createTableSQL = 
            "CREATE TABLE IF NOT EXISTS user_activities (" +
            "   id SERIAL PRIMARY KEY," +
            "   user_id BIGINT NOT NULL," +
            "   activity TEXT NOT NULL," +
            "   created_at TIMESTAMP NOT NULL DEFAULT NOW()," +
            "   CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE" +
            ")";
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.execute(createTableSQL);
            conn.commit();
        }
    }
} 
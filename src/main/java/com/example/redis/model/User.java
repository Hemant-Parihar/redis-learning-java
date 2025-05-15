package com.example.redis.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * User model class to demonstrate Redis and PostgreSQL operations.
 * This class is serializable so it can be stored in Redis.
 */
public class User implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private Long id;
    private String username;
    private String email;
    private LocalDateTime createdAt;
    
    /**
     * Default constructor required for serialization
     */
    public User() {
    }
    
    /**
     * Constructor with all fields except ID (for new user creation)
     */
    public User(String username, String email) {
        this.username = username;
        this.email = email;
        this.createdAt = LocalDateTime.now();
    }
    
    /**
     * Constructor with all fields (for existing user retrieval)
     */
    public User(Long id, String username, String email, LocalDateTime createdAt) {
        this.id = id;
        this.username = username;
        this.email = email;
        this.createdAt = createdAt;
    }
    
    // Getters and Setters
    
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getUsername() {
        return username;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public String getEmail() {
        return email;
    }
    
    public void setEmail(String email) {
        this.email = email;
    }
    
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    /**
     * Convert user object to a string representation suitable for Redis storage
     * Format: id|username|email|createdAt
     */
    public String toRedisString() {
        return String.format("%d|%s|%s|%s", 
            id == null ? 0 : id, 
            username, 
            email, 
            createdAt == null ? LocalDateTime.now() : createdAt);
    }
    
    /**
     * Create a User object from a Redis string representation
     */
    public static User fromRedisString(String str) {
        if (str == null || str.isEmpty()) {
            return null;
        }
        
        String[] parts = str.split("\\|");
        if (parts.length < 4) {
            throw new IllegalArgumentException("Invalid Redis string format for User");
        }
        
        User user = new User();
        user.setId(Long.parseLong(parts[0]));
        user.setUsername(parts[1]);
        user.setEmail(parts[2]);
        user.setCreatedAt(LocalDateTime.parse(parts[3]));
        
        return user;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return Objects.equals(id, user.id) &&
               Objects.equals(username, user.username) &&
               Objects.equals(email, user.email);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, username, email);
    }
    
    @Override
    public String toString() {
        return "User{" +
               "id=" + id +
               ", username='" + username + '\'' +
               ", email='" + email + '\'' +
               ", createdAt=" + createdAt +
               '}';
    }
} 
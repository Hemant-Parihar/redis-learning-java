package com.example.redis.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Product model class to demonstrate Redis and PostgreSQL operations.
 * This class is serializable so it can be stored in Redis.
 */
public class Product implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private Long id;
    private String name;
    private BigDecimal price;
    private Integer stock;
    private LocalDateTime createdAt;
    
    /**
     * Default constructor required for serialization
     */
    public Product() {
    }
    
    /**
     * Constructor with essential fields (for new product creation)
     */
    public Product(String name, BigDecimal price, Integer stock) {
        this.name = name;
        this.price = price;
        this.stock = stock;
        this.createdAt = LocalDateTime.now();
    }
    
    /**
     * Constructor with all fields (for existing product retrieval)
     */
    public Product(Long id, String name, BigDecimal price, Integer stock, LocalDateTime createdAt) {
        this.id = id;
        this.name = name;
        this.price = price;
        this.stock = stock;
        this.createdAt = createdAt;
    }
    
    // Getters and Setters
    
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public BigDecimal getPrice() {
        return price;
    }
    
    public void setPrice(BigDecimal price) {
        this.price = price;
    }
    
    public Integer getStock() {
        return stock;
    }
    
    public void setStock(Integer stock) {
        this.stock = stock;
    }
    
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    /**
     * Convert product object to a string representation suitable for Redis storage
     * Format: id|name|price|stock|createdAt
     */
    public String toRedisString() {
        return String.format("%d|%s|%s|%d|%s",
            id == null ? 0 : id,
            name,
            price.toString(),
            stock,
            createdAt == null ? LocalDateTime.now() : createdAt);
    }
    
    /**
     * Create a Product object from a Redis string representation
     */
    public static Product fromRedisString(String str) {
        if (str == null || str.isEmpty()) {
            return null;
        }
        
        String[] parts = str.split("\\|");
        if (parts.length < 5) {
            throw new IllegalArgumentException("Invalid Redis string format for Product");
        }
        
        Product product = new Product();
        product.setId(Long.parseLong(parts[0]));
        product.setName(parts[1]);
        product.setPrice(new BigDecimal(parts[2]));
        product.setStock(Integer.parseInt(parts[3]));
        product.setCreatedAt(LocalDateTime.parse(parts[4]));
        
        return product;
    }
    
    /**
     * Decrement the stock by the specified amount
     * Returns true if there was enough stock, false otherwise
     */
    public boolean decrementStock(int amount) {
        if (stock >= amount) {
            stock -= amount;
            return true;
        }
        return false;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Product product = (Product) o;
        return Objects.equals(id, product.id) &&
               Objects.equals(name, product.name);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }
    
    @Override
    public String toString() {
        return "Product{" +
               "id=" + id +
               ", name='" + name + '\'' +
               ", price=" + price +
               ", stock=" + stock +
               ", createdAt=" + createdAt +
               '}';
    }
} 
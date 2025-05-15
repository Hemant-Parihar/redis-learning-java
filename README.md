# Redis Learning Project (Java)

This project provides practical examples for learning Redis and comparing its performance with PostgreSQL. It demonstrates various Redis data structures, operations, and common use cases like caching, pub/sub, and more.

## Project Overview

The Redis Learning Project aims to help developers understand Redis through practical, hands-on examples. It covers:

1. Basic Redis operations (SET, GET, INCR, etc.)
2. Redis data types (Strings, Lists, Sets, Hashes, Sorted Sets, etc.)
3. CRUD operations using a DAO pattern
4. Performance comparison with PostgreSQL
5. Redis Pub/Sub functionality
6. Redis as a cache

The project uses Java 17 and includes Docker setup for both Redis and PostgreSQL.

## Prerequisites

- Java 17 or higher
- Docker and Docker Compose
- Maven

## Getting Started

### 1. Clone the repository

```bash
git clone <repository-url>
cd redis-learning-java
```

### 2. Start Redis and PostgreSQL using Docker Compose

```bash
docker-compose up -d
```

This will start:
- Redis server on port 6379
- Redis Commander (web UI) on port 8081
- PostgreSQL on port 5432
- pgAdmin (web UI) on port 8082

### 3. Build the project

```bash
mvn clean package
```

### 4. Run the application

```bash
java -jar target/redis-learning-java-1.0-SNAPSHOT.jar
```

## Accessing Admin UIs

- **Redis Commander**: http://localhost:8081
  - This provides a web interface to inspect Redis data

- **pgAdmin**: http://localhost:8082
  - Email: user@example.com
  - Password: pgadminpass
  - Add a new server with:
    - Host: postgres
    - Port: 5432
    - Username: postgres
    - Password: postgrespass
    - Database: rediscomparison

## Project Structure

- `src/main/java/com/example/redis/`
  - `config/`: Database connection configuration
  - `dao/`: Data Access Objects for Redis and PostgreSQL
  - `model/`: Model classes (User, Product)
  - `performance/`: Performance comparison tests
  - `RedisLearningApplication.java`: Main class with examples

## Examples Included

1. **Basic Redis Operations**: Demonstrates fundamental Redis commands
2. **Redis Data Types**: Shows how to use different Redis data structures
3. **CRUD Operations**: Showcases Create, Read, Update, Delete operations
4. **Performance Comparison**: Benchmarks Redis vs PostgreSQL for various operations
5. **Redis Pub/Sub**: Demonstrates publish/subscribe messaging
6. **Caching Example**: Shows how to use Redis as a cache

## Learning Path

1. Start with Basic Redis Operations to understand core commands
2. Explore Redis Data Types to learn Redis data structures
3. Study the CRUD Operations example to see practical usage patterns
4. Run the Performance Comparison to understand Redis performance benefits
5. Try the Redis Pub/Sub example to learn about messaging
6. Understand caching patterns with the Caching Example

## Performance Comparison

The project includes benchmark tests that compare:

- Single and batch read/write operations
- Atomic operations and transactions
- Counters and increments
- Sorted data retrieval
- Set operations

Each test outputs detailed timing information to help understand the performance characteristics of both Redis and PostgreSQL.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 
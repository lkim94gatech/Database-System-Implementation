# Flat-File Database Management System

## Project Overview

This project implements a **Flat-File Database Management System** for managing users, posts, and engagements in a social media-style application. The system allows for efficient data management using CSV files as the underlying storage. It supports multi-threaded operations, thread-safe updates, and CRUD functionalities.

The project showcases the use of modern C++ features, multi-threading, and file I/O for building a high-performance and extensible flat-file database.

---

## Features

### Core Functionalities
1. **Single-threaded and Multi-threaded Loading**:
   - Load users, posts, and engagements from CSV files into in-memory maps.
   - Multi-threaded implementation for improved performance.

2. **CRUD Operations**:
   - **Create**: Add new users, posts, and engagements.
   - **Read**: Fetch data with efficient lookups.
   - **Update**:
     - Increment post views with thread safety.
     - Update usernames across all related entities (users, posts, engagements).
   - **Delete**: Remove records dynamically from in-memory maps.

3. **Queries**:
   - Fetch all comments by a specific user, ordered by post ID and comment.
   - Get engagement statistics (likes, comments) for a specific location.

### Thread Safety
- Multi-threaded operations on CSV data with mutex locks.
- Ensures data integrity during concurrent updates.

### Performance Optimizations
- Multi-threaded loading reduces time for large datasets.
- Efficient in-memory data structures (`std::map` and `std::unique_ptr`) for fast lookups and minimal overhead.

---

## How to Run

### Prerequisites
- C++17 or later compiler.
- Standard C++ library support for multi-threading (`std::thread`, `std::mutex`).
- CSV files:
  - `users.csv`
  - `posts.csv`
  - `engagements.csv`

### Build and Run
1. **Compile the Code**:
   Use a C++17-compliant compiler to build the code. For example:
   ```bash
   g++ -std=c++17 -pthread -o flatfile main.cpp




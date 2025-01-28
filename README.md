# RAFT Consensus Algorithm

This repository contains an implementation of the **RAFT Consensus Algorithm** in Go. The RAFT algorithm is used to manage a replicated log and ensure consensus in distributed systems.

## Features
- **Leader Election**: Ensures that one node is selected as the leader in the distributed system.
- **Log Replication**: Synchronizes logs across nodes to ensure consistency.
- **Fault Tolerance**: Handles failures to maintain availability and reliability.
- **Persistence**: Maintains state to recover from crashes.

## File Overview
- `config.go`: Contains configuration settings for the RAFT implementation.
- `raft.go`: Implements the core RAFT algorithm, including leader election and log replication.
- `persister.go`: Handles data persistence to recover from system crashes.
- `util.go`: Contains utility functions used throughout the implementation.
- `test_test.go`: Includes tests for verifying the correctness of the RAFT algorithm.

## Getting Started

### Prerequisites
- **Go**: Ensure you have Go installed on your machine. You can download it [here](https://golang.org/dl/).

### Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/TheClothar/RAFT.git
   cd RAFT

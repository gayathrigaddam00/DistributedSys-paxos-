# Distributed Banking System (Paxos)

A fault-tolerant distributed banking application...

[![Spec](https://img.shields.io/badge/Project-Specification-red?logo=adobeacrobatreader&logoColor=white)](CSE535_F25_Project1.pdf)

---

A fault-tolerant distributed banking application built with **Java Spring Boot** and **gRPC**. This system implements the **Paxos Consensus Protocol** to ensure consistent transaction processing across a distributed cluster of nodes, handling node failures and concurrent client requests.

---

## 🚀 Features

* **Paxos Consensus Protocol:** Implements the complete Paxos algorithm (Leader Election, Prepare/Promise, Accept/Accepted, Commit) to ensure total ordering of transactions.
* **Stable-Leader (Multi-Paxos):** Optimizes performance by electing a single stable leader to manage multiple transaction instances (sequences), reducing network overhead.
* **gRPC Communication:** Uses Protocol Buffers (Protobuf) for efficient, strongly-typed node-to-node and client-to-node communication.
* **Fault Tolerance:** Operates correctly with up to **$f$** concurrent node failures in a cluster of **$2f+1$** nodes.
* **Interactive Test Runner:** Includes a CLI tool (`TestRunner`) to execute batch test cases from CSV files and inspect system state.
* **State Machine Replication:** Maintains a consistent Key-Value store (Client Balances) across all replicas.

---

## 🛠️ System Architecture

The system consists of **5 Replica Nodes** and a **Test Client**, simulating a realistic distributed environment.

### 1. The Protocol Flow
The system implements **Stable-Leader Paxos**:
* **Leader Election:** If a leader fails, a new one is elected using the standard Paxos Prepare/Promise phase.
* **Normal Operation:** The leader receives client requests, sequences them, and replicates them via `Accept` messages. Once a majority ($f+1$) acknowledges, the transaction is `Committed`.
* **Catch-Up:** Nodes that lag behind or recover from failure automatically sync with the leader.

### 2. Banking Application Logic
* **Transactions:** Transfer requests in the format `(Sender, Receiver, Amount)`.
* **Validation:** Checks sufficient funds before execution.
* **Datastore:** In-memory map of client balances, updated only upon consensus.

---

## 💻 Tech Stack

* **Language:** Java 21
* **Framework:** Spring Boot 3.3.3
* **Communication:** gRPC (Google Remote Procedure Call)
* **Serialization:** Protocol Buffers (Protobuf 3.25.3)
* **CLI Tools:** Picocli, OpenCSV
* **Build Tool:** Maven

---

## 🚀 Getting Started

### Prerequisites
* **Java 21+** installed.
* **Maven** installed (or use the provided `mvnw` wrapper).

### Installation

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/gayathrigaddam00/DistributedSys-paxos.git](https://github.com/gayathrigaddam00/DistributedSys-paxos.git)
    cd DistributedSys-paxos
    ```

2.  **Build the project:**
    This will compile the Java code and generate the gRPC/Protobuf sources.
    ```bash
    ./mvnw clean install
    ```

---

## 🏃‍♂️ Usage

To run the full distributed system, you need to start the 5 server nodes first, and then run the test client.

### 1. Start the Replica Nodes
You need to start 5 instances of the application, each representing a node (n1, n2, n3, n4, n5). Open 5 separate terminal windows:

**Terminal 1 (Node 1):**
```bash
java -jar target/grpc-demo-0.0.1-SNAPSHOT.jar --spring.profiles.active=n1

```

**Terminal 2 (Node 2):**

```bash
java -jar target/grpc-demo-0.0.1-SNAPSHOT.jar --spring.profiles.active=n2

```

*(Repeat for n3, n4, n5 using their respective profiles)*

### 2. Run the Test Runner

Once all nodes are running, open a new terminal to run the interactive test runner. You must provide a CSV test file.

```bash
java -cp target/grpc-demo-0.0.1-SNAPSHOT.jar:target/lib/* com.example.paxos.TestRunner -f path/to/test_cases.csv

```

---

## 🧪 Testing & Commands

The `TestRunner` provides an interactive shell to control the system.

### Test File Format (CSV)

The system executes transactions in "Sets".

```csv
1, (A, B, 10), [n1, n2, n3, n4, n5]  # Set 1: A sends 10 to B (All nodes active)
2, (B, C, 5),  [n1, n3, n5]          # Set 2: n2 and n4 are simulated as failed

```

### Interactive Commands

Inside the Test Runner, you can use:

| Command | Description |
| --- | --- |
| `next` | Process the next set of transactions from the CSV file. |
| `printdb` | Print the current account balances of all clients across all nodes. |
| `printlog <nodeId>` | Print the message log for a specific node (e.g., `printlog n1`). |
| `printstatus <seq>` | Show the status (Accepted/Committed/Executed) of a transaction sequence. |
| `printview` | Display the history of View Changes (Leader Elections). |
| `exit` | Quit the application. |

---

## 📜 Project Structure

```bash
├── src/main/proto/       # Protobuf definitions (paxos.proto)
├── src/main/java/
│   ├── com/example/paxos/core/      # Core Paxos Logic (PaxosCore.java)
│   ├── com/example/paxos/transport/ # gRPC Service Implementations
│   └── com/example/paxos/TestRunner.java # CLI Test Runner
├── src/main/resources/   # Config files (application-n1.yaml, etc.)
└── pom.xml               # Maven dependencies

```

---

## 🔗 References

* **Paxos Made Simple** - Leslie Lamport
* **gRPC Documentation:** https://grpc.io/

```

```

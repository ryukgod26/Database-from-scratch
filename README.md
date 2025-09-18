# Database-from-scratch

This project is a simple, persistent, single-file database implemented from scratch in C, inspired by SQLite. It features a command-line interface (REPL) and uses a B-Tree data structure for efficient data storage and retrieval.

## Features

*   **Persistent Storage**: The database is saved to a single file.
*   **B-Tree Implementation**: Data is organized in a B-Tree to allow for efficient insertions and lookups, even as the database grows.
*   **REPL Interface**: Interact with the database through a simple command-line shell.
*   **Basic SQL Commands**:
    *   `insert <id> <username> <email>`: Inserts a new record.
    *   `select`: Retrieves and displays all records.
*   **Meta-Commands**:
    *   `.exit`: Exits the database shell, saving all changes to the file.
    *   `.constants`: Displays the size of various data structures.
    *   `.visualize`: Prints a visual representation of the B-Tree structure.

## How to Build and Run

### Prerequisites

*   A C compiler, such as GCC.

### Compilation

Navigate to the project directory and compile the source files using the following command:

```bash
gcc main.c -o db_executable
```

This will create an executable file named `db_executable`.

### Running the Database

To start the database shell, run the compiled executable and provide a name for your database file. If the file does not exist, it will be created.

```bash
./db_executable my_database.db
```

You will then see the `db >` prompt, where you can enter commands.

## Usage Example

Here is a sample session demonstrating the basic functionality:

```
$ ./db_executable test.db
db > insert 1 john john@example.com
db > insert 2 jane jane@example.com
db > insert 3 cstack cstack@example.com
db > select
id:1 ,Username:john, Email:john@example.com 
id:2 ,Username:jane, Email:jane@example.com 
id:3 ,Username:cstack, Email:cstack@example.com 
db > .visualize
Tree: 
- Internal (size 1)
  - leaf (size 2)
   - 1
   - 2
  - key 2
  - leaf (size 1)
   - 3
db > .exit
Database Closed Successfully.
```

## Project Structure

*   `main.c`: Contains the `main` function, the REPL (Read-Eval-Print Loop), and handles user input parsing.
*   `inputBuffer.h`: The core of the database. It defines all data structures (`Row`, `Pager`, `Table`, `Cursor`), constants for node layouts, and implements all the logic for:
    *   Serialization/Deserialization of rows.
    *   Page management and file I/O (`Pager`).
    *   B-Tree operations (insertion, searching, node splitting).
    *   Statement preparation and execution.
*   `README.md`: This documentation file.

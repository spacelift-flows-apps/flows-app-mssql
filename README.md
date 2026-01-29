# Microsoft SQL Server

## Description

App for interacting with Microsoft SQL Server databases. Supports queries, commands, bulk operations, streaming large result sets, and schema introspection.

## Configuration

The app requires SQL Server connection details:

- `server` - SQL Server hostname or IP address (required)
- `port` - Server port (default: 1433)
- `database` - Database name (default: master)
- `username` - SQL Server username (required)
- `password` - SQL Server password (optional)
- `encrypt` - Enable TLS encryption (default: true)
- `trustServerCertificate` - Trust the server certificate without validation, useful for self-signed certs (default: false)
- `caCertificate` - PEM-encoded CA certificate for verifying the server certificate, e.g. AWS RDS CA bundle (optional)
- `connectionTimeout` - Connection timeout in seconds (default: 15)
- `requestTimeout` - Request timeout in milliseconds (default: 30000, optional)

## Blocks

- `executeQuery`
  - Executes SELECT queries and returns results as an array of row objects. Supports named `@parameter` placeholders.

- `executeCommand`
  - Executes INSERT, UPDATE, DELETE, or DDL commands. Returns number of rows affected. For OUTPUT clauses, use executeQuery instead.

- `bulkInsert`
  - Inserts multiple rows in a single parameterized INSERT statement.

- `streamQuery`
  - Executes a query and emits result batches in separate events. Useful for larger datasets.

- `getTableInfo`
  - Retrieves schema information including columns, constraints, and indexes.

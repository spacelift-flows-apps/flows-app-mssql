import { defineApp } from "@slflows/sdk/v1";
import { blocks } from "./blocks/index";
import * as sql from "mssql";

export const app = defineApp({
  name: "Microsoft SQL Server",
  installationInstructions:
    "Connect your Microsoft SQL Server database to Spacelift Flows.\n\nTo install:\n1. Provide your SQL Server connection details\n2. Click 'Confirm' to test the connection\n3. Start using the SQL Server blocks in your flows",

  blocks,

  config: {
    server: {
      name: "Server",
      description: "SQL Server hostname or IP address",
      type: "string",
      required: true,
    },
    port: {
      name: "Port",
      description: "SQL Server port",
      type: "number",
      required: true,
      default: 1433,
    },
    database: {
      name: "Database",
      description: "Name of the database to connect to",
      type: "string",
      required: true,
      default: "master",
    },
    username: {
      name: "Username",
      description: "SQL Server username for authentication",
      type: "string",
      required: true,
    },
    password: {
      name: "Password",
      description: "SQL Server password for authentication",
      type: "string",
      required: false,
      sensitive: true,
    },
    encrypt: {
      name: "Encrypt Connection",
      description: "Enable TLS encryption for connections",
      type: "boolean",
      required: true,
      default: true,
    },
    trustServerCertificate: {
      name: "Trust Server Certificate",
      description:
        "Trust the server certificate without validation (for self-signed certificates)",
      type: "boolean",
      required: true,
      default: false,
    },
    caCertificate: {
      name: "CA Certificate",
      description:
        "PEM-encoded CA certificate for verifying the server certificate (e.g., AWS RDS CA bundle)",
      type: "string",
      required: false,
      sensitive: true,
    },
    connectionTimeout: {
      name: "Connection Timeout",
      description: "Connection timeout in seconds",
      type: "number",
      required: true,
      default: 15,
    },
    requestTimeout: {
      name: "Request Timeout",
      description: "Request timeout in milliseconds (0 for no timeout)",
      type: "number",
      required: false,
      default: 30000,
    },
  },

  async onSync(input) {
    const config = input.app.config;

    const poolConfig: sql.config = {
      server: config.server as string,
      port: config.port as number,
      database: config.database as string,
      user: config.username as string,
      password: config.password as string | undefined,
      connectionTimeout: (config.connectionTimeout as number) * 1000,
      requestTimeout: config.requestTimeout as number,
      pool: { max: 1 },
      options: {
        encrypt: config.encrypt as boolean,
        trustServerCertificate: config.trustServerCertificate as boolean,
        cryptoCredentialsDetails: config.caCertificate
          ? { ca: config.caCertificate as string }
          : undefined,
      },
    };

    let pool: sql.ConnectionPool | null = null;
    try {
      pool = new sql.ConnectionPool(poolConfig);
      await pool.connect();

      // Test basic connectivity
      await pool.request().query("SELECT 1");

      // Check database access permission
      const permCheck = await pool
        .request()
        .input("dbName", sql.NVarChar, config.database)
        .query(
          `SELECT HAS_PERMS_BY_NAME(@dbName, 'DATABASE', 'CONNECT') AS can_connect`,
        );

      if (!permCheck.recordset[0].can_connect) {
        await pool.close();
        return {
          newStatus: "failed" as const,
          customStatusDescription: "Insufficient database permissions",
        };
      }

      await pool.close();

      return {
        newStatus: "ready" as const,
      };
    } catch (error: any) {
      if (pool) {
        await pool.close().catch(() => {});
      }

      console.error("SQL Server connection test failed:", error.message);

      let statusDescription = "Connection failed";
      if (error.code === "ENOTFOUND" || error.code === "ECONNREFUSED") {
        statusDescription = "Cannot reach database server";
      } else if (error.code === "ESOCKET") {
        statusDescription = "Network error connecting to server";
      } else if (error.number === 18456) {
        statusDescription = "Authentication failed";
      } else if (error.number === 4060) {
        statusDescription = "Database does not exist";
      }

      return {
        newStatus: "failed" as const,
        customStatusDescription: statusDescription,
      };
    }
  },
});

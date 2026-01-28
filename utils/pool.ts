import * as sql from "mssql";
import crypto from "crypto";

// Global pool instance
let globalPool: sql.ConnectionPool | null = null;
let currentConfigHash: string | null = null;
let poolInitializationPromise: Promise<sql.ConnectionPool> | null = null;

/**
 * Creates a hash of the configuration to detect changes
 */
function getConfigHash(appConfig: any): string {
  const configForHash = {
    server: appConfig.server,
    port: appConfig.port,
    database: appConfig.database,
    username: appConfig.username,
    password: appConfig.password,
    encrypt: appConfig.encrypt,
    trustServerCertificate: appConfig.trustServerCertificate,
    caCertificate: appConfig.caCertificate,
    connectionTimeout: appConfig.connectionTimeout,
    requestTimeout: appConfig.requestTimeout,
  };
  return crypto
    .createHash("sha256")
    .update(JSON.stringify(configForHash))
    .digest("hex");
}

/**
 * Creates pool configuration from app config
 */
function createPoolConfig(appConfig: any): sql.config {
  return {
    server: appConfig.server as string,
    port: appConfig.port as number,
    database: appConfig.database as string,
    user: appConfig.username as string,
    password: appConfig.password as string | undefined,
    connectionTimeout: (appConfig.connectionTimeout as number) * 1000,
    requestTimeout: appConfig.requestTimeout as number,
    pool: {
      max: 10,
      min: 0,
      idleTimeoutMillis: 30000,
    },
    options: {
      encrypt: appConfig.encrypt as boolean,
      trustServerCertificate: appConfig.trustServerCertificate as boolean,
      cryptoCredentialsDetails: appConfig.caCertificate
        ? { ca: appConfig.caCertificate as string }
        : undefined,
    },
  };
}

/**
 * Gets or creates the global pool instance.
 * This ensures all blocks share the same pool and handles config changes.
 */
export async function getPool(appConfig: any): Promise<sql.ConnectionPool> {
  const configHash = getConfigHash(appConfig);

  // If config hasn't changed and we have a pool, return it
  if (globalPool && currentConfigHash === configHash && globalPool.connected) {
    return globalPool;
  }

  // If initialization is already in progress, wait for it
  if (poolInitializationPromise && currentConfigHash === configHash) {
    return poolInitializationPromise;
  }

  // Start initialization (this prevents multiple simultaneous initializations)
  poolInitializationPromise = (async () => {
    try {
      // Close the old pool if config changed
      if (globalPool && currentConfigHash !== configHash) {
        console.log("SQL Server config changed, recreating pool");
        try {
          await globalPool.close();
        } catch (error) {
          console.error("Error closing old pool:", error);
        }
        globalPool = null;
      }

      // Create new pool
      const poolConfig = createPoolConfig(appConfig);
      const newPool = new sql.ConnectionPool(poolConfig);

      // Set up error handlers
      newPool.on("error", (err) => {
        console.error("Unexpected SQL Server pool error:", err);
      });

      // Connect the pool
      await newPool.connect();

      // Store the new pool and config hash
      globalPool = newPool;
      currentConfigHash = configHash;

      return newPool;
    } finally {
      // Clear the initialization promise when done
      poolInitializationPromise = null;
    }
  })();

  return poolInitializationPromise;
}

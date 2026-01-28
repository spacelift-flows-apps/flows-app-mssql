import { AppBlock, events } from "@slflows/sdk/v1";
import { getPool } from "../utils/pool.ts";

/**
 * Sanitizes a SQL identifier (table or schema name) to prevent SQL injection.
 * Removes any existing brackets and wraps in square brackets.
 */
function sanitizeIdentifier(name: string): string {
  // Remove existing brackets and escape any remaining brackets within the name
  const cleaned = name.replace(/[\[\]]/g, "");
  return `[${cleaned}]`;
}

/**
 * Parses and sanitizes a table name that may include schema (e.g., 'dbo.users' or 'users')
 */
function sanitizeTableName(tableName: string): string {
  const parts = tableName.split(".");
  return parts.map(sanitizeIdentifier).join(".");
}

export const bulkInsert: AppBlock = {
  name: "Bulk Insert",
  description: "Efficiently inserts multiple rows using a parameterized query",
  category: "Bulk Operations",

  inputs: {
    default: {
      config: {
        table: {
          name: "Table Name",
          description:
            "Target table name (optionally with schema, e.g., 'dbo.users')",
          type: "string",
          required: true,
        },
        columns: {
          name: "Column Names",
          description: "Array of column names to insert into",
          type: {
            type: "array",
            items: {
              type: "string",
            },
          },
          required: true,
        },
        rows: {
          name: "Rows Data",
          description:
            "Array of row data arrays (each inner array must match columns order)",
          type: {
            type: "array",
            items: {
              type: "array",
              items: {},
            },
          },
          required: true,
        },
      },
      async onEvent(input) {
        const { table, columns, rows } = input.event.inputConfig;
        const pool = await getPool(input.app.config);

        const rowsData = rows as any[][];

        if (rowsData.length === 0) {
          await events.emit({
            rowCount: 0,
            table: table as string,
          });
          return;
        }

        const request = pool.request();

        // Build the INSERT query with multiple value sets using parameterized queries
        // Use square brackets for column quoting (MSSQL syntax)
        const columnsList = (columns as string[])
          .map((col) => `[${col}]`)
          .join(", ");

        // Create placeholder sets for each row
        const valuePlaceholders: string[] = [];
        let paramIndex = 1;

        for (const row of rowsData) {
          const rowPlaceholders: string[] = [];
          for (const value of row) {
            rowPlaceholders.push(`@p${paramIndex}`);
            // Handle BigInt values
            if (typeof value === "bigint") {
              request.input(`p${paramIndex}`, value.toString());
            } else {
              request.input(`p${paramIndex}`, value);
            }
            paramIndex++;
          }
          valuePlaceholders.push(`(${rowPlaceholders.join(", ")})`);
        }

        const safeTableName = sanitizeTableName(table as string);
        const insertQuery = `INSERT INTO ${safeTableName} (${columnsList}) VALUES ${valuePlaceholders.join(", ")}`;

        const result = await request.query(insertQuery);

        // Sum all affected rows
        const totalRowsAffected = result.rowsAffected.reduce(
          (sum, count) => sum + count,
          0,
        );

        await events.emit({
          rowCount: totalRowsAffected || rowsData.length,
          table: table as string,
        });
      },
    },
  },

  outputs: {
    default: {
      name: "Insert Result",
      description: "The result of the bulk insert operation",
      default: true,
      possiblePrimaryParents: ["default"],
      type: {
        type: "object",
        properties: {
          rowCount: {
            type: "number",
            description: "Number of rows inserted",
          },
          table: {
            type: "string",
            description: "The table name where rows were inserted",
          },
        },
        required: ["rowCount", "table"],
      },
    },
  },
};

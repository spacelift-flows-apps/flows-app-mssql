import { AppBlock, events } from "@slflows/sdk/v1";
import * as sql from "mssql";
import { getPool } from "../utils/pool.ts";

async function* streamRows(
  request: sql.Request,
  query: string,
): AsyncGenerator<any> {
  request.stream = true;

  let resolver: ((value: IteratorResult<any>) => void) | null = null;
  let rejecter: ((error: Error) => void) | null = null;
  const rowQueue: any[] = [];
  let done = false;
  let error: Error | null = null;

  request.on("row", (row: any) => {
    if (resolver) {
      resolver({ value: row, done: false });
      resolver = null;
    } else {
      rowQueue.push(row);
    }
  });

  request.on("error", (err: Error) => {
    error = err;
    if (rejecter) {
      rejecter(err);
      rejecter = null;
    }
  });

  request.on("done", () => {
    done = true;
    if (resolver) {
      resolver({ value: undefined, done: true });
      resolver = null;
    }
  });

  request.query(query);

  while (true) {
    if (error) throw error;

    if (rowQueue.length > 0) {
      yield rowQueue.shift();
    } else if (done) {
      return;
    } else {
      yield await new Promise<any>((resolve, reject) => {
        resolver = (result) => {
          if (result.done) {
            resolve(undefined);
          } else {
            resolve(result.value);
          }
        };
        rejecter = reject;

        // Check again in case events fired while setting up
        if (error) {
          reject(error);
        } else if (rowQueue.length > 0) {
          resolve(rowQueue.shift());
          resolver = null;
        } else if (done) {
          resolve(undefined);
          resolver = null;
        }
      });

      // Check if we got the "done" signal
      if (done && rowQueue.length === 0) {
        return;
      }
    }
  }
}

export const streamQuery: AppBlock = {
  name: "Stream Query",
  description:
    "Executes a query and streams results in batches as separate events for large datasets.",
  category: "Bulk Operations",

  inputs: {
    default: {
      config: {
        query: {
          name: "SQL Query",
          description:
            "SQL query to execute and stream results, with optional @parameter placeholders",
          type: "string",
          required: true,
        },
        parameters: {
          name: "Parameters",
          description:
            "Map of parameter names to values (e.g. { userId: 123, name: 'John' } for @userId, @name)",
          type: {
            type: "object",
            additionalProperties: true,
          },
          required: false,
        },
        batchSize: {
          name: "Batch Size",
          description: "Number of rows per batch event",
          type: "number",
          required: false,
        },
      },
      async onEvent(input) {
        const {
          query,
          parameters,
          batchSize: configBatchSize,
        } = input.event.inputConfig;
        const batchSize = (configBatchSize as number) || 100;

        const pool = await getPool(input.app.config);
        const request = pool.request();

        // Add user-defined parameters
        const params = (parameters as Record<string, any>) || {};
        for (const [name, value] of Object.entries(params)) {
          request.input(name, value);
        }

        let batchNumber = 0;
        let currentBatch: any[] = [];

        for await (const row of streamRows(request, query as string)) {
          if (row === undefined) break;

          // Handle BigInt serialization
          const serializedRow: any = {};
          for (const [key, value] of Object.entries(row)) {
            if (typeof value === "bigint") {
              serializedRow[key] = value.toString();
            } else {
              serializedRow[key] = value;
            }
          }

          currentBatch.push(serializedRow);

          if (currentBatch.length >= batchSize) {
            await events.emit({
              batchNumber,
              rows: currentBatch,
              rowCount: currentBatch.length,
              hasMore: true,
            });
            batchNumber++;
            currentBatch = [];
          }
        }

        // Emit any remaining rows
        if (currentBatch.length > 0) {
          await events.emit({
            batchNumber,
            rows: currentBatch,
            rowCount: currentBatch.length,
            hasMore: false,
          });
        }
      },
    },
  },

  outputs: {
    default: {
      name: "Batch",
      description: "Emitted for each batch of rows",
      default: true,
      possiblePrimaryParents: ["default"],
      type: {
        type: "object",
        properties: {
          batchNumber: {
            type: "number",
            description: "Sequential batch number starting from 0",
          },
          rows: {
            type: "array",
            description: "Array of rows in this batch",
            items: {
              type: "object",
            },
          },
          rowCount: {
            type: "number",
            description: "Number of rows in this batch",
          },
          hasMore: {
            type: "boolean",
            description: "Whether more batches are expected",
          },
        },
        required: ["batchNumber", "rows", "rowCount", "hasMore"],
      },
    },
  },
};

import { AppBlock, events } from "@slflows/sdk/v1";
import { getPool } from "../utils/pool.ts";

export const executeQuery: AppBlock = {
  name: "Execute Query",
  description: "Executes a SELECT query and returns the results",
  category: "Basic",

  inputs: {
    default: {
      config: {
        query: {
          name: "SQL Query",
          description:
            "SQL SELECT query with optional @parameter placeholders",
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
      },
      async onEvent(input) {
        const { query, parameters } = input.event.inputConfig;
        const pool = await getPool(input.app.config);

        const request = pool.request();

        // Add user-defined parameters
        const params = (parameters as Record<string, any>) || {};
        for (const [name, value] of Object.entries(params)) {
          request.input(name, value);
        }

        const result = await request.query(query as string);

        // Handle BigInt serialization
        const rows = result.recordset.map((row: any) => {
          const serializedRow: any = {};
          for (const [key, value] of Object.entries(row)) {
            if (typeof value === "bigint") {
              serializedRow[key] = value.toString();
            } else {
              serializedRow[key] = value;
            }
          }
          return serializedRow;
        });

        await events.emit({
          rows,
        });
      },
    },
  },

  outputs: {
    default: {
      name: "Query Result",
      description: "The result of the SELECT query",
      default: true,
      possiblePrimaryParents: ["default"],
      type: {
        type: "object",
        properties: {
          rows: {
            type: "array",
            description: "Array of result rows",
            items: {
              type: "object",
            },
          },
        },
        required: ["rows"],
      },
    },
  },
};

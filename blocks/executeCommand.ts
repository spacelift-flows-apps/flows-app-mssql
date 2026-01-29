import { AppBlock, events } from "@slflows/sdk/v1";
import { getPool } from "../utils/pool.ts";

export const executeCommand: AppBlock = {
  name: "Execute Command",
  description:
    "Executes INSERT, UPDATE, DELETE, or DDL commands (use Execute Query for OUTPUT clauses)",
  category: "Basic",

  inputs: {
    default: {
      config: {
        command: {
          name: "SQL Command",
          description:
            "SQL command to execute with optional @parameter placeholders",
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
        const { command, parameters } = input.event.inputConfig;
        const pool = await getPool(input.app.config);

        const request = pool.request();

        // Add user-defined parameters
        const params = (parameters as Record<string, any>) || {};
        for (const [name, value] of Object.entries(params)) {
          request.input(name, value);
        }

        const result = await request.query(command as string);

        // rowsAffected is an array - sum all affected rows
        const totalRowsAffected = result.rowsAffected.reduce(
          (sum, count) => sum + count,
          0,
        );

        await events.emit({
          rowsAffected: totalRowsAffected,
        });
      },
    },
  },

  outputs: {
    default: {
      name: "Command Result",
      description: "The result of the SQL command execution",
      default: true,
      possiblePrimaryParents: ["default"],
      type: {
        type: "object",
        properties: {
          rowsAffected: {
            type: "number",
            description: "Number of rows affected by the command",
          },
        },
        required: ["rowsAffected"],
      },
    },
  },
};

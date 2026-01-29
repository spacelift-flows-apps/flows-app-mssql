import { AppBlock, events } from "@slflows/sdk/v1";
import { getPool } from "../utils/pool.ts";

export const getTableInfo: AppBlock = {
  name: "Get Table Info",
  description: "Retrieves schema information about tables and columns",
  category: "Utility",

  inputs: {
    default: {
      config: {
        schema: {
          name: "Schema Name",
          description: "Database schema name",
          type: "string",
          required: false,
        },
        table: {
          name: "Table Name",
          description: "Table name to get information about",
          type: "string",
          required: true,
        },
      },
      async onEvent(input) {
        const { schema: schemaName, table } = input.event.inputConfig;
        const actualSchema = schemaName || "dbo";

        const pool = await getPool(input.app.config);

        // Get table information
        const tableQuery = `
          SELECT
            s.name AS table_schema,
            t.name AS table_name,
            CASE WHEN t.type = 'U' THEN 'BASE TABLE' WHEN t.type = 'V' THEN 'VIEW' END AS table_type,
            CAST(ep.value AS NVARCHAR(MAX)) AS table_comment
          FROM sys.tables t
          INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
          LEFT JOIN sys.extended_properties ep
            ON ep.major_id = t.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
          WHERE s.name = @p1 AND t.name = @p2
        `;

        const tableRequest = pool.request();
        tableRequest.input("p1", actualSchema);
        tableRequest.input("p2", table);
        const tableResult = await tableRequest.query(tableQuery);

        if (tableResult.recordset.length === 0) {
          throw new Error(`Table ${actualSchema}.${table} not found`);
        }

        const tableInfo = tableResult.recordset[0];

        // Get column information
        const columnsQuery = `
          SELECT
            c.name AS column_name,
            TYPE_NAME(c.user_type_id) AS data_type,
            CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
            dc.definition AS column_default,
            c.max_length AS character_maximum_length,
            c.precision AS numeric_precision,
            c.scale AS numeric_scale,
            CAST(ep.value AS NVARCHAR(MAX)) AS column_comment
          FROM sys.columns c
          INNER JOIN sys.tables t ON c.object_id = t.object_id
          INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
          LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
          LEFT JOIN sys.extended_properties ep
            ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description'
          WHERE s.name = @p1 AND t.name = @p2
          ORDER BY c.column_id
        `;

        const columnsRequest = pool.request();
        columnsRequest.input("p1", actualSchema);
        columnsRequest.input("p2", table);
        const columnsResult = await columnsRequest.query(columnsQuery);

        // Get constraints (primary keys, unique constraints)
        const constraintsQuery = `
          SELECT
            kc.name AS constraint_name,
            kc.type_desc AS constraint_type,
            STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns,
            NULL AS foreign_table_schema,
            NULL AS foreign_table_name,
            NULL AS foreign_columns
          FROM sys.key_constraints kc
          INNER JOIN sys.tables t ON kc.parent_object_id = t.object_id
          INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
          INNER JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
          INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
          WHERE s.name = @p1 AND t.name = @p2
          GROUP BY kc.name, kc.type_desc

          UNION ALL

          SELECT
            fk.name AS constraint_name,
            'FOREIGN_KEY' AS constraint_type,
            STRING_AGG(pc.name, ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS columns,
            rs.name AS foreign_table_schema,
            rt.name AS foreign_table_name,
            STRING_AGG(rc.name, ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS foreign_columns
          FROM sys.foreign_keys fk
          INNER JOIN sys.tables pt ON fk.parent_object_id = pt.object_id
          INNER JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
          INNER JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
          INNER JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
          INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
          INNER JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
          INNER JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
          WHERE ps.name = @p1 AND pt.name = @p2
          GROUP BY fk.name, rs.name, rt.name
        `;

        const constraintsRequest = pool.request();
        constraintsRequest.input("p1", actualSchema);
        constraintsRequest.input("p2", table);
        const constraintsResult =
          await constraintsRequest.query(constraintsQuery);

        // Get indexes
        const indexesQuery = `
          SELECT
            i.name AS index_name,
            i.is_unique,
            i.is_primary_key AS is_primary,
            STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
          FROM sys.indexes i
          INNER JOIN sys.tables t ON i.object_id = t.object_id
          INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
          INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
          INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
          WHERE s.name = @p1 AND t.name = @p2 AND i.name IS NOT NULL
          GROUP BY i.name, i.is_unique, i.is_primary_key
        `;

        const indexesRequest = pool.request();
        indexesRequest.input("p1", actualSchema);
        indexesRequest.input("p2", table);
        const indexesResult = await indexesRequest.query(indexesQuery);

        await events.emit({
          schema: tableInfo.table_schema,
          tableName: tableInfo.table_name,
          tableType: tableInfo.table_type,
          tableComment: tableInfo.table_comment,
          columns: columnsResult.recordset.map((col: any) => ({
            name: col.column_name,
            dataType: col.data_type,
            nullable: col.is_nullable === "YES",
            defaultValue: col.column_default,
            maxLength: col.character_maximum_length,
            numericPrecision: col.numeric_precision,
            numericScale: col.numeric_scale,
            comment: col.column_comment,
          })),
          constraints: constraintsResult.recordset.map((con: any) => ({
            name: con.constraint_name,
            type: con.constraint_type,
            columns: con.columns,
            foreignTableSchema: con.foreign_table_schema,
            foreignTableName: con.foreign_table_name,
            foreignColumns: con.foreign_columns,
          })),
          indexes: indexesResult.recordset.map((idx: any) => ({
            name: idx.index_name,
            isUnique: idx.is_unique,
            isPrimary: idx.is_primary,
            columns: idx.columns,
          })),
        });
      },
    },
  },

  outputs: {
    default: {
      name: "Table Information",
      description: "Complete schema information about the table",
      default: true,
      possiblePrimaryParents: ["default"],
      type: {
        type: "object",
        properties: {
          schema: {
            type: "string",
            description: "Schema name",
          },
          tableName: {
            type: "string",
            description: "Table name",
          },
          tableType: {
            type: "string",
            description: "Table type (BASE TABLE, VIEW, etc.)",
          },
          tableComment: {
            type: "string",
            description: "Table comment/description",
          },
          columns: {
            type: "array",
            description: "Array of column definitions",
            items: {
              type: "object",
              properties: {
                name: {
                  type: "string",
                  description: "Column name",
                },
                dataType: {
                  type: "string",
                  description: "SQL Server data type",
                },
                nullable: {
                  type: "boolean",
                  description: "Whether the column allows NULL values",
                },
                defaultValue: {
                  type: "string",
                  description: "Default value expression",
                },
                maxLength: {
                  type: "number",
                  description: "Maximum length (for string/binary types)",
                },
                numericPrecision: {
                  type: "number",
                  description: "Numeric precision (for numeric types)",
                },
                numericScale: {
                  type: "number",
                  description: "Numeric scale (for numeric types)",
                },
                comment: {
                  type: "string",
                  description: "Column comment/description",
                },
              },
            },
          },
          constraints: {
            type: "array",
            description: "Array of table constraints",
            items: {
              type: "object",
              properties: {
                name: {
                  type: "string",
                  description: "Constraint name",
                },
                type: {
                  type: "string",
                  description:
                    "Constraint type (PRIMARY_KEY, FOREIGN_KEY, UNIQUE_CONSTRAINT)",
                },
                columns: {
                  type: "string",
                  description: "Columns involved in the constraint",
                },
                foreignTableSchema: {
                  type: "string",
                  description: "Referenced table schema (for foreign keys)",
                },
                foreignTableName: {
                  type: "string",
                  description: "Referenced table name (for foreign keys)",
                },
                foreignColumns: {
                  type: "string",
                  description: "Referenced columns (for foreign keys)",
                },
              },
            },
          },
          indexes: {
            type: "array",
            description: "Array of table indexes",
            items: {
              type: "object",
              properties: {
                name: {
                  type: "string",
                  description: "Index name",
                },
                isUnique: {
                  type: "boolean",
                  description: "Whether the index enforces uniqueness",
                },
                isPrimary: {
                  type: "boolean",
                  description: "Whether this is the primary key index",
                },
                columns: {
                  type: "string",
                  description: "Columns included in the index",
                },
              },
            },
          },
        },
        required: [
          "schema",
          "tableName",
          "tableType",
          "columns",
          "constraints",
          "indexes",
        ],
      },
    },
  },
};

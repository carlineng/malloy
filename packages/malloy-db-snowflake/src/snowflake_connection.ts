/*
 * Copyright 2022 Google LLC
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 */

// LTNOTE: we need this extension to be installed to correctly index
//  postgres data...  We should probably do this on connection creation...
//
//     create extension if not exists tsm_system_rows
//

import * as crypto from "crypto";
import {
  StructDef,
  MalloyQueryData,
  NamedStructDefs,
  AtomicFieldTypeInner,
  QueryData,
  PooledConnection,
  parseTableURI,
  SQLBlock,
  Connection,
  QueryDataRow,
  FetchSchemaAndRunSimultaneously,
  FetchSchemaAndRunStreamSimultaneously,
  PersistSQLResults,
  StreamingConnection,
} from "@malloydata/malloy";
import {
  createConnection,
  Statement,
  SnowflakeError,
  Connection as SnowflakeSDKConnection,
} from "snowflake-sdk";

const snowflakeToMalloyTypes: { [key: string]: AtomicFieldTypeInner } = {
  BOOLEAN: "boolean",
  DATE: "date",
  FLOAT: "number",
  NUMBER: "number",
  TEXT: "string",
  VARCHAR: "string",
  TIMESTAMP_LTZ: "timestamp",
  TIMESTAMP_NTZ: "timestamp",
  TIMESTAMP_TZ: "timestamp",
  TIMESTAMP: "timestamp",
};

interface SnowflakeQueryOptions {
  rowLimit?: number;
}

type SnowflakeQueryOptionsReader =
  | SnowflakeQueryOptions
  | (() => SnowflakeQueryOptions)
  | (() => Promise<SnowflakeQueryOptions>);

interface SnowflakeConnectionConfiguration {
  account?: string;
  username?: string;
  password?: string;
  database?: string;
  schema?: string;
  warehouse?: string;
  role?: string;
}

export class SnowflakeAuthenticationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SnowflakeAuthenticationError";
  }
}

type SnowflakeConnectionConfigurationReader =
  | SnowflakeConnectionConfiguration
  | (() => Promise<SnowflakeConnectionConfiguration>);

export class SnowflakeConnection
  implements Connection, StreamingConnection, PersistSQLResults
{
  private schemaCache = new Map<
    string,
    | { schema: StructDef; error?: undefined }
    | { error: string; schema?: undefined }
  >();
  private sqlSchemaCache = new Map<
    string,
    | { schema: StructDef; error?: undefined }
    | { error: string; schema?: undefined }
  >();
  private queryOptionsReader: SnowflakeQueryOptionsReader;
  private configReader: SnowflakeConnectionConfigurationReader;
  public readonly name;

  constructor(
    name: string,
    queryOptionsReader: SnowflakeQueryOptionsReader = {},
    configReader: SnowflakeConnectionConfigurationReader = {}
  ) {
    this.queryOptionsReader = queryOptionsReader;
    this.configReader = configReader;
    this.name = name;
  }

  private async readQueryOptions(): Promise<SnowflakeQueryOptions> {
    if (this.queryOptionsReader instanceof Function) {
      return this.queryOptionsReader();
    } else {
      return this.queryOptionsReader;
    }
  }

  private async readConfig(): Promise<SnowflakeConnectionConfiguration> {
    if (this.configReader instanceof Function) {
      return this.configReader();
    } else {
      return this.configReader;
    }
  }

  get dialectName(): string {
    return "snowflake";
  }

  public isPool(): this is PooledConnection {
    return false;
  }

  public canPersist(): this is PersistSQLResults {
    return true;
  }

  public canFetchSchemaAndRunSimultaneously(): this is FetchSchemaAndRunSimultaneously {
    // TODO feature-sql-block Implement FetchSchemaAndRunSimultaneously
    return false;
  }

  public canFetchSchemaAndRunStreamSimultaneously(): this is FetchSchemaAndRunStreamSimultaneously {
    return false;
  }

  public canStream(): this is StreamingConnection {
    return true;
  }

  public async fetchSchemaForTables(missing: string[]): Promise<{
    schemas: Record<string, StructDef>;
    errors: Record<string, string>;
  }> {
    const schemas: NamedStructDefs = {};
    const errors: { [name: string]: string } = {};

    for (const tableURL of missing) {
      let inCache = this.schemaCache.get(tableURL);
      if (!inCache) {
        try {
          inCache = {
            schema: await this.getTableSchema(tableURL),
          };
          this.schemaCache.set(tableURL, inCache);
        } catch (error) {
          inCache = { error: error.message };
        }
      }
      if (inCache.schema !== undefined) {
        schemas[tableURL] = inCache.schema;
      } else {
        errors[tableURL] = inCache.error;
      }
    }
    return { schemas, errors };
  }

  public async fetchSchemaForSQLBlocks(sqlRefs: SQLBlock[]): Promise<{
    schemas: Record<string, StructDef>;
    errors: Record<string, string>;
  }> {
    const schemas: NamedStructDefs = {};
    const errors: { [name: string]: string } = {};

    for (const sqlRef of sqlRefs) {
      const key = sqlRef.name;
      let inCache = this.sqlSchemaCache.get(key);
      if (!inCache) {
        try {
          inCache = {
            schema: await this.getSQLBlockSchema(sqlRef),
          };
          this.schemaCache.set(key, inCache);
        } catch (error) {
          inCache = { error: error.message };
        }
      }
      if (inCache.schema !== undefined) {
        schemas[key] = inCache.schema;
      } else {
        errors[key] = inCache.error;
      }
    }
    return { schemas, errors };
  }

  protected async getClient(): Promise<SnowflakeSDKConnection> {
    const config = await this.readConfig();

    const connection = createConnection({
      ...config,
      account: config.account || "",
      username: config.username || "",
      schema: `"UpperSchema"`,
    });

    connection.connect((err, conn) => {
      if (err) {
        throw new SnowflakeAuthenticationError(err.message);
      } else {
        // eslint-disable-next-line no-console
        console.log(`Successfully connected to Snowflake: ${conn.getId()}`);
      }
    });

    return connection;
  }

  protected async runSnowflakeQuery(
    sqlCommand: string
  ): Promise<MalloyQueryData> {
    const client = await this.getClient();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const execute = (): Promise<any[] | undefined> => {
      return new Promise((resolve, reject) => {
        const options = {
          sqlText: sqlCommand,
          complete: function (
            err: SnowflakeError | undefined,
            stmt: Statement | undefined,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            rows: any[] | undefined
          ) {
            if (err) {
              reject(err);
            }
            resolve(rows);
          },
        };
        client.execute(options);
      });
    };

    const result = await execute();
    if (result) {
      return { rows: result as QueryData, totalRows: result.length };
    } else {
      return { rows: [] as QueryData, totalRows: 0 };
    }
  }

  private async getSQLBlockSchema(sqlRef: SQLBlock): Promise<StructDef> {
    const structDef: StructDef = {
      type: "struct",
      dialect: "snowflake",
      name: sqlRef.name,
      structSource: {
        type: "sql",
        method: "subquery",
        sqlBlock: sqlRef,
      },
      structRelationship: {
        type: "basetable",
        connectionName: this.name,
      },
      fields: [],
    };

    const tempTableName = `TMP_${Math.floor(Math.random() * 10000000)}`;
    await this.runSnowflakeQuery(`DROP TABLE IF EXISTS ${tempTableName}`);
    await this.runSnowflakeQuery(`
      CREATE TRANSIENT TABLE ${tempTableName} AS
      SELECT * FROM (
        ${sqlRef.select}
      ) AS x WHERE false
      `);

    const infoQuery = `
      SELECT
        column_name
        , c.data_type
      FROM information_schema.columns c
      WHERE table_name = '${tempTableName}';
    `;
    await this.schemaFromQuery(
      infoQuery,
      structDef,
      tempTableName,
      sqlRef.select
    );
    return structDef;
  }

  private async schemaFromQuery(
    infoQuery: string,
    structDef: StructDef,
    tempTableName?: string,
    selectStatement?: string
  ): Promise<void> {
    const result = await this.runSnowflakeQuery(infoQuery);
    for (const row of result.rows) {
      const snowflakeDataType = row["DATA_TYPE"] as string;
      let s = structDef;
      let malloyType = snowflakeToMalloyTypes[snowflakeDataType];
      let name = row["COLUMN_NAME"] as string;

      // TODO: figure out how to get variant schemas...
      // Do something like SELECT ... LIMIT 1 w/ LATERAL FLATTEN to explode struct, then use `TYPEOF` to get types
      // See fillStructDefFromTypeMap in duckdb
      if (snowflakeDataType === "VARIANT") {
        malloyType = snowflakeToMalloyTypes[row["element_type"] as string];
        s = {
          type: "struct",
          name: row["COLUMN_NAME"] as string,
          dialect: this.dialectName,
          structRelationship: { type: "nested", field: name, isArray: true },
          structSource: { type: "nested" },
          fields: [],
        };
        structDef.fields.push(s);
        name = "VALUE";
      } else if (snowflakeDataType === "ARRAY") {
        const selectLimitOne = `
        SELECT
          GET(${name}, 0) AS sample_element,
          TYPEOF(sample_element) AS data_type
        FROM (${selectStatement})
        LIMIT 1`;

        const sampleQueryResult = await this.runSnowflakeQuery(selectLimitOne);

        if (sampleQueryResult.rows.length > 0) {
          const r = sampleQueryResult.rows[0];
          const sfDataType = r["DATA_TYPE"] as string;
          malloyType = snowflakeToMalloyTypes[sfDataType];
          s = {
            type: "struct",
            name: row["COLUMN_NAME"] as string,
            dialect: this.dialectName,
            structRelationship: { type: "nested", field: name, isArray: true },
            structSource: { type: "nested" },
            fields: [],
          };
          structDef.fields.push(s);
          name = "VALUE";
        } else {
          throw new Error(`Source select ${selectStatement} returned no rows`);
        }
      }

      if (malloyType !== undefined) {
        s.fields.push({
          type: malloyType,
          name,
        });
      } else {
        throw new Error(`unknown Snowflake type ${snowflakeDataType}`);
      }
    }
    if (tempTableName) {
      await this.runSnowflakeQuery(`DROP TABLE IF EXISTS ${tempTableName}`);
    }
  }

  private async getTableSchema(tableURL: string): Promise<StructDef> {
    const { tablePath: tableName } = parseTableURI(tableURL);
    const structDef: StructDef = {
      type: "struct",
      name: tableURL,
      dialect: "snowflake",
      structSource: { type: "table", tablePath: tableName },
      structRelationship: {
        type: "basetable",
        connectionName: this.name,
      },
      fields: [],
    };

    const [schema, table] = tableName.split(".");
    if (table === undefined) {
      throw new Error("Default schema not yet supported in Snowflake");
    }
    const infoQuery = `
      SELECT
        column_name
        , data_type
      FROM information_schema.columns c
      WHERE table_name = UPPER('${table}')
      AND table_schema = UPPER('${schema}')
    `;

    await this.schemaFromQuery(infoQuery, structDef);
    return structDef;
  }

  public async executeSQLRaw(query: string): Promise<QueryData> {
    const queryData = await this.runSnowflakeQuery(query);
    return queryData.rows;
  }

  public async test(): Promise<void> {
    await this.executeSQLRaw("SELECT 1");
  }

  public async runSQL(sql: string): Promise<MalloyQueryData> {
    return await this.runSnowflakeQuery(sql);
  }

  public async *runSQLStream(
    sqlCommand: string,
    options?: { rowLimit?: number }
  ): AsyncIterableIterator<QueryDataRow> {
    const client = await this.getClient();
    const statement = client.execute({
      sqlText: sqlCommand,
    });
    let index = 0;
    const stream = statement.streamRows();
    for await (const row of stream) {
      yield row as QueryDataRow;
      index += 1;
      if (options?.rowLimit !== undefined && index >= options.rowLimit) {
        // eslint-disable-next-line no-console
        client.destroy((err) => console.log(err));
        break;
      }
    }
  }

  public async manifestTemporaryTable(sqlCommand: string): Promise<string> {
    const hash = crypto.createHash("md5").update(sqlCommand).digest("hex");
    const tableName = `tt${hash}`;

    // TODO: need some way to clean these up.
    const cmd = `CREATE OR REPLACE TRANSIENT TABLE ${tableName} AS (${sqlCommand});`;
    // console.log(cmd);
    await this.runSnowflakeQuery(cmd);
    return tableName;
  }
}

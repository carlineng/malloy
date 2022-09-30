/* eslint-disable no-console */
/*
 * Copyright 2021 Google LLC
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without evenro the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 */

import { Result } from "@malloydata/malloy";
import { RuntimeList } from "../../runtimes";
import { describeIfDatabaseAvailable } from "../../util";

const [describe] = describeIfDatabaseAvailable(["snowflake"]);

describe("Snowflake tests", () => {
  const runtimeList = new RuntimeList(["snowflake"]);
  const runtime = runtimeList.runtimeMap.get("snowflake");
  if (runtime === undefined) {
    throw new Error("Couldn't build runtime");
  }

  // Idempotently create schema and tables with capital letters to use in tests.
  beforeAll(async () => {
    await runtime.connection.runSQL(
      'create schema if not exists "UpperSchema";'
    );
    await Promise.all([
      runtime.connection.runSQL(
        'create table if not exists "UpperSchema"."UpperSchemaUpperTable" as select 1 as one;'
      ),
      runtime.connection.runSQL(
        'create table if not exists "UpperTablePublic" as select 1 as one;'
      ),
    ]);
  });

  afterAll(async () => {
    await runtimeList.closeAll();
  });

  // How to handle casing? Snowflake is case-insensitive and re-maps lowercase to uppercase by default...
  it(`❌ lowercase column aliases`, async () => {
    const result: Result = await runtime
      .loadQuery(
        `
      sql: one is ||
        SELECT 1 as n
       ;;

      query: from_sql(one) -> { project: n }
      `
      )
      .run();
    expect(result.data.value[0].N).toBe(1);
  });

  it(`✅ uppercase column aliases`, async () => {
    const result: Result = await runtime
      .loadQuery(
        `
      sql: one is ||
        SELECT 1 as N
       ;;

      query: from_sql(one) -> { project: N }
      `
      )
      .run();
    expect(result.data.value[0].N).toBe(1);
  });
});

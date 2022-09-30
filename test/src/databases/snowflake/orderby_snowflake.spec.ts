/* eslint-disable no-console */
/*
 * Copyright 2021 Google LLC
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

import * as malloy from "@malloydata/malloy";
import { RuntimeList } from "../../runtimes";

const runtimes = new RuntimeList(["snowflake"]);

afterAll(async () => {
  await runtimes.closeAll();
});

async function validateCompilation(
  databaseName: string,
  sql: string
): Promise<boolean> {
  try {
    const runtime = runtimes.runtimeMap.get(databaseName);
    if (runtime === undefined) {
      throw new Error(`Unknown database ${databaseName}`);
    }
    await (
      await runtime.connections.lookupConnection(databaseName)
    ).runSQL(`WITH test AS(\n${sql}) SELECT '[{"foo":1}]' as results`);
  } catch (e) {
    console.log(`SQL: didn't compile\n=============\n${sql}`);
    throw e;
  }
  return true;
}

const expressionModels = new Map<string, malloy.ModelMaterializer>();
runtimes.runtimeMap.forEach((runtime, databaseName) =>
  expressionModels.set(
    databaseName,
    runtime.loadModel(`
    explore: MODELS is table('TEST.AIRCRAFT_MODELS'){
      measure: MODEL_COUNT is count()
    }
  `)
  )
);

expressionModels.forEach((orderByModel, databaseName) => {
  it(`✅ boolean type - ${databaseName}`, async () => {
    const result = await orderByModel
      .loadQuery(
        `
        query: MODELS-> {
          group_by: BIG is SEATS >=20
          aggregate: MODEL_COUNT is count()
        }
        `
      )
      .run();
    expect(result.data.row(0).cell("BIG").value).toBe(false);
    expect(result.data.row(0).cell("MODEL_COUNT").value).toBe(58451);
  });

  it(`✅ boolean in pipeline - ${databaseName}`, async () => {
    const result = await orderByModel
      .loadQuery(
        `
        query: MODELS->{
          group_by:
            MANUFACTURER,
            BIG is SEATS >=21
          aggregate: MODEL_COUNT is count()
        }->{
          group_by: BIG
          aggregate: MODEL_COUNT is MODEL_COUNT.sum()
        }
        `
      )
      .run();
    expect(result.data.row(0).cell("BIG").value).toBe(false);
    expect(result.data.row(0).cell("MODEL_COUNT").value).toBe(58500);
  });

  it(`✅ filtered measures in model are aggregates #352 - ${databaseName}`, async () => {
    const result = await orderByModel
      .loadQuery(
        `
        query: MODELS->{
          aggregate: J_NAMES is MODEL_COUNT {where: MANUFACTURER ~ 'J%'}
        }
        -> {
          group_by: J_NAMES
        }
        `
      )
      .run();
    expect(result.data.row(0).cell("J_NAMES").value).toBe(1358);
  });

  it(`✅ reserved words are quoted - ${databaseName}`, async () => {
    const sql = await orderByModel
      .loadQuery(
        `
      query: MODELS->{
        aggregate: FETCH is count()
      }->{
        group_by: FETCH
      }
      `
      )
      .getSQL();
    await validateCompilation(databaseName, sql);
  });

  it(`✅ reserved words are quoted in turtles - ${databaseName}`, async () => {
    const sql = await orderByModel
      .loadQuery(
        `
      query: MODELS->{
        nest: WITHX is {
          group_by: SELECT is UPPER(MANUFACTURER)
          aggregate: FETCH is count()
        }
      } -> {
        project:
          WITHXZ is lower(WITHX.SELECT)
          FETCH is WITHX.FETCH
      }
      `
      )
      .getSQL();
    await validateCompilation(databaseName, sql);
  });

  it.skip("reserved words in structure definitions", async () => {
    const sql = await orderByModel
      .loadQuery(
        `
      query: MODELS->{
        nest: WITHX is {
          group_by: is SELECT is UPPER(MANUFACTURER)
          aggregate: FETCH is count()
        }
      } -> {
        project: WITHXIS lower(WITHX.SELECT)
        project: FETCH is WITH.FETCH
      }
      `
      )
      .getSQL();
    await validateCompilation(databaseName, sql);
  });

  it(`✅ aggregate and scalar conditions - ${databaseName}`, async () => {
    const sql = await orderByModel
      .loadQuery(
        `
      query: MODELS->{
        aggregate: MODEL_COUNT is count(){? MANUFACTURER ? ~'A%' }
      }
      `
      )
      .getSQL();
    await validateCompilation(databaseName, sql);
  });

  // I'm not sure I have the syntax right here...
  it(`✅ modeled having simple - ${databaseName}`, async () => {
    const result = await orderByModel
      .loadQuery(
        `
        explore: POPULAR_NAMES is from(MODELS->{
          where: MODEL_COUNT > 100
          group_by: MANUFACTURER
          aggregate: MODEL_COUNT
        })

        query: POPULAR_NAMES->{
          order_by: 2
          project: MANUFACTURER, MODEL_COUNT
        }
        `
      )
      .run();
    expect(result.data.row(0).cell("MODEL_COUNT").value).toBe(102);
  });

  it(`✅ modeled having complex - ${databaseName}`, async () => {
    const result = await orderByModel
      .loadQuery(
        `
        explore: POPULAR_NAMES is from(MODELS->{
          where: MODEL_COUNT > 100
          group_by: MANUFACTURER
          aggregate: MODEL_COUNT
          nest: L is {
            top: 5
            group_by: MANUFACTURER
            aggregate: MODEL_COUNT
          }
        })

        query: POPULAR_NAMES->{
         order_by: 2
         project: MANUFACTURER, MODEL_COUNT
        }
        `
      )
      .run();
    expect(result.data.row(0).cell("MODEL_COUNT").value).toBe(102);
  });

  it(`✅ turtle references joined element - ${databaseName}`, async () => {
    const sql = await orderByModel
      .loadQuery(
        `
    explore: A is table('TEST.AIRCRAFT'){
      primary_key: TAIL_NUM
      measure: AIRCRAFT_COUNT is count(*)
    }

    explore: F is table('TEST.FLIGHTS'){
      primary_key: ID2
      join_one: A with TAIL_NUM

      measure: FLIGHT_COUNT is count()
      query: FOO is {
        group_by: CARRIER
        aggregate: FLIGHT_COUNT
        aggregate: A.AIRCRAFT_COUNT
      }
    }
    query: F->FOO
  `
      )
      .getSQL();
    await validateCompilation(databaseName, sql);
  });
});

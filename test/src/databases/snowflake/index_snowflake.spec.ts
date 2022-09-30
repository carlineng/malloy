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

// eslint-disable-next-line @typescript-eslint/no-explicit-any

import { RuntimeList } from "../../runtimes";

// No prebuilt shared model, each test is complete.  Makes debugging easier.

const runtimes = new RuntimeList(["snowflake"]);

afterAll(async () => {
  await runtimes.closeAll();
});

runtimes.runtimeMap.forEach((runtime, databaseName) => {
  it(`✅ basic index  - ${databaseName}`, async () => {
    const model = await runtime.loadModel(
      `
        source: AIRPORTS is table('TEST.AIRPORTS') {
        }
    `
    );
    let result = await model.search("AIRPORTS", "SANTA", 10);

    // if (result !== undefined) {
    //   console.log(result);
    // } else {
    //   console.log("no result");
    // }
    expect(result).toBeDefined();
    if (result !== undefined) {
      expect(result[0].fieldName).toBe("COUNTY");
      expect(result[0].fieldValue).toBe("SANTA ROSA");
      expect(result[0].weight).toBe(26);
      expect(result.length).toBe(10);
    }

    result = await model.search("AIRPORTS", "SANTA A", 100, "CITY");
    if (result !== undefined) {
      // console.log(result);
      expect(result[0].fieldName).toBe("CITY");
      expect(result[0].fieldValue).toBe("SANTA ANA");
    }
  });

  it(`❌ index value map  - ${databaseName}`, async () => {
    const model = await runtime.loadModel(
      `
        source: AIRPORTS is table('TEST.AIRPORTS') {
        }
    `
    );
    const result = await model.searchValueMap("AIRPORTS");
    // if (result !== undefined) {
    //   console.log(result[4].values);
    // } else {
    //   console.log("no result");
    // }
    expect(result).toBeDefined();
    if (result !== undefined) {
      expect(result[4].values[0].fieldValue).toBe("WASHINGTON");
      expect(result[4].values[0].weight).toBe(214);
    }
  });

  it(`✅ index no sample rows - ${databaseName}`, async () => {
    const result = await runtime
      .loadQuery(
        `
        source: T is table('TEST.STATE_FACTS') {
          dimension: one is 'one'
        }

        query: T-> {index:one, STATE }
          -> {project: fieldName, weight, fieldValue; order_by: 2 desc; where: fieldName = 'one'}
    `
      )
      .run();
    // console.log(result.data.toObject());
    expect(result.data.path(0, "fieldName").value).toBe("one");
    expect(result.data.path(0, "weight").value).toBe(51);
  });

  // bigquery doesn't support row count based sampling.
  (databaseName === "bigquery" || databaseName === "snowflake" ? it.skip : it)(
    `index rows count - ${databaseName}`,
    async () => {
      const result = await runtime
        .loadQuery(
          `
        source: t is table('malloytest.state_facts') {
          dimension: one is 'one'
        }

        query: t-> {index:one, state; sample: 10 }
          -> {project: fieldName, weight, fieldValue; order_by: 2 desc; where: fieldName = 'one'}
    `
        )
        .run();
      expect(result.data.path(0, "fieldName").value).toBe("one");
      expect(result.data.path(0, "weight").value).toBe(10);
    }
  );

  it(`✅ index rows count - ${databaseName}`, async () => {
    const result = await runtime
      .loadQuery(
        `
        source: T is table('TEST.FLIGHTS') {
          dimension: one is 'one'
        }

        query: T-> {INDEX:one, TAILNUM; SAMPLE: 50% }
          -> {project: fieldName, weight, fieldValue; order_by: 2 desc; where: fieldName = 'one'}
    `
      )
      .run();
    // console.log(result.sql);
    // Hard to get consistent results here so just check that we get a value back.
    //console.log(result.data.toObject());
    expect(result.data.path(0, "fieldName").value).toBe("one");
  });

  // it(`fanned data index  - ${databaseName}`, async () => {
  //   const result = await runtime
  //     .loadModel(
  //       `
  //       source: movies is table('malloy-303216.imdb.movies') {
  //       }
  //   `
  //     )
  //     .search("movies", "Tom");
  //   // if (result !== undefined) {
  //   //   console.log(result);
  //   // } else {
  //   //   console.log("no result");
  //   // }
  //   expect(result).toBeDefined();
  //   if (result !== undefined) {
  //     expect(result[0].fieldName).toBe("county");
  //     expect(result[0].fieldValue).toBe("SANTA ROSA");
  //     expect(result[0].weight).toBe(26);
  //   }
  // });
});

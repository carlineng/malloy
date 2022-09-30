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
/* eslint-disable no-console */

import { RuntimeList } from "../../runtimes";

const joinModelText = `
  explore: AIRCRAFT_MODELS is table('TEST.AIRCRAFT_MODELS') {
    primary_key: AIRCRAFT_MODEL_CODE
    measure: MODEL_COUNT is count(*)
    query: MANUFACTURER_MODELS is {
      group_by: MANUFACTURER
      aggregate: NUM_MODELS is count(*)
    }
    query: MANUFACTURER_SEATS is {
      group_by: MANUFACTURER
      aggregate: TOTAL_SEATS is SEATS.sum()
    }
  }

  explore: AIRCRAFT is table('TEST.AIRCRAFT'){
    primary_key: TAIL_NUM
    measure: AIRCRAFT_COUNT is count(*)
  }

  explore: FUNNEL is from(AIRCRAFT_MODELS->MANUFACTURER_MODELS) {
    join_one: SEATS is from(AIRCRAFT_MODELS->MANUFACTURER_SEATS)
        with MANUFACTURER
  }
`;

const runtimes = new RuntimeList(["snowflake"]);

afterAll(async () => {
  await runtimes.closeAll();
});

// const models = new Map<string, malloy.ModelMaterializer>();
// runtimes.runtimeMap.forEach((runtime, key) => {
//   models.set(key, runtime.loadModel(joinModelText));
// });

describe("join expression tests", () => {
  runtimes.runtimeMap.forEach((runtime, database) => {
    it(`✅ model explore refine join - ${database}`, async () => {
      const result = await runtime
        .loadModel(joinModelText)
        .loadQuery(
          `
      explore: a2 is AIRCRAFT {
        join_one: AIRCRAFT_MODELS with AIRCRAFT_MODEL_CODE
      }

      query: a2 -> {
        aggregate:
          AIRCRAFT_COUNT
          AIRCRAFT_MODELS.MODEL_COUNT
      }
      `
        )
        .run();
      expect(result.data.value[0].MODEL_COUNT).toBe(1416);
    });

    it(`✅ model explore refine in query join - ${database}`, async () => {
      const result = await runtime
        .loadModel(joinModelText)
        .loadQuery(
          `
      query: AIRCRAFT {
        join_one: AIRCRAFT_MODELS with AIRCRAFT_MODEL_CODE
      } -> {
        aggregate:
          AIRCRAFT_COUNT
          AIRCRAFT_MODELS.MODEL_COUNT
      }
      `
        )
        .run();
      expect(result.data.value[0].MODEL_COUNT).toBe(1416);
    });

    it(`✅ model: join fact table query - ${database}`, async () => {
      const result = await runtime
        .loadModel(joinModelText)
        .loadQuery(
          `
      query: AIRCRAFT_MODELS {
        join_one: AM_FACTS is from(
          AIRCRAFT_MODELS->{
            group_by: M is MANUFACTURER
            aggregate: NUM_MODELS is count(*)
          }) with MANUFACTURER
      } -> {
        project:
          MANUFACTURER
          AM_FACTS.NUM_MODELS
        order_by: 2 desc
        limit: 1
      }
    `
        )
        .run();
      expect(result.data.value[0].NUM_MODELS).toBe(1147);
    });

    it(`✅ model: explore based on query - ${database}`, async () => {
      const result = await runtime
        .loadModel(joinModelText)
        .loadQuery(
          `
      query:
          AIRCRAFT_MODELS-> {
            group_by: M is MANUFACTURER
            aggregate: NUM_MODELS is count(*)
          }
      -> {
        project:
          M
          NUM_MODELS
        order_by: 2 desc
        limit: 1
      }
        `
        )
        .run();
      expect(result.data.value[0].NUM_MODELS).toBe(1147);
    });

    it(`✅ model: funnel - merge two queries - ${database}`, async () => {
      const result = await runtime
        .loadModel(joinModelText)
        .loadQuery(
          `
          query: from(AIRCRAFT_MODELS->{
            group_by: M is MANUFACTURER
            aggregate: NUM_MODELS is count(*)
            }){
            join_one: SEATS is from(
              AIRCRAFT_MODELS->{
                group_by: M is MANUFACTURER
                aggregate: TOTAL_SEATS is SEATS.sum()
              }
            ) with M
          }
          -> {
            project:
              M
              NUM_MODELS
              SEATS.TOTAL_SEATS
            order_by: 2 desc
            limit: 1
          }
        `
        )
        .run();
      expect(result.data.value[0].NUM_MODELS).toBe(1147);
      expect(result.data.value[0].TOTAL_SEATS).toBe(252771);
    });

    it(`✅ model: modeled funnel - ${database}`, async () => {
      const result = await runtime
        .loadModel(joinModelText)
        .loadQuery(
          `
      explore: FOO is from(AIRCRAFT_MODELS-> MANUFACTURER_MODELS){
        join_one: SEATS is from(AIRCRAFT_MODELS->MANUFACTURER_SEATS)
          with MANUFACTURER
      }
      query: FOO-> {
        project:
          MANUFACTURER,
          NUM_MODELS,
          SEATS.TOTAL_SEATS
        order_by: 2 desc
        limit: 1
      }
        `
        )
        .run();
      expect(result.data.value[0].NUM_MODELS).toBe(1147);
      expect(result.data.value[0].TOTAL_SEATS).toBe(252771);
    });

    it(`✅ model: modeled funnel2 - ${database}`, async () => {
      const result = await runtime
        .loadModel(joinModelText)
        .loadQuery(
          `
      query: FUNNEL->{
        project:
         MANUFACTURER
          NUM_MODELS
          SEATS.TOTAL_SEATS
        order_by: 2 desc
        limit: 1
      }
        `
        )
        .run();
      expect(result.data.value[0].NUM_MODELS).toBe(1147);
      expect(result.data.value[0].TOTAL_SEATS).toBe(252771);
    });

    it(`✅ model: double_pipe - ${database}`, async () => {
      const result = await runtime
        .loadModel(joinModelText)
        .loadQuery(
          `
      query: AIRCRAFT_MODELS->{
        group_by: MANUFACTURER
        aggregate: F is count(*)
      }->{
        aggregate: F_SUM is F.sum()
      }->{
        project: F_SUM2 is F_SUM+1
      }
    `
        )
        .run();
      expect(result.data.value[0].F_SUM2).toBe(60462);
    });

    it(`✅ model: unnest is left join - ${database}`, async () => {
      const result = await runtime
        .loadModel(joinModelText)
        .loadQuery(
          `
          // produce a table with 4 rows that has a nested element
          query: A_STATES is table('TEST.STATE_FACTS')-> {
            where: STATE ? ~ 'A%'
            group_by: STATE
            nest: SOMTHING is {group_by: STATE}
          }

          // join the 4 rows and reference the
          //  nested column. should return all the rows.
          //  If the unnest is an inner join, we'll get back just 4 rows.
          query: table('TEST.STATE_FACTS') {
            join_one: A_STATES is from(->A_STATES) with STATE
          }
          -> {
            group_by: STATE
            aggregate: C is count()
            nest: A is  {
              group_by: A_STATES.SOMTHING.STATE
            }
          }
    `
        )
        .run();
      // console.log(result.data.toObject());
      expect(result.data.rowCount).toBeGreaterThan(4);
    });

    // not sure how to solve this one yet.
    it(`✅ All joins at the same level - ${database}`, async () => {
      const result = await runtime
        .loadQuery(
          `
        source: FLIGHTS is table('TEST.FLIGHTS') {
          join_one: AIRCRAFT is table('TEST.AIRCRAFT')
            on TAIL_NUM = AIRCRAFT.TAIL_NUM
          join_one: AIRCRAFT_MODELS is table('TEST.AIRCRAFT_MODELS')
            on AIRCRAFT.AIRCRAFT_MODEL_CODE = AIRCRAFT_MODELS.AIRCRAFT_MODEL_CODE
        }

        query: FLIGHTS -> {
          group_by: AIRCRAFT_MODELS.SEATS
          aggregate: FLIGHT_COUNT is count()
        }
        `
        )
        .run();
      // console.log(result.data.toObject());
      expect(result.data.rowCount).toBeGreaterThan(4);
    });

    it(`✅ join issue440 - ${database}`, async () => {
      const result = await runtime
        .loadQuery(
          `
        source: AIRCRAFT_MODELS is table('TEST.AIRCRAFT_MODELS')

        source: AIRCRAFT is table('TEST.AIRCRAFT')

        source: FLIGHTS is table('TEST.FLIGHTS'){
          join_one: AIRCRAFT on AIRCRAFT.TAIL_NUM = TAIL_NUM
          join_one: AIRCRAFT_MODELS on AIRCRAFT_MODELS.AIRCRAFT_MODEL_CODE = AIRCRAFT.AIRCRAFT_MODEL_CODE
        }

        query: FLIGHTS-> {
          group_by: TESTINGTWO is AIRCRAFT_MODELS.MODEL
        }
      `
        )
        .run();
      // console.log(result.data.toObject());
      expect(result.data.rowCount).toBeGreaterThan(4);
    });
  });
});

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

import { QueryMaterializer, Result } from "@malloydata/malloy";
import { RuntimeList } from "../../runtimes";

// No prebuilt shared model, each test is complete.  Makes debugging easier.

// const runtimes = new RuntimeList(databasesFromEnvironmentOr(allDatabases));
// const runtimes = new RuntimeList(["postgres"]);
// const runtimes = new RuntimeList(["duckdb"]);
const runtimes = new RuntimeList(["snowflake"]);

const splitFunction: Record<string, string> = {
  bigquery: "split",
  postgres: "string_to_array",
  duckdb: "string_to_array",
  snowflake: "split",
};

afterAll(async () => {
  await runtimes.closeAll();
});

// describe("snowflake tmp tests", () => {
const databaseName = "snowflake";
const runtime = runtimes.runtimeMap.get(databaseName);

// Issue: #151
// it(`unknown dialect  - ${databaseName}`, async () => {
//   const result = await runtime
//     .loadQuery(
//       `
//       query: q is table('${schema}.aircraft')->{
//         where: state != null
//         group_by: state
//       }

//       explore: r is from(->q){
//         query: foo is {
//           order_by: 1 desc
//           group_by: state
//         }
//       }

//       query: r->foo
//   `
//     )
//     .run();
//   // console.log(result.data.toObject());
//   expect(result.data.path(0, "state").value).toBe("WY");
// });
let schema = "${schema}";
if (databaseName === "snowflake") {
  schema = "TEST";
}
if (!runtime) {
  throw new Error("Undefined runtime");
}

// Issue #149
it(`✅ refine query from query  - ${databaseName}`, async () => {
  const query = runtime.loadQuery(
    `
        query: from(
          table('${schema}.STATE_FACTS')->{group_by: STATE; order_by: 1 desc; limit: 1}
          )
          {
            dimension: LOWER_STATE is lower(STATE)
          }
          -> {project: LOWER_STATE}
        `
  );

  const result = await query.run();
  // console.log(result.data.toObject());
  expect(result.data.path(0, "LOWER_STATE").value).toBe("wy");
});

// issue #157
it(`✅ explore - not -found  - ${databaseName}`, async () => {
  // console.log(result.data.toObject());
  let error;
  try {
    await runtime
      .loadQuery(
        `
        explore: foo is table('.STATE_FACTS'){primary_key: state}
        query: foox->{aggregate: c is count()}
       `
      )
      .run();
  } catch (e) {
    error = e;
  }
  expect(error.toString()).not.toContain("Unknown Dialect");
});

it(`✅ join_many - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      explore: a is table('${schema}.AIRCRAFT'){
        measure: avg_year is floor(avg(YEAR_BUILT))
      }
      explore: m is table('${schema}.AIRCRAFT_MODELS'){
        join_many: a on a.AIRCRAFT_MODEL_CODE=AIRCRAFT_MODEL_CODE
        measure: avg_seats is floor(avg(SEATS))
      }
      query: m->{aggregate: avg_seats, a.avg_year}
      `
    )
    .run();
  expect(result.data.value[0].avg_year).toBe(1969);
  expect(result.data.value[0].avg_seats).toBe(7);
});

it(`✅ join_many condition no primary key - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      explore: a is table('${schema}.AIRPORTS'){}
      explore: b is table('${schema}.STATE_FACTS') {
        join_many: a on STATE=a.STATE
      }
      query: b->{aggregate: c is AIRPORT_COUNT.sum()}
      `
    )
    .run();
  expect(result.data.value[0].c).toBe(19701);
});

it(`✅ join_many filter multiple values - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      explore: a is table('${schema}.AIRPORTS'){
        where: STATE = 'NH' | 'CA'
      }
      explore: b is table('${schema}.STATE_FACTS') {
        join_many: a on STATE=a.STATE
      }
      query: b->{
        aggregate: c is AIRPORT_COUNT.sum()
        group_by: a.STATE
      }
      `
    )
    .run();
  expect(result.data.value[0].c).toBe(18605);
  expect(result.data.value[0].STATE).toBeNull();
  expect(result.data.value[1].c).toBe(984);
  expect(result.data.value[1].STATE).toBe("CA");
  expect(result.data.value[2].c).toBe(112);
  expect(result.data.value[2].STATE).toBe("NH");
});

it(`✅ join_one condition no primary key - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      explore: a is table('${schema}.STATE_FACTS'){}
      explore: b is table('${schema}.AIRPORTS') {
        join_one: a on STATE=a.STATE
      }
      query: b->{aggregate: c is a.AIRPORT_COUNT.sum()}

      `
    )
    .run();
  expect(result.data.value[0].c).toBe(19701);
});

it(`✅ join_one filter multiple values - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      explore: a is table('${schema}.STATE_FACTS'){
        where: STATE = 'TX' | 'LA'
      }
      explore: b is table('${schema}.AIRPORTS') {
        join_one: a on STATE=a.STATE
      }
      query: b->{
        aggregate: c is a.AIRPORT_COUNT.sum()
        group_by: a.STATE
      }
      `
    )
    .run();
  // https://github.com/looker-open-source/malloy/pull/501#discussion_r861022857
  expect(result.data.value).toHaveLength(3);
  expect(result.data.value).toContainEqual({ c: 1845, STATE: "TX" });
  expect(result.data.value).toContainEqual({ c: 500, STATE: "LA" });
  expect(result.data.value).toContainEqual({ c: 0, STATE: null });
});

it(`✅ join_many cross from  - ${databaseName}`, async () => {
  // a cross join produces a Many to Many result.
  // symmetric aggregate are needed on both sides of the join
  // Check the row count and that sums on each side work properly.
  const result = await runtime
    .loadQuery(
      `
      explore: a is table('${schema}.STATE_FACTS')
      explore: f is a{
        join_cross: a
      }
      query: f->{
        aggregate:
          row_count is count(distinct concat(STATE,a.STATE))
          left_count is count()
          right_count is a.count()
          left_sum is AIRPORT_COUNT.sum()
          right_sum is a.AIRPORT_COUNT.sum()
      }
      `
    )
    .run();
  expect(result.data.value[0].row_count).toBe(51 * 51);
  expect(result.data.value[0].left_sum).toBe(19701);
  expect(result.data.value[0].right_sum).toBe(19701);
});

it(`✅ join_one only  - ${databaseName}`, async () => {
  // a cross join produces a Many to Many result.
  // symmetric aggregate are needed on both sides of the join
  // Check the row count and that sums on each side work properly.
  const result = await runtime
    .loadQuery(
      `
      query: q is table('${schema}.STATE_FACTS')->{
        aggregate: r is AIRPORT_COUNT.sum()
      }
      explore: f is table('${schema}.STATE_FACTS'){
        join_one: a is from(->q)
      }
      query: f->{
        aggregate:
          row_count is count(distinct concat(STATE,a.r))
          left_sum is AIRPORT_COUNT.sum()
          right_sum is a.r.sum()
          sum_sum is sum(AIRPORT_COUNT + a.r)
      }
      `
    )
    .run();
  expect(result.data.value[0].row_count).toBe(51);
  expect(result.data.value[0].left_sum).toBe(19701);
  expect(result.data.value[0].right_sum).toBe(19701);
  expect(result.data.value[0].sum_sum).toBe(19701 + 51 * 19701);
});

it(`✅ join_many cross ON  - ${databaseName}`, async () => {
  // a cross join produces a Many to Many result.
  // symmetric aggregate are needed on both sides of the join
  // Check the row count and that sums on each side work properly.
  const result = await runtime
    .loadQuery(
      `
      explore: a is table('${schema}.STATE_FACTS')
      explore: f is a{
        join_cross: a on a.STATE = 'CA' | 'NY'
      }
      query: f->{
        aggregate:
          row_count is count(distinct concat(STATE,a.STATE))
          left_sum is AIRPORT_COUNT.sum()
          right_sum is a.AIRPORT_COUNT.sum()
      }
      `
    )
    .run();
  expect(result.data.value[0].row_count).toBe(51 * 2);
  expect(result.data.value[0].left_sum).toBe(19701);
  expect(result.data.value[0].right_sum).toBe(1560);
});

it(`✅ limit - provided - ${databaseName}`, async () => {
  // a cross join produces a Many to Many result.
  // symmetric aggregate are needed on both sides of the join
  // Check the row count and that sums on each side work properly.
  const query = `
    query: table('${schema}.STATE_FACTS') -> {
        group_by: STATE
        aggregate: c is count()
        limit: 3
      }
    `;

  const queryMaterializer = runtime.loadQuery(query);
  const sqlQuery = await queryMaterializer.getSQL();
  console.log(sqlQuery);
  const result = await queryMaterializer.run();
  expect(result.resultExplore.limit).toBe(3);
});

// it(`number as null- ${databaseName}`, async () => {
//   // a cross join produces a Many to Many result.
//   // symmetric aggregate are needed on both sides of the join
//   // Check the row count and that sums on each side work properly.
//   const result = await runtime
//     .loadQuery(
//       `
//       source: s is table('${schema}.state_facts') + {
//       }
//       query: s-> {
//         group_by: state
//         nest: ugly is {
//           group_by: popular_name
//           aggregate: foo is NULLIF(sum(airport_count)*0,0)+1
//         }
//       }
//     `
//     )
//     .run();
//   expect(result.data.path(0, "ugly", 0, "foo").value).toBe(null);
// });

it(`✅ limit - not provided - ${databaseName}`, async () => {
  // a cross join produces a Many to Many result.
  // symmetric aggregate are needed on both sides of the join
  // Check the row count and that sums on each side work properly.
  const result = await runtime
    .loadQuery(
      `
      query: table('${schema}.STATE_FACTS') -> {
        group_by: STATE
        aggregate: c is count()
      }
      `
    )
    .run();
  expect(result.resultExplore.limit).toBe(undefined);
});

it(`✅ limit pipeline - provided - ${databaseName}`, async () => {
  // a cross join produces a Many to Many result.
  // symmetric aggregate are needed on both sides of the join
  // Check the row count and that sums on each side work properly.
  const result = await runtime
    .loadQuery(
      `
      query: table('${schema}.STATE_FACTS') -> {
        project: STATE
        limit: 10
      }
      -> {
        project: STATE
        limit: 3
      }
      `
    )
    .run();
  expect(result.resultExplore.limit).toBe(3);
});

// TODO: casing error, "births_per_100k__1" is not referenced in quotes
// Need to capitalize it to get it to work.
it(`✅ ungrouped top level - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
        source: s is table('${schema}.STATE_FACTS') + {
          measure: total_births is BIRTHS.sum()
          measure: BIRTHS_PER_100K is floor(total_births/ all(total_births) * 100000)
        }

        query:s-> {
          group_by: STATE
          aggregate: BIRTHS_PER_100K
        }
      `
    )
    .run();
  // console.log(result.sql);
  expect(result.data.path(0, "BIRTHS_PER_100K").value).toBe(9742);
});

it(`✅ ungrouped top level with nested  - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
        source: s is table('${schema}.STATE_FACTS') + {
          measure: TOTAL_BIRTHS is BIRTHS.sum()
          measure: BIRTHS_PER_100K is floor(TOTAL_BIRTHS/ all(TOTAL_BIRTHS) * 100000)
        }

        query:s-> {
          group_by: STATE
          aggregate: BIRTHS_PER_100K
          nest: by_name is {
            group_by: POPULAR_NAME
            aggregate: TOTAL_BIRTHS
          }
          limit: 1000
        }
      `
    )
    .run();
  expect(result.data.path(0, "BIRTHS_PER_100K").value).toBe(9742);
});

it(`✅ ungrouped - eliminate rows  - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
        source: s is table('${schema}.STATE_FACTS') + {
          measure: M is all(BIRTHS.sum())
          where: STATE='CA' | 'NY'
        }

        query:s-> {
          group_by: STATE
          aggregate: M
        }
      `
    )
    .run();
  // console.log(result.sql);
  expect(result.data.toObject().length).toBe(2);
});

it(`✅ ungrouped nested with no grouping above - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
        source: s is table('${schema}.STATE_FACTS') + {
          measure: TOTAL_BIRTHS is BIRTHS.sum()
          measure: BIRTHS_PER_100K is floor(TOTAL_BIRTHS/ all(TOTAL_BIRTHS) * 100000)
        }

        query: s-> {
          aggregate: TOTAL_BIRTHS
          nest: BY_NAME is {
            group_by: POPULAR_NAME
            aggregate: BIRTHS_PER_100K
          }
        }

      `
    )
    .run();
  // console.log(result.sql);
  expect(result.data.path(0, "BY_NAME", 0, "BIRTHS_PER_100K").value).toBe(
    66703
  );
});

it(`✅ ungrouped - partial grouping - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
        source: AIRPORTS is table('${schema}.AIRPORTS') {
          measure: C is count()
        }


         query: AIRPORTS -> {
          where: STATE = 'TX' | 'NY'
          group_by:
            FAA_REGION
            STATE
          aggregate:
            C
            ALL_ is all(C)
            AIRPORT_COUNT is C {? FAC_TYPE = 'AIRPORT'}
          nest: FAC_TYPE is {
            group_by: FAC_TYPE
            aggregate:
              C
              ALL_ is all(C)
              ALL_STATE_REGION is exclude(C,FAC_TYPE)
              ALL_OF_THIS_TYPE is exclude(C, STATE, FAA_REGION)
              ALL_TOP is exclude(C, STATE, FAA_REGION, FAC_TYPE)
          }
        }

      `
    )
    .run();
  // console.log(result.sql);
  expect(result.data.path(0, "FAC_TYPE", 0, "ALL_").value).toBe(1845);
  expect(result.data.path(0, "FAC_TYPE", 0, "ALL_STATE_REGION").value).toBe(
    1845
  );
  expect(result.data.path(0, "FAC_TYPE", 0, "ALL_OF_THIS_TYPE").value).toBe(
    1782
  );
  expect(result.data.path(0, "FAC_TYPE", 0, "ALL_TOP").value).toBe(2421);
});

it(`✅ ungrouped - all nested - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
        source: airports is table('${schema}.AIRPORTS') {
          measure: C is count()
        }


         query: airports -> {
          where: STATE = 'TX' | 'NY'
          group_by:
            STATE
          aggregate:
            C
            ALL_ is all(C)
            AIRPORT_COUNT is C {? FAC_TYPE = 'AIRPORT'}
          nest: FAC_TYPE is {
            group_by: FAC_TYPE, MAJOR
            aggregate:
              C
              ALL_ is all(C)
              ALL_MAJOR is all(C,MAJOR)
          }
        }


      `
    )
    .run();
  // console.log(result.sql);
  expect(result.data.path(0, "FAC_TYPE", 0, "ALL_").value).toBe(1845);
  expect(result.data.path(0, "FAC_TYPE", 0, "ALL_MAJOR").value).toBe(1819);
});

it(`✅ ungrouped nested  - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
        source: s is table('${schema}.STATE_FACTS') + {
          measure: TOTAL_BIRTHS is BIRTHS.sum()
          measure: BIRTHS_PER_100K is floor(TOTAL_BIRTHS/ all(TOTAL_BIRTHS) * 100000)
        }

        query:s ->  {
          group_by: POPULAR_NAME
          nest: BY_STATE is {
            group_by: STATE
            aggregate: BIRTHS_PER_100K
          }
        }

      `
    )
    .run();
  // console.log(result.sql);
  expect(result.data.path(0, "BY_STATE", 0, "BIRTHS_PER_100K").value).toBe(
    36593
  );
});

it(`✅ ungrouped nested expression  - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
        source: s is table('${schema}.STATE_FACTS') + {
          measure: TOTAL_BIRTHS is BIRTHS.sum()
          measure: BIRTHS_PER_100K is floor(TOTAL_BIRTHS/ all(TOTAL_BIRTHS) * 100000)
        }

        query:s ->  {
          group_by: UPPER_NAME is upper(POPULAR_NAME)
          nest: BY_STATE is {
            group_by: STATE
            aggregate: BIRTHS_PER_100K
          }
        }

      `
    )
    .run();
  // console.log(result.sql);
  expect(result.data.path(0, "BY_STATE", 0, "BIRTHS_PER_100K").value).toBe(
    36593
  );
});

it(`✅ ungrouped nested group by float  - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
        source: s is table('${schema}.STATE_FACTS') + {
          measure: TOTAL_BIRTHS is BIRTHS.sum()
          measure: UG is all(TOTAL_BIRTHS)
        }

        query:s ->  {
          group_by: F is floor(AIRPORT_COUNT/300.0)
          nest: BY_STATE is {
            group_by: STATE
            aggregate: UG
          }
        }

      `
    )
    .run();
  // console.log(result.sql);
  // console.log(JSON.stringify(result.data.toObject(), null, 2));
  expect(result.data.path(0, "BY_STATE", 0, "UG").value).toBe(62742230);
});

it(`✅ all with parameters - basic  - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
        source: S is table('${schema}.STATE_FACTS') + {
          measure: TOTAL_BIRTHS is BIRTHS.sum()
        }

        query: S -> {
          group_by: POPULAR_NAME, STATE
          aggregate:
            TOTAL_BIRTHS
            ALL_BIRTHS is all(TOTAL_BIRTHS)
            ALL_NAME is exclude(TOTAL_BIRTHS, STATE)
        }

      `
    )
    .run();
  // console.log(result.sql);
  // console.log(JSON.stringify(result.data.toObject(), null, 2));
  expect(result.data.path(0, "ALL_BIRTHS").value).toBe(295727065);
  expect(result.data.path(0, "ALL_NAME").value).toBe(197260594);
});

it(`✅ all with parameters - nest  - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
        source: S is table('${schema}.STATE_FACTS') + {
          measure: TOTAL_BIRTHS is BIRTHS.sum()
          dimension: ABC is floor(AIRPORT_COUNT/300)
        }

        query: S -> {
          group_by: ABC
          aggregate: TOTAL_BIRTHS
          nest: BY_STUFF is {
            group_by: POPULAR_NAME, STATE
            aggregate:
              TOTAL_BIRTHS
              ALL_BIRTHS is all(TOTAL_BIRTHS)
              ALL_NAME is exclude(TOTAL_BIRTHS, STATE)
          }
        }

      `
    )
    .run();
  // console.log(result.sql);
  // console.log(JSON.stringify(result.data.toObject(), null, 2));
  expect(result.data.path(0, "BY_STUFF", 0, "ALL_BIRTHS").value).toBe(
    119809719
  );
  expect(result.data.path(0, "BY_STUFF", 0, "ALL_NAME").value).toBe(61091215);
});

it(`❌ single value to udf - snowflake`, async () => {
  const loadedQuery: QueryMaterializer = runtime.loadQuery(
    `
      source: F is  table('${schema}.STATE_FACTS') {
        query: FUN is {
          aggregate: T is count()
        }
        -> {
          project: T1 is T+1
        }
      }
      query: F-> {
        nest: FUN
      }
      `
  );

  const result: Result = await loadedQuery.run();
  // console.log(result.sql);
  expect(result.data.path(0, "FUN", 0, "T1").value).toBe(52);
});

it(`❌ Multi value to udf - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      source: F is  table('${schema}.STATE_FACTS') {
        query: FUN is {
          group_by: ONE is 1
          aggregate: T is count()
        }
        -> {
          PROJECT: T1 is T+1
        }
      }
      query: F-> {
        nest: FUN
      }
      `
    )
    .run();
  // console.log(result.sql);
  // console.log(result.data.toObject());
  expect(result.data.path(0, "FUN", 0, "T1").value).toBe(52);
});

it(`❌ Multi value to udf group by - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      source: F is  table('${schema}.STATE_FACTS') {
        query: FUN is {
          group_by: ONE is 1
          aggregate: T is count()
        }
        -> {
          group_by: T1 is T+1
        }
      }
      query: F-> {
        nest: FUN
      }
      `
    )
    .run();
  // console.log(result.sql);
  // console.log(result.data.toObject());
  expect(result.data.path(0, "FUN", 0, "T1").value).toBe(52);
});

it(`✅ sql_block - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      sql: ONE is ||
        SELECT 1 as A, 2 as B
        UNION ALL SELECT 3, 4
      ;;

      explore: EONE is  from_sql(ONE) {}

      query: EONE -> { project: A }
      `
    )
    .run();
  expect(result.data.value[0].A).toBe(1);
});

it(`✅ sql_block no explore- ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      sql: ONE is ||
        SELECT 1 as A, 2 as B
        UNION ALL SELECT 3, 4
      ;;

      query: from_sql(ONE) -> { project: A }
      `
    )
    .run();
  expect(result.data.value[0].A).toBe(1);
});

// it(`sql_block version- ${databaseName}`, async () => {
//   const result = await runtime
//     .loadQuery(
//       `
//     sql: one is ||
//       select version() as version
//     ;;

//     query: from_sql(one) -> { project: version }
//     `
//     )
//     .run();
//   expect(result.data.value[0].version).toBe("something");
// });

// local declarations
it(`✅ local declarations external query - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      sql: ONE is ||
        SELECT 1 as A, 2 as B
        UNION ALL SELECT 3, 4
      ;;

      query: from_sql(ONE) -> {
        declare: C is A + 1
        project: C
      }
      `
    )
    .run();
  expect(result.data.value[0].C).toBe(2);
});

it(`✅ local declarations named query - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      sql: ONE is ||
        SELECT 1 as A, 2 as B
        UNION ALL SELECT 3, 4
      ;;

      source: FOO is from_sql(ONE) + {
        QUERY: BAR is {
          declare: C is A + 1
          project: C
        }
      }

      QUERY: FOO-> BAR
      `
    )
    .run();
  expect(result.data.value[0].C).toBe(2);
});

it(`✅ local declarations refined named query - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      sql: ONE is ||
        SELECT 1 as A, 2 as B
        UNION ALL SELECT 3, 4
      ;;

      source: FOO is from_sql(ONE) + {
        query: BAR is {
          declare: C is A + 1
          project: C
        }

        query: BAZ is BAR + {
          declare: D is C + 1
          project: D
        }
      }

      query: FOO-> BAZ
      `
    )
    .run();
  expect(result.data.value[0].D).toBe(3);
});

it(`✅ regexp match- ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      sql: ONE is ||
        SELECT 'hello mom' as A, 'cheese tastes good' as B
        UNION ALL SELECT 'lloyd is a bozo', 'michael likes poetry'
      ;;

      query: from_sql(ONE) -> {
        aggregate: LLO is count() {? A ~ r'llo'}
        aggregate: M2 is count() {? A !~ r'bozo'}
      }
      `
    )
    .run();
  expect(result.data.value[0].LLO).toBe(2);
  expect(result.data.value[0].M2).toBe(1);
});

it(`✅ substitution precidence- ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
      sql: ONE is ||
        SELECT 5 as A, 2 as B
        UNION ALL SELECT 3, 4
      ;;

      query: from_sql(ONE) -> {
        declare: C is B + 4
        project: X is A * C
      }
      `
    )
    .run();
  expect(result.data.value[0].X).toBe(30);
});

it(`✅ array unnest 1 -- snowflake`, async () => {
  const loadedQuery = runtime.loadQuery(
    `
        sql: ATITLE is ||
          SELECT
            city,
            ${splitFunction[databaseName]}(city,' ') as WORDS
          FROM ${schema}.AIRCRAFT
        ;;

        source: TITLE is from_sql(ATITLE){}

        query: TITLE ->  {
          where: WORDS.VALUE != null
          group_by: WORDS.VALUE
          aggregate: C is count()
        }
      `
  );
  const result = await loadedQuery.run();
  console.log(result.sql);
  expect(result.data.value[0].C).toBe(145);
});

// make sure we can count the total number of elements when fanning out.
it(`✅ array unnest x 2 - snowflake`, async () => {
  const result = await runtime
    .loadQuery(
      `
        sql: ATITLE is ||
          SELECT
            CITY,
            ${splitFunction[databaseName]}(city,' ') as WORDS,
            ${splitFunction[databaseName]}(city,'A') as ABREAK
          FROM ${schema}.AIRCRAFT
          where CITY IS NOT null
        ;;

        source: TITLE is from_sql(ATITLE){}

        query: TITLE ->  {
          aggregate:
            B is count()
            C is WORDS.count()
            A is ABREAK.count()
        }
      `
    )
    .run();
  expect(result.data.value[0].B).toBe(3552);
  expect(result.data.value[0].C).toBe(4586);
  expect(result.data.value[0].A).toBe(6601);
});

it(`✅ nest null - ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
        query: table('${schema}.AIRPORTS') -> {
          where: FAA_REGION = null
          group_by: FAA_REGION
          aggregate: AIRPORT_COUNT is count()
          nest: BY_STATE is {
            where: STATE != null
            group_by: STATE
            aggregate: AIRPORT_COUNT is count()
          }
          nest: BY_STATE1 is {
            where: STATE != null
            group_by: STATE
            aggregate: AIRPORT_COUNT is count()
            limit: 1
          }
        }
      `
    )
    .run();

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const d: any = result.data.toObject();
  expect(d[0]["BY_STATE"]).not.toBe(null);
  expect(d[0]["BY_STATE1"]).not.toBe(null);
});

it(`✅ number as null- ${databaseName}`, async () => {
  const result = await runtime
    .loadQuery(
      `
        source: S is table('${schema}.STATE_FACTS') + {
        }
        query: S-> {
          group_by: STATE
          nest: UGLY is {
            group_by: POPULAR_NAME
            aggregate: FOO is NULLIF(sum(AIRPORT_COUNT)*0,0)+1
          }
        }
      `
    )
    .run();
  expect(result.data.path(0, "UGLY", 0, "FOO").value).toBe(null);
});

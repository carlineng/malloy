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

import * as malloy from "@malloydata/malloy";
import { RuntimeList } from "../../runtimes";
import "../../util/is-sql-eq";
import { mkSqlEqWith } from "../../util";

const runtimes = new RuntimeList(["snowflake"]);

const expressionModelText = `
explore: AIRCRAFT_MODELS is table('TEST.AIRCRAFT_MODELS'){
  primary_key: AIRCRAFT_MODEL_CODE
  measure:
    AIRPORT_COUNT is count(*),
    AIRCRAFT_MODEL_COUNT is count(),
    TOTAL_SEATS is sum(SEATS),
    BOEING_SEATS is sum(SEATS) {? MANUFACTURER ? 'BOEING'},
    PERCENT_BOEING is BOEING_SEATS / TOTAL_SEATS * 100,
    PERCENT_BOEING_FLOOR is FLOOR(BOEING_SEATS / TOTAL_SEATS * 100),
  dimension: SEATS_BUCKETED is FLOOR(SEATS/20)*20.0
}

explore: AIRCRAFT is table('TEST.AIRCRAFT'){
  primary_key: TAIL_NUM
  join_one: AIRCRAFT_MODELS with AIRCRAFT_MODEL_CODE
  measure: AIRCRAFT_COUNT is count(*)
  query: BY_MANUFACTURER is {
    top: 5
    group_by: AIRCRAFT_MODELS.MANUFACTURER
    aggregate: AIRCRAFT_COUNT
  }
}
`;

const expressionModels = new Map<string, malloy.ModelMaterializer>();
runtimes.runtimeMap.forEach((runtime, databaseName) =>
  expressionModels.set(databaseName, runtime.loadModel(expressionModelText))
);

expressionModels.forEach((expressionModel, databaseName) => {
  // basic calculations for sum, filtered sum, without a join.
  it(`✅ basic calculations - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
        query: AIRCRAFT_MODELS->{
          aggregate:
            TOTAL_SEATS,
            TOTAL_SEATS2 is sum(SEATS),
            BOEING_SEATS,
            BOEING_SEATS2 is sum(SEATS) {? MANUFACTURER ? 'BOEING'},
            BOEING_SEATS3 is TOTAL_SEATS {? MANUFACTURER ? 'BOEING'},
            PERCENT_BOEING,
            PERCENT_BOEING2 is BOEING_SEATS / TOTAL_SEATS * 100,
            -- PERCENT_BOEING_FLOOR,
            -- PERCENT_BOEING_FLOOR2 is FLOOR(BOEING_SEATS / TOTAL_SEATS * 100)
        }
        `
      )
      .run();
    expect(result.data.path(0, "TOTAL_SEATS").value).toBe(452415);
    expect(result.data.path(0, "TOTAL_SEATS2").value).toBe(452415);
    expect(result.data.path(0, "BOEING_SEATS").value).toBe(252771);
    expect(result.data.path(0, "BOEING_SEATS2").value).toBe(252771);
    expect(result.data.path(0, "BOEING_SEATS3").value).toBe(252771);
    expect(Math.floor(result.data.path(0, "PERCENT_BOEING").number.value)).toBe(
      55
    );
    expect(
      Math.floor(result.data.path(0, "PERCENT_BOEING2").number.value)
    ).toBe(55);
    // expect(result.data.path(0, "percent_boeing_floor").value).toBe(55);
    // expect(result.data.path(0, "percent_boeing_floor2").value).toBe(55);
  });
  // Floor is broken (doesn't compile because the expression returned isn't an aggregate.)
  it(`✅ Floor() -or any function bustage with aggregates - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
        query: AIRCRAFT_MODELS->{
          aggregate:
            PERCENT_BOEING_FLOOR
            PERCENT_BOEING_FLOOR2 is FLOOR(BOEING_SEATS / TOTAL_SEATS * 100)
        }
      `
      )
      .run();
    expect(result.data.path(0, "PERCENT_BOEING_FLOOR").value).toBe(55);
    expect(result.data.path(0, "PERCENT_BOEING_FLOOR2").value).toBe(55);
  });

  // Model based version of sums.
  it(`✅ model: expression fixups. - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
            query: AIRCRAFT->{
              aggregate:
                AIRCRAFT_MODELS.TOTAL_SEATS
                AIRCRAFT_MODELS.BOEING_SEATS
            }
          `
      )
      .run();
    expect(result.data.path(0, "TOTAL_SEATS").value).toBe(18294);
    expect(result.data.path(0, "BOEING_SEATS").value).toBe(6244);
  });

  // turtle expressions
  it(`✅ model: turtle - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
          query: AIRCRAFT->BY_MANUFACTURER
          `
      )
      .run();
    expect(result.data.path(0, "MANUFACTURER").value).toBe("CESSNA");
  });

  // filtered turtle expressions
  it(`✅ model: filtered turtle - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
          query: AIRCRAFT->{
            nest: B is BY_MANUFACTURER{? AIRCRAFT_MODELS.MANUFACTURER ?~'B%'}
          }
        `
      )
      .run();
    expect(result.data.path(0, "B", 0, "MANUFACTURER").value).toBe("BEECH");
  });

  // having.
  it(`✅ model: simple having - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
          query: AIRCRAFT->{
            having: AIRCRAFT_COUNT >90
            group_by: STATE
            aggregate: AIRCRAFT_COUNT
            order_by: 2
          }
          `
      )
      .run();
    expect(result.data.path(0, "AIRCRAFT_COUNT").value).toBe(91);
  });

  it(`✅ model: turtle having2 - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
      -- hacking a null test for now
      query: AIRCRAFT->{
        top: 10
        order_by: 1
        where: REGION != NULL
        group_by: REGION
        nest: BY_STATE is {
          top: 10
          order_by: 1 desc
          having: AIRCRAFT_COUNT > 50
          group_by: STATE
          aggregate: AIRCRAFT_COUNT
        }
      }
        `
      )
      .run();
    expect(result.data.path(0, "BY_STATE", 0, "STATE").value).toBe("VA");
  });

  it(`✅ model: turtle having on main - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
      query: AIRCRAFT->{
        order_by: 2 asc
        having: AIRCRAFT_COUNT ? >500
        group_by: REGION
        aggregate: AIRCRAFT_COUNT
        nest: BY_STATE is {
          order_by: 2 asc
          having: AIRCRAFT_COUNT >45
          group_by: STATE
          aggregate: AIRCRAFT_COUNT
          nest: BY_CITY is {
            order_by: 2 asc
            having: AIRCRAFT_COUNT ? >5
            group_by: CITY
            aggregate: AIRCRAFT_COUNT
          }
        }
      }
        `
      )
      .run();
    expect(result.data.path(0, "BY_STATE", 0, "BY_CITY", 0, "CITY").value).toBe(
      "ALBUQUERQUE"
    );
  });

  // bigquery doesn't like to partition by floats,
  it(`✅ model: having float group by partition - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
      query: AIRCRAFT_MODELS->{
        order_by: 1
        having: SEATS_BUCKETED > 0, AIRCRAFT_MODEL_COUNT > 400
        group_by: SEATS_BUCKETED
        aggregate: AIRCRAFT_MODEL_COUNT
        nest: FOO is {
          group_by: ENGINES
          aggregate: AIRCRAFT_MODEL_COUNT
        }
      }
      `
      )
      .run();
    expect(result.data.path(0, "AIRCRAFT_MODEL_COUNT").value).toBe(448);
  });

  it(`✅ model: aggregate functions distinct min max - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
        query: AIRCRAFT_MODELS->{
          aggregate:
            DISTINCT_SEATS is count(DISTINCT SEATS),
            BOEING_DISTINCT_SEATS is count(DISTINCT SEATS) {?MANUFACTURER ? 'BOEING'},
            MIN_SEATS is min(SEATS),
            CESSNA_MIN_SEATS is min(SEATS) {? MANUFACTURER ? 'CESSNA'},
            MAX_SEATS is max(SEATS),
            CESSNA_MAX_SEATS is max(SEATS) {? MANUFACTURER ? 'CESSNA'},
            MIN_CODE is min(AIRCRAFT_MODEL_CODE),
            BOEING_MIN_MODEL is min(MODEL) {? MANUFACTURER ? 'BOEING'},
            MAX_MODEL is max(MODEL),
            BOEING_MAX_MODEL is max(MODEL) {? MANUFACTURER ? 'BOEING'},
        }
        `
      )
      .run();
    expect(result.data.path(0, "DISTINCT_SEATS").value).toBe(187);
    expect(result.data.path(0, "BOEING_DISTINCT_SEATS").value).toBe(85);
    expect(result.data.path(0, "MIN_SEATS").value).toBe(0);
    expect(result.data.path(0, "CESSNA_MIN_SEATS").value).toBe(1);
    expect(result.data.path(0, "MAX_SEATS").value).toBe(660);
    expect(result.data.path(0, "MIN_CODE").value).toBe("0030109");
    expect(result.data.path(0, "CESSNA_MAX_SEATS").value).toBe(14);
    expect(result.data.path(0, "BOEING_MIN_MODEL").value).toBe("100");
    expect(result.data.path(0, "MAX_MODEL").value).toBe("ZWEIFEL PA18");
    expect(result.data.path(0, "BOEING_MAX_MODEL").value).toBe("YL-15");
  });

  (databaseName !== "bigquery" ? it.skip : it)(
    `model: dates named - ${databaseName}`,
    async () => {
      const result = await expressionModel
        .loadQuery(
          `
        query: table('malloytest.alltypes')->{
          group_by:
            t_date,
            t_date_month is t_date.month,
            t_date_year is t_date.year,
            t_timestamp,
            t_timestamp_date is t_timestamp.day,
            t_timestamp_hour is t_timestamp.hour,
            t_timestamp_minute is t_timestamp.minute,
            t_timestamp_second is t_timestamp.second,
            t_timestamp_month is t_timestamp.month,
            t_timestamp_year is t_timestamp.year,
        }

        `
        )
        .run();
      expect(result.data.path(0, "t_date").value).toEqual(
        new Date("2020-03-02")
      );
      expect(result.data.path(0, "t_date_month").value).toEqual(
        new Date("2020-03-01")
      );
      expect(result.data.path(0, "t_date_year").value).toEqual(
        new Date("2020-01-01")
      );
      expect(result.data.path(0, "t_timestamp").value).toEqual(
        new Date("2020-03-02T12:35:56.000Z")
      );
      expect(result.data.path(0, "t_timestamp_second").value).toEqual(
        new Date("2020-03-02T12:35:56.000Z")
      );
      expect(result.data.path(0, "t_timestamp_minute").value).toEqual(
        new Date("2020-03-02T12:35:00.000Z")
      );
      expect(result.data.path(0, "t_timestamp_hour").value).toEqual(
        new Date("2020-03-02T12:00:00.000Z")
      );
      expect(result.data.path(0, "t_timestamp_date").value).toEqual(
        new Date("2020-03-02")
      );
      expect(result.data.path(0, "t_timestamp_month").value).toEqual(
        new Date("2020-03-01")
      );
      expect(result.data.path(0, "t_timestamp_year").value).toEqual(
        new Date("2020-01-01")
      );
    }
  );

  it.skip("defines in model", async () => {
    // const result1 = await model.makeQuery(`
    //   define a is ('malloytest.alltypes');
    //   explore a | reduce x is count(*)
    //   `);
    // const result = await model.makeQuery(`
    //     define a is ('malloytest.alltypes');
    //     explore a | reduce x is count(*)
    //     `);
  });

  it(`✅ named query metadata undefined - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
        query: AIRCRAFT->{
          aggregate: AIRCRAFT_COUNT is count()
        }
        `
      )
      .run();
    // TODO The result explore should really be unnamed. This test currently
    //      inspects inner information because we have no way to have unnamed
    //       explores today.
    // expect(result.getResultExplore().name).toBe(undefined);
    expect(result._queryResult.queryName).toBe(undefined);
  });

  it(`✅ named query metadata named - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
        query: AIRCRAFT->BY_MANUFACTURER
        `
      )
      .run();
    expect(result.resultExplore.name).toBe("BY_MANUFACTURER");
  });

  it(`✅ named query metadata named head of pipeline - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
        query: AIRCRAFT->BY_MANUFACTURER->{ aggregate: C is count()}
        `
      )
      .run();
    // TODO Same as above -- this test should check the explore name
    // expect(result.getResultExplore().name).toBe(undefined);
    expect(result._queryResult.queryName).toBe(undefined);
  });

  it(`✅ filtered explores basic - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
        explore: B is AIRCRAFT{ where: AIRCRAFT_MODELS.MANUFACTURER ? ~'B%' }

        query: B->{aggregate: M_COUNT is count(distinct AIRCRAFT_MODELS.MANUFACTURER) }
        `
      )
      .run();
    expect(result.data.path(0, "M_COUNT").value).toBe(63);
  });

  it(`✅ query with aliasname used twice - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
        query: AIRCRAFT->{
          group_by: FIRST is substring(CITY,1,1)
          aggregate: AIRCRAFT_COUNT is count()
          nest: AIRCRAFT is {
            group_by: FIRST_TWO is SUBSTRING(CITY,1,2)
            aggregate: AIRCRAFT_COUNT is count()
            nest: AIRCRAFT is {
              group_by: FIRST_THREE is substring(CITY,1,3)
              aggregate: AIRCRAFT_COUNT is count()
            }
          }
        } -> {
          project:
            AIRCRAFT.AIRCRAFT.FIRST_THREE
            AIRCRAFT_COUNT
            order_by: 2 desc, 1
        }
      `
      )
      .run();
    expect(result.data.path(0, "FIRST_THREE").value).toBe("SAB");
  });

  it.skip("✅ join foreign_key reverse", async () => {
    const result = await expressionModel
      .loadQuery(
        `
  explore: A is table('TEST.AIRCRAFT') {
    primary_key: TAIL_NUM
    measure: AIRCRAFT_COUNT is count()
  }
  query: table('TEST.AIRCRAFT_MODELS') {
    primary_key: AIRCRAFT_MODEL_CODE
    join_many: A on A.AIRCRAFT_MODEL_CODE

    SOME_MEASURES is {
      aggregate: AM_COUNT is count()
      aggregate: A.AIRCRAFT_COUNT
    }
  } -> SOME_MEASURE
    `
      )
      .run();
    expect(result.data.path(0, "FIRST_THREE").value).toBe("SAN");
  });

  it(`✅ joined filtered explores - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
    explore: A_MODELS is table('TEST.AIRCRAFT_MODELS'){
      where: MANUFACTURER ? ~'B%'
      primary_key: AIRCRAFT_MODEL_CODE
      measure:MODEL_COUNT is count()
    }

    explore: AIRCRAFT2 is table('TEST.AIRCRAFT'){
      join_one: MODEL is A_MODELS with AIRCRAFT_MODEL_CODE
      measure: AIRCRAFT_COUNT is count()
    }

    query: AIRCRAFT2->{
      aggregate:
        MODEL.MODEL_COUNT
        AIRCRAFT_COUNT
    }
        `
      )
      .run();
    expect(result.data.path(0, "MODEL_COUNT").value).toBe(244);
    expect(result.data.path(0, "AIRCRAFT_COUNT").value).toBe(3599);
  });

  it(`✅ joined filtered explores with dependancies - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
    explore: BO_MODELS is
      from(
          table('TEST.AIRCRAFT_MODELS') {? MANUFACTURER ? ~ 'BO%' }
          -> { project: AIRCRAFT_MODEL_CODE, MANUFACTURER, SEATS }
        ) {
          primary_key: AIRCRAFT_MODEL_CODE
          measure: BO_COUNT is count()
        }

    explore: B_MODELS is
        from(
          table('TEST.AIRCRAFT_MODELS') {? MANUFACTURER ? ~ 'B%' }
          -> { project: AIRCRAFT_MODEL_CODE, MANUFACTURER, SEATS }
        ) {
          where: BO_MODELS.SEATS > 200
          primary_key: AIRCRAFT_MODEL_CODE
          measure: B_COUNT is count()
          join_one: BO_MODELS with AIRCRAFT_MODEL_CODE
        }

    explore: MODELS is table('TEST.AIRCRAFT_MODELS') {
      join_one: B_MODELS with AIRCRAFT_MODEL_CODE
      measure: MODEL_COUNT is count()
    }

    query: MODELS -> {
      aggregate: MODEL_COUNT
      aggregate: B_MODELS.B_COUNT
      -- aggregate: B_MODELS.BO_MODELS.BO_COUNT
    }
        `
      )
      .run();
    expect(result.data.path(0, "MODEL_COUNT").value).toBe(60461);
    expect(result.data.path(0, "B_COUNT").value).toBe(355);
  });

  it(`🟡 group by explore - simple group by - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
        query: AIRCRAFT->{
          group_by: AIRCRAFT_MODELS
          aggregate: AIRCRAFT_COUNT
        }
    `
      )
      .run();
    expect(result.data.path(0, "AIRCRAFT_COUNT").value).toBe(58);
    // TODO: Hmm, this is a weird one... `id` is appended in lowercase.
    expect(result.data.path(0, "AIRCRAFT_MODELS_id").value).toBe("7102802");
  });

  it(`✅ group by explore - pipeline - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
        query: AIRCRAFT->{
          group_by: AIRCRAFT_MODELS
          aggregate: AIRCRAFT_COUNT
        } -> {
          group_by: AIRCRAFT_MODELS.MANUFACTURER
          aggregate: AIRCRAFT_COUNT is AIRCRAFT_COUNT.sum()
        }
    `
      )
      .run();
    expect(result.data.path(0, "AIRCRAFT_COUNT").value).toBe(1048);
    expect(result.data.path(0, "MANUFACTURER").value).toBe("CESSNA");
  });

  it(`✅ group by explore - pipeline 2 levels - ${databaseName}`, async () => {
    const result = await expressionModel
      .loadQuery(
        `
      explore: F is table('TEST.FLIGHTS'){
        join_one: A is table('TEST.AIRCRAFT') {
          join_one: STATE_FACTS is table('TEST.STATE_FACTS'){primary_key: STATE} with STATE
        } on TAIL_NUM = A.TAIL_NUM
      }

      query: F-> {
        group_by: A.STATE_FACTS
        aggregate: FLIGHT_COUNT is count()
      } -> {
        group_by: STATE_FACTS.POPULAR_NAME
        aggregate: FLIGHT_COUNT is FLIGHT_COUNT.sum()
      }
    `
      )
      .run();
    // console.log(result.data.toObject());
    expect(result.data.path(0, "FLIGHT_COUNT").value).toBe(199726);
    expect(result.data.path(0, "POPULAR_NAME").value).toBe("Isabella");
  });
});

runtimes.runtimeMap.forEach((runtime, databaseName) => {
  const sqlEq = mkSqlEqWith(runtime, {
    malloy: `+ {
      dimension: friName is 'friday'
      dimension: friDay is 5
      dimension: satName is 'saturday'
      dimension: satDay is 6
    }`,
  });

  describe.skip(`alternations with not-eq - ${databaseName}`, () => {
    /*
     Here's the desired truth table ...

     x      x != y | z
     ====== ============
     y      false
     z      false
     ^[yz]  true
     */
    test("x not-eq y or z : x eq y", async () => {
      const result = await sqlEq("6 != (6|7)", false);
      expect(result).isSqlEq();
    });
    test("x not-eq y or z : x eq z", async () => {
      const result = await sqlEq("7 != (6|7)", false);
      expect(result).isSqlEq();
    });
    test("x not-eq y or z : else", async () => {
      const result = await sqlEq("5 != (6|7)", true);
      expect(result).isSqlEq();
    });
    /*
      Writing this the old way, should have the same truth table ...
        x != y & != z
    */
    test("x not-eq y and not-eq z : x eq y", async () => {
      const result = await sqlEq("6 != (6 & !=7)", false);
      expect(result).isSqlEq();
    });
    test("x not-eq y and not-eq z : x eq z", async () => {
      const result = await sqlEq("7 != (6 & != 7)", false);
      expect(result).isSqlEq();
    });
    test("x not-eq y and not-eq z : else", async () => {
      const result = await sqlEq("5 != (6 & !=7)", true);
      expect(result).isSqlEq();
    });
  });

  describe(`interval extraction - ${databaseName}`, () => {
    const sqlEq = mkSqlEqWith(runtime);

    test("✅ seconds", async () => {
      expect(await sqlEq("seconds(now to now + 1 second)", 1)).isSqlEq();
      expect(await sqlEq("seconds(now to now)", 0)).isSqlEq();
      expect(await sqlEq("seconds(now to now + 2 seconds)", 2)).isSqlEq();
      expect(await sqlEq("seconds(now to now - 2 seconds)", -2)).isSqlEq();
    });

    test("❌ minutes", async () => {
      expect(
        await sqlEq("minutes(@2022-10-03 10:23:08 to @2022-10-03 10:24:07)", 0)
      ).isSqlEq();

      expect(await sqlEq("minutes(now to now + 1 minute)", 1)).isSqlEq();
      expect(await sqlEq("minutes(now to now + 59 seconds)", 0)).isSqlEq();
      expect(await sqlEq("minutes(now to now + 2 minutes)", 2)).isSqlEq();
      expect(await sqlEq("minutes(now to now - 2 minutes)", -2)).isSqlEq();
    });

    test("❌ hours", async () => {
      expect(
        await sqlEq("hours(@2022-10-03 10:23:00 to @2022-10-03 11:22:00)", 0)
      ).isSqlEq();
      expect(await sqlEq("hours(now to now + 1 hour)", 1)).isSqlEq();
      expect(await sqlEq("hours(now to now + 59 minutes)", 0)).isSqlEq();
      expect(await sqlEq("hours(now to now + 120 minutes)", 2)).isSqlEq();
      expect(await sqlEq("hours(now to now - 2 hours)", -2)).isSqlEq();
    });

    test("✅ days", async () => {
      expect(await sqlEq("days(now.day to now.day + 1 day)", 1)).isSqlEq();
      expect(await sqlEq("days(now.day to now.day + 23 hours)", 0)).isSqlEq();
      expect(await sqlEq("days(now.day to now.day + 48 hours)", 2)).isSqlEq();
      expect(await sqlEq("days(now.day to now.day - 48 hours)", -2)).isSqlEq();

      expect(
        await sqlEq("days(@2022-10-03 10:23:00 to @2022-10-04 09:23:00)", 1)
      ).isSqlEq();
    });

    test("❌ weeks", async () => {
      expect(await sqlEq("weeks(now.week to now.week + 1 week)", 1)).isSqlEq();
      expect(await sqlEq("weeks(now.week to now.week + 6 days)", 0)).isSqlEq();
      expect(await sqlEq("weeks(now.week to now.week + 14 days)", 2)).isSqlEq();
      expect(
        await sqlEq("weeks(now.week to now.week - 14 days)", -2)
      ).isSqlEq();
      expect(await sqlEq("weeks(@2022-10-03 to @2022-10-10)", 1)).isSqlEq();
      expect(await sqlEq("weeks(@2022-10-03 to @2022-10-09)", 1)).isSqlEq();
      expect(await sqlEq("weeks(@2022-10-02 to @2022-10-08)", 0)).isSqlEq();
      expect(await sqlEq("weeks(@2022-10-02 to @2023-10-02)", 52)).isSqlEq();

      expect(
        await sqlEq("weeks(@2022-10-02 10:00 to @2023-10-02 10:00)", 52)
      ).isSqlEq();
    });

    test("✅ months", async () => {
      expect(await sqlEq("months(now to now + 1 month)", 1)).isSqlEq();
      expect(
        await sqlEq("months(now.month to now.month + 27 days)", 0)
      ).isSqlEq();
      expect(await sqlEq("months(now to now + 2 months)", 2)).isSqlEq();
      expect(await sqlEq("months(now to now - 2 months)", -2)).isSqlEq();

      expect(
        await sqlEq("months(@2022-10-02 10:00 to @2022-11-02 09:00)", 1)
      ).isSqlEq();
    });

    test("✅ quarters", async () => {
      expect(await sqlEq("quarters(@2022-03-31 to @2022-04-01)", 1)).isSqlEq();
      expect(await sqlEq("quarters(now to now + 1 quarter)", 1)).isSqlEq();
      expect(
        await sqlEq("quarters(now.quarter to now.quarter + 27 days)", 0)
      ).isSqlEq();
      expect(await sqlEq("quarters(now to now + 2 quarters)", 2)).isSqlEq();
      expect(await sqlEq("quarters(now to now - 2 quarters)", -2)).isSqlEq();

      expect(
        await sqlEq("quarters(@2022-10-02 10:00 to @2023-04-02 09:00)", 2)
      ).isSqlEq();
    });

    test("✅ years", async () => {
      expect(await sqlEq("years(@2022 to @2023)", 1)).isSqlEq();
      expect(await sqlEq("years(@2022-01-01 to @2022-12-31)", 0)).isSqlEq();
      expect(await sqlEq("years(@2022 to @2024)", 2)).isSqlEq();
      expect(await sqlEq("years(@2024 to @2022)", -2)).isSqlEq();
      expect(
        await sqlEq("years(@2022-01-01 10:00 to @2024-01-01 09:00)", 2)
      ).isSqlEq();
    });
  });
});

afterAll(async () => {
  await runtimes.closeAll();
});

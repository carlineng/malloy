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
/* eslint-disable no-console */

import "../../util/is-sql-eq";
import { RuntimeList } from "../../runtimes";
import { describeIfDatabaseAvailable } from "../../util";

// No prebuilt shared model, each test is complete.  Makes debugging easier.

const [describe, databases] = describeIfDatabaseAvailable([
  "bigquery",
  "duckdb",
]);

const modelText = `
source:ga_sessions is table('malloytest.ga_sample'){

  measure:
    user_count is count(distinct fullVisitorId)
    session_count is count()
    total_visits is totals.visits.sum()
    total_hits is totals.hits.sum()
    total_page_views is totals.pageviews.sum()
    t2 is totals.pageviews.sum()
    total_productRevenue is hits.product.productRevenue.sum()
    hits_count is hits.count()
    sold_count is hits.count() { where: hits.product.productQuantity > 0 }

  query: by_source is {
    where: trafficSource.source != '(direct)'
    group_by: trafficSource.source
    aggregate: hits_count
    limit: 10
  }
  query: by_adContent_bar_chart is {
    group_by: device.browser
    aggregate: user_count
    group_by: device.deviceCategory
  }
  query: by_region is {
    where: geoNetwork.region !~ '%demo%'
    group_by: geoNetwork.region
    aggregate: user_count
    limit: 10
  }
  query: by_device is {
    group_by: device.browser
    aggregate: user_count
    group_by: device.deviceCategory
  }
  query: by_category is {
    group_by: category is hits.product.v2ProductCategory
    aggregate: total_productRevenue
    aggregate: sold_count
    limit: 10
  }
  query: by_hour_of_day is {
    // group_by: gsession_hour is hour(start_time::timestamp)
    aggregate: session_count
    order_by: 1
  }

  query: page_load_times is {
    group_by: hits.page.pageTitle
    aggregate: hit_count is hits.count()
    nest: load_bar_chart is {
      group_by: hit_seconds is floor(hits.latencyTracking.pageLoadTime / 2) * 2
      aggregate: hits_count
    }
    limit: 10
  }

  query: by_page_title is { where: totals.transactionRevenue > 0
    group_by: hits.page.pageTitle
    aggregate: hits_count
    nest: sold_count
  }

  query: by_all is {
    nest: by_source
    nest: by_adContent_bar_chart
    nest: by_region
    nest: by_category
  }

  query: search_index is {
    index: *, hits.*, customDimensions.* totals.*, trafficSource.*, hits.product.*
    sample: 1%
  }
}

query: sessions_dashboard is ga_sessions -> {
  nest:
    by_region
    by_device
    by_source
    by_category is {
      group_by: category is hits.product.v2ProductCategory
      aggregate: total_productRevenue
      aggregate: sold_count
    }
}
`;

describe("Nested Source Table", () => {
  const runtimes = new RuntimeList(databases);

  afterAll(async () => {
    await runtimes.closeAll();
  });

  runtimes.runtimeMap.forEach((runtime, databaseName) => {
    const model = runtime.loadModel(modelText);

    test(`repeated child of record - ${databaseName}`, async () => {
      const result = await model
        .loadQuery(
          `
        query: ga_sessions->by_page_title
        `
        )
        .run();
      // console.log(result.data.toObject());
      // console.log(result.sql);
      expect(result.data.path(0, "pageTitle").value).toBe("Shopping Cart");
    });

    test(`search_index - ${databaseName}`, async () => {
      const result = await model
        .loadQuery(
          `
        query: ga_sessions->search_index -> {
          where: fieldName != null
          project: *
          order_by: fieldName, weight desc
          limit: 10
        }
        `
        )
        .run();
      console.log(result.data.toObject());
      expect(result.data.path(0, "fieldName").value).toBe("channelGrouping");
      expect(result.data.path(0, "fieldValue").value).toBe("Organic Search");
      // expect(result.data.path(0, "weight").value).toBe(18);
    });
  });
});
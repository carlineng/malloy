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

// Copy/Pasta'd from Duckdb Dialect

import {
  DateUnit,
  Expr,
  ExtractUnit,
  getIdentifier,
  isSamplingEnable,
  isSamplingPercent,
  isSamplingRows,
  mkExpr,
  Sampling,
  StructDef,
  TimeFieldType,
  TimestampUnit,
  TimeValue,
  TypecastFragment,
} from "../model";
import { indent } from "../model/utils";
import { Dialect, DialectFieldList, FunctionInfo } from "./dialect";

const sourceStageName = "<SOURCE_STAGE_NAME>";

// need to refactor runSQL to take a SQLBlock instead of just a sql string.
const hackSplitComment = "-- hack: split on this";

const keywords = `
ALL
ANALYSE
ANALYZE
AND
ANY
ARRAY
AS
ASC_P
ASYMMETRIC
BOTH
CASE
CAST
CHECK_P
COLLATE
COLUMN
CONSTRAINT
CREATE_P
CURRENT_CATALOG
CURRENT_DATE
CURRENT_ROLE
CURRENT_TIME
CURRENT_TIMESTAMP
CURRENT_USER
DEFAULT
DEFERRABLE
DESC_P
DISTINCT
DO
ELSE
END_P
EXCEPT
FALSE_P
FETCH
FOR
FOREIGN
FROM
GRANT
GROUP_P
HAVING
IN_P
INITIALLY
INTERSECT
INTO
LATERAL_P
LEADING
LIMIT
LOCALTIME
LOCALTIMESTAMP
NOT
NULL_P
OFFSET
ON
ONLY
OR
ORDER
PLACING
PRIMARY
REFERENCES
RETURNING
SELECT
SESSION_USER
SOME
SYMMETRIC
TABLE
THEN
TO
TRAILING
TRUE_P
UNION
UNIQUE
USER
USING
VARIADIC
WHEN
WHERE
WINDOW
WITH
`.split(/\s/);

const castMap: Record<string, string> = {
  number: "float",
  string: "varchar",
};

const pgExtractionMap: Record<string, string> = {
  day_of_week: "dow",
  day_of_year: "doy",
};

const inSeconds: Record<string, number> = {
  second: 1,
  minute: 60,
  hour: 3600,
};

export class SnowflakeDialect extends Dialect {
  name = "snowflake";
  defaultNumberType = "FLOAT";
  hasFinalStage = false;
  stringTypeName = "TEXT";
  divisionIsInteger = false;
  supportsSumDistinctFunction = true;
  unnestWithNumbers = true;
  defaultSampling = { rows: 50000 };
  supportUnnestArrayAgg = false;
  supportsCTEinCoorelatedSubQueries = false;

  functionInfo: Record<string, FunctionInfo> = {
    concat: { returnType: "string" },
  };

  // hack until they support temporary macros.
  get udfPrefix(): string {
    return `__udf${Math.floor(Math.random() * 100000)}`;
  }

  quoteTablePath(tableName: string): string {
    return tableName.match(/\//) ? `'${tableName}'` : tableName;
  }

  sqlGroupSetTable(groupSetCount: number): string {
    return `CROSS JOIN (
      SELECT
        ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS group_set
      FROM table(generator(rowcount => ${groupSetCount}+1))
    ) group_set`;
  }

  sqlAnyValue(groupSet: number, fieldName: string): string {
    return `ANY_VALUE(${fieldName})`;
  }

  mapFields(fieldList: DialectFieldList): string {
    return fieldList.join(", ");
  }

  sqlAggregateTurtle(
    groupSet: number,
    fieldList: DialectFieldList,
    orderBy: string | undefined,
    limit: number | undefined
  ): string {
    const fields = fieldList
      .map((f) => {
        const unquotedFieldName = f.sqlOutputName.replace(/"/g, "'");
        return `${unquotedFieldName}, ${f.sqlExpression}`;
      })
      .join(", ");

    const objectConstruct = `
    CASE
      WHEN group_set = ${groupSet} THEN OBJECT_CONSTRUCT_KEEP_NULL(${fields})
      ELSE NULL
    END`;

    let arrayAgg = `
    ARRAY_AGG( ${objectConstruct} ) WITHIN GROUP (${orderBy})
    `;

    if (limit !== undefined) {
      arrayAgg = `ARRAY_SLICE(${arrayAgg}, 0, ${limit} - 1)`;
    }

    const finalSql = `
    COALESCE(
      ${arrayAgg},
      []
    )`;

    return finalSql;
  }

  sqlAnyValueTurtle(groupSet: number, fieldList: DialectFieldList): string {
    const fields = fieldList
      .map((f) => `${f.sqlExpression}, ${f.sqlOutputName}`)
      .join(", ");
    return `ANY_VALUE(CASE WHEN group_set=${groupSet} THEN OBJECT_AGG(${fields}))`;
  }

  sqlAnyValueLastTurtle(
    name: string,
    groupSet: number,
    sqlName: string
  ): string {
    return `MAX(CASE WHEN group_set=${groupSet} THEN "${name}__${groupSet}" END) as ${sqlName}`;
  }

  // I think this is equivalent?
  // https://docs.snowflake.com/en/sql-reference/functions/object_construct.html
  sqlCoaleseMeasuresInline(
    groupSet: number,
    fieldList: DialectFieldList
  ): string {
    const fields = fieldList
      .map(
        (f) => `
      CASE WHEN group_set=${groupSet} THEN ${f.sqlOutputName.replace(
          /"/g,
          "'"
        )} ELSE NULL END,
      CASE WHEN group_set=${groupSet} THEN ${f.sqlExpression} ELSE NULL END`
      )
      .join(",");

    const nullValues = fieldList
      .map((f) => `${f.sqlOutputName.replace(/"/g, "'")},NULL`)
      .join(",");

    return `COALESCE(
        OBJECT_AGG(${fields})
        , OBJECT_CONSTRUCT_KEEP_NULL(${nullValues})
      )`;
  }

  // horrible hack to get around casing issues --
  // add both "__row_id" and "__ROW_ID" to this table
  sqlUnnestAlias(
    source: string,
    alias: string,
    _fieldList: DialectFieldList,
    _needDistinctKey: boolean
  ): string {
    return `LEFT JOIN (
      SELECT
        ROW_NUMBER() OVER (ORDER BY SEQ4()) AS "__row_id"
        , "__row_id" AS __ROW_ID
      FROM table(generator(rowcount => 100000))
      ) as ${alias} ON  ${alias}."__row_id" <= ARRAY_SIZE(${source})`;
  }

  // Snowflake looks like it supports SUM(DISTINCT x)
  sqlSumDistinctHashedKey(_sqlDistinctKey: string): string {
    return "uses sumDistinctFunction, should not be called";
  }

  sqlGenerateUUID(): string {
    return `UUID_STRING()`;
  }

  sqlDateToString(sqlDateExp: string): string {
    return `(${sqlDateExp})::date::varchar`;
  }

  sqlFieldReference(
    alias: string,
    fieldName: string,
    _fieldType: string,
    _isNested: boolean,
    isArray: boolean
  ): string {
    if (isArray) {
      return alias;
    } else if (_isNested) {
      // TODO: this doesn't work properly.
      // In Snowflake, a nested semi-structured object must be accessed via `:`
      // However, it looks like `_isNested` is also true when a table is nested via a JOIN.
      // In the JOIN case, we need to use `.`
      return `${alias}:${this.sqlMaybeQuoteIdentifier(fieldName)}`;
    } else {
      return `${alias}.${this.sqlMaybeQuoteIdentifier(fieldName)}`;
    }
  }

  sqlUnnestPipelineHead(
    isSingleton: boolean,
    sourceSQLExpression: string
  ): string {
    let p = sourceSQLExpression;
    if (isSingleton) {
      p = `ARRAY_AGG(${p})`;
    }

    return `(
      SELECT
        value as base
      FROM TABLE(FLATTEN(input => SELECT ${p} FROM ${sourceStageName} ))
    )`;
  }

  sqlPipelinedStage(pipelinesSQL: string, lastStageName: string): string {
    // TODO: CE: not sure if this actually is the right thing to do.
    // When `sqlUnnestPipelineHead` is called, we don't yet have access to `lastStageName`,
    // so I'm just doing a string replacement here to fill in CTE name.
    return `
    SELECT
    ${pipelinesSQL.replace(sourceStageName, lastStageName)}
    `;
  }

  sqlCreateFunction(id: string, funcText: string): string {
    throw new Error("Not implemented Yet");
  }

  sqlCreateFunctionCombineLastStage(
    lastStageName: string,
    structDef: StructDef
  ): string {
    return `SELECT ARRAY_AGG(OBJECT_CONSTRUCT(${structDef.fields
      .map(
        (fieldDef) =>
          `${this.sqlMaybeQuoteIdentifier(getIdentifier(fieldDef)).replace(
            /"/g,
            "'"
          )}, ${this.sqlMaybeQuoteIdentifier(getIdentifier(fieldDef))}`
      )
      .join(",")})) FROM ${lastStageName}\n`;
  }

  sqlSelectAliasAsStruct(alias: string, physicalFieldNames: string[]): string {
    return `OBJECT_CONSTRUCT(${physicalFieldNames
      .map((name) => `${name.replace(/"/g, "'")}, ${alias}.${name}`)
      .join(", ")})`;
  }

  sqlMaybeQuoteIdentifier(identifier: string): string {
    return '"' + identifier + '"';
  }

  // The simple way to do this is to add a comment on the table
  //  with the expiration time. https://docs.snowflake.com/en/sql-reference/sql/create-table.html
  //  and have a reaper that read comments.
  // Looks like this is used for PDTs/in-warehouse caching
  sqlCreateTableAsSelect(_tableName: string, _sql: string): string {
    throw new Error("Not implemented Yet");
  }

  getFunctionInfo(functionName: string): FunctionInfo | undefined {
    return this.functionInfo[functionName];
  }

  sqlMeasureTime(from: TimeValue, to: TimeValue, units: string): Expr {
    let lVal = from.value;
    let rVal = to.value;
    let diffUsing = "TIMESTAMPDIFF";

    if (units == "second" || units == "minute" || units == "hour") {
      if (from.valueType != "timestamp") {
        lVal = mkExpr`TIMESTAMP(${lVal})`;
      }
      if (to.valueType != "timestamp") {
        rVal = mkExpr`TIMESTAMP(${rVal})`;
      }
      const durationInSeconds = mkExpr`TIMESTAMPDIFF('seconds', ${lVal}, ${rVal})`;
      return mkExpr`FLOOR(${durationInSeconds} / ${inSeconds[
        units
      ].toString()})`;
    } else if (units == "week") {
      diffUsing = "DATEDIFF";
      lVal = mkExpr`DATE(${lVal}) + INTERVAL '1 DAY'`;
      rVal = mkExpr`DATE(${rVal}) + INTERVAL '1 DAY'`;
    } else {
      diffUsing = "DATEDIFF";
      if (from.valueType != "date") {
        lVal = mkExpr`DATE(${lVal})`;
      }
      if (to.valueType != "date") {
        rVal = mkExpr`DATE(${rVal})`;
      }
    }
    return mkExpr`${diffUsing}(${units}, ${lVal}, ${rVal})`;
  }

  sqlNow(): Expr {
    return mkExpr`CURRENT_TIMESTAMP`;
  }

  sqlTrunc(sqlTime: TimeValue, units: TimestampUnit): Expr {
    // adjusting for monday/sunday weeks
    const week = units == "week";
    const truncThis = week
      ? mkExpr`${sqlTime.value}+interval '1 day'`
      : sqlTime.value;
    const trunced = mkExpr`DATE_TRUNC('${units}', ${truncThis})`;
    return week ? mkExpr`(${trunced}-interval '1 day')` : trunced;
  }

  sqlExtract(from: TimeValue, units: ExtractUnit): Expr {
    const pgUnits = pgExtractionMap[units] || units;
    const extracted = mkExpr`EXTRACT(${pgUnits} FROM ${from.value})`;
    return units == "day_of_week" ? mkExpr`(${extracted}+1)` : extracted;
  }

  sqlAlterTime(
    op: "+" | "-",
    expr: TimeValue,
    n: Expr,
    timeframe: DateUnit
  ): Expr {
    const interval = mkExpr`INTERVAL '${n} ${timeframe}'`;
    return mkExpr`((${expr.value})${op}${interval})`;
  }

  sqlCast(cast: TypecastFragment): Expr {
    if (cast.dstType !== cast.srcType) {
      const castTo = castMap[cast.dstType] || cast.dstType;
      return mkExpr`cast(${cast.expr} as ${castTo})`;
    }
    return cast.expr;
  }

  sqlRegexpMatch(expr: Expr, regexp: string): Expr {
    return mkExpr`IFF(REGEXP_INSTR(${expr}, ${regexp}) > 0, true, false)`;
  }

  sqlLiteralTime(
    timeString: string,
    type: TimeFieldType,
    _timezone: string
  ): string {
    if (type == "date") {
      return `DATE('${timeString}')`;
    } else if (type == "timestamp") {
      return `TIMESTAMP '${timeString}'`;
    } else {
      throw new Error(`Unknown Literal time format ${type}`);
    }
  }

  sqlSumDistinct(key: string, value: string): string {
    const _factor = 32;
    const precision = 0.000001;
    // This might not be sufficient? lower64 of md5 could still have collisions
    const keySQL = `MD5_NUMBER_LOWER64(${key}::varchar)`;
    return `
    (SUM(DISTINCT ${keySQL} + FLOOR(IFNULL(${value},0)/${precision})) -  SUM(DISTINCT ${keySQL}))*${precision}
    `;
  }

  // default to sampling 50K rows.
  sqlSampleTable(tableSQL: string, sample: Sampling | undefined): string {
    if (sample !== undefined) {
      if (isSamplingEnable(sample) && sample.enable) {
        sample = this.defaultSampling;
      }
      if (isSamplingRows(sample)) {
        return `(SELECT * FROM ${tableSQL} SAMPLE (${sample.rows} ROWS))`;
      } else if (isSamplingPercent(sample)) {
        return `(SELECT * FROM ${tableSQL} SAMPLE (${sample.percent}))`;
      }
    }
    return tableSQL;
  }

  sqlOrderBy(orderTerms: string[]): string {
    return `ORDER BY ${orderTerms.map((t) => `${t} NULLS LAST`).join(",")}`;
  }
}

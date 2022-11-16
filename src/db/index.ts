import { createClient } from '@clickhouse/client'
import knex, { Knex } from 'knex'


const { CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE } = process.env

const client = createClient({
  host: CLICKHOUSE_HOST ?? 'http://localhost:8123',
  username: CLICKHOUSE_USER ?? 'default',
  password: CLICKHOUSE_PASSWORD ?? '',
  database: CLICKHOUSE_DATABASE ?? 'VKM'
})

const pg = knex({ client: 'pg' });

export const db = async (builder: (knex: Knex) => Knex.QueryBuilder) => {
  return await client.exec({
    query: builder(pg).toQuery(),
  })
}

export async function dbSelect<T>(builder: (knex: Knex) => Knex.QueryBuilder) {
  const query = builder(pg).toSQL()

  const result = await client.query({ query: query.sql })
  const json = await result.json() as {
    data: T[]
    meta: { name: string, type: string }[]
    rows: number,
    statistics: { elapsed: number, rows_read: number, bytes_read: number }
  }

  return json
}

export async function dbInsert(table: string, data: any[]) {

  await client.insert({
    table: table,
    values: data,
    format: 'JSONEachRow',
  })
}

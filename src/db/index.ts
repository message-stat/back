import { createClient } from '@clickhouse/client'
import knex, { Knex } from 'knex'
import { up } from './up'



const { CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE } = process.env

const client = createClient({
  host: CLICKHOUSE_HOST ?? 'http://localhost:8123',
  username: CLICKHOUSE_USER ?? 'default',
  password: CLICKHOUSE_PASSWORD ?? '',
  database: CLICKHOUSE_DATABASE ?? 'VKM'
})

up.split(';')
  .map(t => t.trim())
  .filter(t => t.length > 0)
  .forEach((t) => client.exec({ query: t, }))

const pg = knex({ client: 'pg' });

export const db = async (builder: (knex: Knex) => Knex.QueryBuilder) => {
  return await client.exec({
    query: builder(pg).toQuery(),
  })
}

export async function dbSelect<T>(builder: (knex: Knex) => Knex.QueryBuilder) {
  const query = builder(pg)

  const result = await client.query({ query: query.toQuery() })
  const json = await result.json() as {
    data: T[]
    meta: { name: string, type: string }[]
    rows: number,
    statistics: { elapsed: number, rows_read: number, bytes_read: number }
  }

  return json
}

export async function dbInsert(table: string, data: any[]) {

  if (data.length === 0) return
  await client.insert({
    table: table,
    values: data,
    format: 'JSONEachRow',
  })
}

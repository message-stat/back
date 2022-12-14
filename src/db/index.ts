import { ClickHouseClient, createClient } from '@clickhouse/client'
import knex, { Knex } from 'knex'
import { up } from './up'



const { CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE } = process.env

const client = createClient({
  host: CLICKHOUSE_HOST ?? 'http://localhost:8123',
  username: CLICKHOUSE_USER ?? 'default',
  password: CLICKHOUSE_PASSWORD ?? '',
  database: CLICKHOUSE_DATABASE ?? 'VKM'
})

export async function connect() {
  let connected = false
  while (!connected) {
    try {
      console.log('Connecting to ClickHouse...')
      await client.query({ query: 'SELECT 1' })
      connected = true
    }
    catch (e) {
      await new Promise(resolve => setTimeout(resolve, 1000))

    }
  }


  up.split(';')
    .map(t => t.trim())
    .filter(t => t.length > 0)
    .forEach((t) => client.exec({ query: t, }))

}

export const pg = knex({ client: 'pg' });

export const db = async (builder: (knex: Knex) => Knex.QueryBuilder) => {
  return await client.exec({
    query: builder(pg).toQuery(),
  })
}

export async function dbSelect<T>(builder: (knex: Knex) => Knex.QueryBuilder) {
  return dbSelectRaw<T>(builder(pg))
}
export async function dbSelectRaw<T>(query: Knex.QueryBuilder) {
  // console.log(query.toQuery());

  const result = await client.query({
    query: query.toQuery(), clickhouse_settings: {
      joined_subquery_requires_alias: 0
    }
  })
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

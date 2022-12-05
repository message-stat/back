import { Router } from "express";
import { Knex, knex } from "knex";
import { dbSelect as select, dbSelectRaw as selectRaw, pg } from "../db";

const router = Router();

router.get('/stats', async (req, res) => {
  const { userId } = req.query

  const result = await Promise.all([
    select<any>(knex => knex('Word').count('*', { as: 'count' })),
    select<any>(knex => knex('Message').count('*', { as: 'count' })),
    select<any>(knex => knex('Message').countDistinct('userId', { as: 'count' })),
    select<any>(knex => knex('system.columns').select(knex.raw('sum(data_uncompressed_bytes) as size')).where({ database: 'VKM' }).groupBy('database')),
  ])

  const server = {
    totalWords: result[0].data[0].count,
    totalMessages: result[1].data[0].count,
    totalUsers: result[2].data[0].count,
    totalSize: result[3].data[0].size / 1024 / 1024,
  }

  let user = {}

  if (userId) {
    const result = await Promise.all([
      select<any>(knex => knex('Word').where({ userId }).count('*', { as: 'count' })),
      select<any>(knex => knex('Message').where({ userId }).count('*', { as: 'count' })),
    ])

    user = {
      totalWords: result[0].data[0].count,
      totalMessages: result[1].data[0].count,
    }
  }


  return res.json({
    server,
    user,
    elapsed: Math.max(...result.map(t => t.statistics.elapsed))
  })
})

router.get('/load/:chart', async (req, res) => {
  const { chart } = req.params
  const { userId } = req.query

  const result = await wordCountByTime({ userId: userId as string })

  return res.json(result)
})

declare type ChartParams = {
  userId?: string,
  timeSeries?: {
    from: string,
    to: string,
  }
}

async function selectProcessed(params: {
  userId?: string,
  process: (knex: Knex) => Knex.QueryBuilder
}) {
  const server = params.process(pg)

  let user = null
  if (params.userId) {
    user = server.clone().where({ userId: params.userId })
  }

  const result = await Promise.all([server, user]
    .filter(t => t)
    .map(t => selectRaw<any>(t!)))

  return {
    server: result[0],
    user: result.length > 1 ? result[1] : null,
  }
}

async function wordCountByTime(params: ChartParams) {
  const groupBy = 'month';
  const grop = `date_trunc('${groupBy}', dateTime)`


  return await selectProcessed({
    userId: params.userId,
    process: (knex) => knex('Word')
      .select(knex.raw(`${grop} as x`)).count('*', { as: 'y' })
      .groupByRaw(grop)
      .orderByRaw(grop)
  })

}

export default router

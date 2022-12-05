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

const chartByName: {
  [key: string]: (params: ChartParams) => any
} = {
  'wordCountByTime': wordCountByTime,
  'wordLengthByTime': wordLengthByTime,
  'wordDistribution': wordDistribution,
}

router.get('/load/:chart', async (req, res) => {
  const { chart } = req.params
  const { userId } = req.query

  if (chart in chartByName) {
    const result = await chartByName[chart]({ userId: userId as string })
    return res.json(result)
  }

  return res.status(404).json({ error: 'Chart not found' })
})

declare type ChartParams = {
  userId?: string,
  timeSeries?: {
    from: string,
    to: string,
  }
}

declare type ChartResult = {
  server: any[],
  user: any[],
  elapsed: number,
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
    elapsed: result.map(t => t.statistics.elapsed).reduce((a, b) => a + b, 0)
  }
}

async function wordCountByTime(params: ChartParams) {
  const groupBy = 'quarter';
  const grop = `date_trunc('${groupBy}', dateTime)`


  return await selectProcessed({
    userId: params.userId,
    process: (knex) => knex('Word')
      .select(knex.raw(`${grop} as x`)).count('*', { as: 'y' })
      .groupByRaw(grop)
      .orderByRaw(grop)
  })

}

async function wordLengthByTime(params: ChartParams) {
  const groupBy = 'quarter';
  const grop = `date_trunc('${groupBy}', dateTime)`


  return await selectProcessed({
    userId: params.userId,
    process: (knex) => knex('Word')
      .select(knex.raw(`${grop} as x, avg(length(text)) as y`))
      .groupByRaw(grop)
      .orderByRaw(grop)
      .having(knex.raw('count(*) > 200'))
  })

}

async function wordDistribution(params: ChartParams) {
  const sample = pg.fromRaw('Word sample 0.2')

  function calc(sampe: Knex.QueryBuilder) {
    const count = sampe.clone().count('*', { as: 'count' }).select({ text: 'stem' }).groupBy('text')

    const sumState = pg.from(count)
      .select('count')
      .select('text')
      .select(pg.raw('sumState(count) as sumState'))
      .groupBy('text').groupBy('count').orderBy('count', 'desc')

    const runningAccum = pg.from(sumState)
      .select('count')
      .select(pg.raw(`runningAccumulate(sumState) / (${sampe.clone().count('*').toQuery()}) as ra`))
      .select(pg.raw('ceil(ra * 100) as x'))

    const res = pg.from(runningAccum).select('x').count('*', { as: 'y' }).groupBy('x').orderBy('x')


    console.log(res.toQuery());

    return selectRaw<{ x: number, y: string }>(res)
  }

  function postProcess(res: { x: number, y: string }[]) {
    return res.map(t => ({ x: t.x, y: Number.parseInt(t.y) }))
  }

  const server = calc(sample)

  if (params.userId) {
    const user = calc(sample.clone().where({ userId: params.userId }))

    const result = await Promise.all([server, user])
    return {
      server: postProcess(result[0].data),
      user: postProcess(result[1].data),
      elapsed: result.map(t => t.statistics.elapsed).reduce((a, b) => a + b, 0)
    }
  }


  const s = await server
  return {
    server: postProcess(s.data),
    elapsed: s.statistics.elapsed
  }
}



export default router

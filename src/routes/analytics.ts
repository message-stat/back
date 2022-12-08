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
  [key: string]: (params: ChartParams & any) => any
} = {
  'wordCountByTime': wordCountByTime,
  'wordLengthByTime': wordLengthByTime,
  'wordDistribution': wordDistribution,
  'messageLengthByTime': messageLengthByTime,
  'messageCountByTime': messageCountByTime,
  'wordLengthDistribution': wordLengthDistribution,
  'topWords': topWords,
  'wordDistributionByTime': wordDistributionByTime
}

router.get('/load/:chart', async (req, res) => {
  const { chart } = req.params
  const { userId } = req.query

  if (chart in chartByName) {
    try {
      const result = await chartByName[chart]({ userId: userId as string, ...req.query })
      return res.json(result)
    }
    catch (e: any) {
      return res.status(500).json({ error: e.message })
    }
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
  server: { x: any, y: any }[],
  user: { x: any, y: any }[] | null,
  elapsed: number,
}

async function selectProcessed(params: {
  userId?: string,
  process: (knex: Knex) => Knex.QueryBuilder
}): Promise<ChartResult> {
  const server = params.process(pg)

  let user = null
  if (params.userId) {
    user = server.clone().where({ userId: params.userId })
  }

  const result = await Promise.all([server, user]
    .filter(t => t)
    .map(t => selectRaw<{ x: any, y: any }>(t!)))

  return {
    server: result[0].data,
    user: result.length > 1 ? result[1].data : null,
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

async function messageCountByTime(params: ChartParams) {
  const groupBy = 'quarter';
  const grop = `date_trunc('${groupBy}', dateTime)`


  return await selectProcessed({
    userId: params.userId,
    process: (knex) => knex('Message')
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
    process: (knex) => knex.fromRaw('Word sample 1000000')
      .select(knex.raw(`${grop} as x, avg(length(text)) as y`))
      .groupByRaw(grop)
      .orderByRaw(grop)
      .having(knex.raw('count(*) > 500'))
  })

}

async function messageLengthByTime(params: ChartParams & { variant: 'word' | 'char' }) {
  const groupBy = 'quarter';
  const grop = `date_trunc('${groupBy}', dateTime)`
  const select = pg.raw('x, avg(y) OVER (ORDER BY x ASC Rows BETWEEN 1 PRECEDING AND CURRENT ROW) as y')

  const from = pg.fromRaw('Message sample 0.5')
    .select(pg.raw(`${grop} as x, avg(${params.variant === 'word' ? 'words' : 'symbols'}) as y`))
    .groupByRaw(grop)
    .orderByRaw(grop)
    .having(pg.raw('count(*) > 200'))

  const server = pg.from(from)
    .select(select)

  const serverRes = await selectRaw<{ x: number, y: string }>(server)

  let user = null

  if (params.userId) {
    user = await selectRaw<{ x: number, y: string }>(
      pg.from(from.clone().where({ userId: params.userId }))
        .select(select)
    )
  }

  return {
    server: serverRes.data,
    user: user ? user.data : null,
    elapsed: serverRes.statistics.elapsed
  } as ChartResult
}

async function wordDistribution(params: ChartParams & { minWordCount: number, groupVariant: 'stem' | 'text' | 'lemma' }) {

  const { userId, minWordCount, groupVariant } = params
  const sample = pg.fromRaw('Word sample 0.2')

  function calc(sampe: Knex.QueryBuilder) {
    let groupBy = 'text'
    if (groupVariant === 'lemma') groupBy = 'lemma'
    if (groupVariant === 'stem') groupBy = 'stem'

    const count = sampe.clone().count('*', { as: 'count' }).select({ text: groupBy }).groupBy('text').having('count', '>', minWordCount)

    console.log(count.toQuery());


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

    return selectRaw<{ x: number, y: string }>(res)
  }

  function postProcess(res: { x: number, y: string }[]) {
    return res.map(t => ({ x: t.x, y: Number.parseInt(t.y) }))
  }

  let user = null
  if (params.userId) {
    user = calc(sample.clone().where({ userId: params.userId }))
  }

  const server = calc(sample)
  const result = await Promise.all([server, user])

  return {
    server: postProcess(result[0].data),
    user: result[1] ? postProcess(result[1].data) : null,
    elapsed: result.filter(t => t).map(t => t!.statistics.elapsed).reduce((a, b) => a + b, 0)
  } as ChartResult
}

async function wordDistributionByTime(params: ChartParams & { minWordCount: number, groupVariant: 'stem' | 'text' | 'lemma' }) {
  const sample = pg.fromRaw('Word sample 0.2')
  const groupedBy = pg.raw(`date_trunc('month', dateTime)`)

  const { userId, minWordCount, groupVariant } = params
  let groupBy = 'text'
  if (groupVariant === 'lemma') groupBy = 'lemma'
  if (groupVariant === 'stem') groupBy = 'stem'

  function calc(sample: Knex.QueryBuilder) {
    const count = sample.clone().count('*', { as: 'count' })
      .select({ text: groupBy, date: groupedBy })
      .groupBy(['text', 'date']).having('count', '>', minWordCount)

    const sumState = pg.from(count)
      .select({ count: 'count', text: 'text', date: 'date', sumState: pg.raw('sumState(count)') })
      .groupBy(['text', 'count', 'date']).orderBy('date').orderBy('count', 'desc')

    const countByDate = sample.clone().select({ date: groupedBy })
      .count('*', { as: 'totalByGroup' })
      .groupBy('date').having('totalByGroup', '>', 1000)

    const extendedState = pg.from(sumState.as('S')).join(countByDate.as('C'), 'S.date', 'C.date')
      .select({ count: 'S.count', date: 'S.date', sumState: 'S.sumState', totalByGroup: 'totalByGroup' })


    const runningAccum = pg.from(extendedState)
      .select({
        y: 'count',
        ra: pg.raw(`runningAccumulate(sumState, date) / totalByGroup`),
        x: pg.raw('ceil(ra * 100)'),
        date: 'date'
      })

    const tempRes = pg.from(runningAccum).select('x').select('date').count('*', { as: 'y' })
      .groupBy(['date', 'x'])

    const ySumState = pg.from(tempRes).select(['date', 'x', 'y']).select(pg.raw('sumState(y) as ySumState'))
      .groupBy(['date', 'x', 'y']).orderBy(['date', 'x'])

    const yRunningAccum = pg.from(ySumState).select(['date', 'x']).select(pg.raw(`runningAccumulate(ySumState, date) as y`))
    return selectRaw<{ x: number, y: string, date: Date }>(yRunningAccum)
  }


  let user = null
  if (params.userId) {
    console.log(sample.clone().where({ userId: params.userId }).toQuery());

    user = calc(sample.clone().where({ userId: params.userId }))
  }

  const result = await Promise.all([calc(sample), user])

  return {
    server: result[0].data,
    user: result[1] ? result[1].data : null,
    elapsed: result.filter(t => t).map(t => t!.statistics.elapsed).reduce((a, b) => a + b, 0)
  }
}

async function wordLengthDistribution(params: ChartParams) {
  const from = pg.fromRaw('Word sample 100000')

  function calc(sampe: Knex.QueryBuilder) {
    return selectRaw<{ x: number, y: string }>(
      sampe.select(pg.raw(`lengthUTF8(text) as x, count(*) / (${sampe.clone().count('*').toQuery()}) as y`))
        .groupByRaw(pg.raw('lengthUTF8(text)'))
        .groupByRaw(pg.raw('lengthUTF8(text)'))
        .where(pg.raw('lengthUTF8(text) < 20'))
    )
  }

  let user = null
  if (params.userId) {
    user = calc(from.clone().where({ userId: params.userId }))
  }

  const result = await Promise.all([calc(from), user])
  return {
    server: result[0].data,
    user: result[1] ? result[1].data : null,
    elapsed: result.filter(t => t).map(t => t!.statistics.elapsed).reduce((a, b) => a + b, 0)
  } as ChartResult

}

async function topWords(params: ChartParams) {

  const sample = pg.fromRaw('Word sample 0.2')

  if (params.userId) {
    sample.where({ userId: params.userId })
  }

  const res = await select<{ x: string, y: string }>(t =>
    t
      .from('Word')
      .groupBy('text')
      .orderByRaw(pg.raw('count(*) desc'))
      .select({
        x: 'text',
        y: pg.raw(`count(*) / (${sample.clone().count('*').toQuery()})`)
      })
      .limit(100)
  )

  return {
    words: res.data,
    elapsed: res.statistics.elapsed
  }

}



export default router

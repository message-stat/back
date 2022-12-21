import e, { Router } from "express";
import { Knex, knex } from "knex";
import { dbSelect as select, dbSelectRaw as selectRaw, pg } from "../db";
import Atricles from './articles.json'

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
  'wordDistributionByTime': wordDistributionByTime,
  'wordTrakingByTime': wordTrakingByTime,
  'pieWordPosition': pieWordPosition,
  'messageDistribution': messageDistribution
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
      .select({
        x: knex.raw(`${grop}`),
        y: knex.raw('count(*) / count(distinct userId)')
      })
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
  const sample = pg.fromRaw('Word sample 1000000')

  let groupBy = 'text'
  if (groupVariant === 'lemma') groupBy = 'lemma'
  if (groupVariant === 'stem') groupBy = 'stem'

  function calc(sample: Knex.QueryBuilder) {
    const inside = sample.clone()
      .count('*', { as: 'count' })
      .select({
        text: groupBy,
        sum: pg.raw('sum(count) over (order by count desc rows between unbounded preceding and current row)'),
        total: pg.raw('sum(count) over ()'),
        p: pg.raw('floor(sum / total * 100)')
      })
      .groupBy('text')
      .having('count', '>', minWordCount)
      .orderBy('count', 'desc')

    const res = pg.from(inside)
      .select({
        x: 'p',
        y: pg.raw('sum(c) over (order by x rows between unbounded preceding and current row)')
      })
      .count('*', { as: 'c' })
      .where('p', '<', '100')
      .groupBy('p')
      .orderBy('p')

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
  const sample = pg.fromRaw('Word sample 2000000')
  const groupedBy = pg.raw(`date_trunc('month', dateTime)`)

  const { userId, minWordCount, groupVariant } = params
  let groupBy = 'text'
  if (groupVariant === 'lemma') groupBy = 'lemma'
  if (groupVariant === 'stem') groupBy = 'stem'

  function calc(sample: Knex.QueryBuilder) {
    const inside = sample.clone()
      .count('*', { as: 'count' })
      .select({
        text: groupBy,
        date: groupedBy,
        sum: pg.raw('sum(count) over (partition by date order by count desc rows between unbounded preceding and current row)'),
        total: pg.raw('sum(count) over (partition by date )'),
        t: pg.raw('floor(sum / total * 100)')
      })
      .groupBy(['text', 'date']).having(' count  ', '>', minWordCount)


    const xy = pg.from(inside)
      .select({
        date: 'date',
        x: 't',
        y: pg.raw('sum("c") over (partition by "date" order by "t")')
      })
      .where('total', '>', 500)
      .count('*', { as: 'c' })
      .groupBy(['date', 't'])
      .orderBy(['date', 't'])

    return selectRaw<{ x: number, y: string, date: Date }>(xy)
  }


  let user = null
  if (params.userId) {
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

async function topWords(params: ChartParams & { article: string }) {

  const sample = pg.fromRaw('Word sample 1000000')

  if (params.userId) {
    sample.where({ userId: params.userId })
  }

  if (params.article == 'false') {
    sample.whereNotIn('text', Atricles)
  }

  const inside = pg.from(sample.clone())
    .select({ x: 'text' })
    .count('*', { as: 'count' })
    .groupBy('text')
    .orderBy('count', 'desc')
    .limit(100)

  const res = await select<{ x: string, y: string }>(t =>
    t
      .from(inside)
      .select({
        x: 'x',
        y: pg.raw('count / sum(count) over ()')
      })
  )

  return {
    words: res.data,
    elapsed: res.statistics.elapsed
  }

}


type WordTrakingParams = ChartParams & { word: string, group: 'stem' | 'text' | 'lemma', scale: 'absolute' | 'relative' }
function whereWordTraking(params: WordTrakingParams) {
  const m = params.word.toLowerCase().trim().match(/([а-я|ё]+)|([a-z]+)/g)
  const word = (m && m[0]) ? m[0] : ''
  let where = pg.raw(`'${word}'`)
  const lang = word.match(/[а-я|ё]+/)

  if (params.group === 'stem') where = pg.raw(lang ? `stem('ru', lemmatize('ru', '${word}'))` : `stem('en', lemmatize('en', '${word}'))`)
  if (params.group === 'lemma') where = pg.raw(lang ? `lemmatize('ru', '${word}')` : `lemmatize('en', '${word}')`)


  let target = 'text'
  if (params.group === 'stem') target = 'stem'
  if (params.group === 'lemma') target = 'lemma'

  return {
    where,
    target
  }
}


async function wordTrakingByTime(params: WordTrakingParams) {
  const groupBy = 'month';
  const grop = `date_trunc('${groupBy}', dateTime)`

  const { where, target } = whereWordTraking(params)

  const sample = pg.from('Word')
    .groupByRaw(grop)


  function calc(sample: Knex.QueryBuilder) {

    if (params.scale === 'relative') {

      const left = sample.clone()
        .where(target, where)
        .select({ date: pg.raw(grop), count: pg.raw('count(*)') })

      const right = sample.clone()
        .select({ date: pg.raw(grop), count: pg.raw('count(*)') })
        .having('count', '>', 5000)

      const all = pg.from(left.as('l'))
        .join(right.as('r'), 'r.date', 'l.date')
        .select({ x: 'l.date', y: pg.raw('l.count / r.count') })

      return selectRaw<{ x: number, y: string }>(all)
    } else {
      const all = sample.clone()
        .where(target, where)
        .select({ x: pg.raw(grop), y: pg.raw('count(*) / count(distinct userId)') })
        .orderBy('x')

      return selectRaw<{ x: number, y: string }>(all)
    }
  }


  let user = null
  if (params.userId) {
    user = calc(sample.clone().where({ userId: params.userId }))
  }

  const result = await Promise.all([calc(sample), user])

  return {
    server: result[0].data,
    user: result[1] ? result[1].data : null,
    elapsed: result.filter(t => t).map(t => t!.statistics.elapsed).reduce((a, b) => a + b, 0)
  } as ChartResult

}

async function pieWordPosition(params: WordTrakingParams) {
  const { where, target } = whereWordTraking(params)

  return await selectProcessed({
    userId: params.userId,
    process: (knex) => knex('Word')
      .where(target, where)
      .groupBy('position')
      .select({ name: 'position', count: pg.raw('toInt32(count(*))') })
      .orderBy('position')
  })
}

async function messageDistribution(params: WordTrakingParams & { variant: 'message' | 'word' | 'symbols' }) {
  const sample = pg.from('Message')

  const { variant } = params

  let sum = 'count(*)'
  if (variant === 'word') sum = 'sum(words)'
  if (variant === 'symbols') sum = 'sum(symbols)'

  function calc(sample: Knex.QueryBuilder) {

    const byChat = sample.clone()
      .groupBy(['userId', 'chatId'])
      .select({
        userId: 'userId',
        chatId: 'chatId',
        count: pg.raw(sum),
        sum: pg.raw('sum(count) over (partition by userId order by userId, count desc rows between unbounded preceding and current row)'),
        total: pg.raw('sum(count) over (partition by userId)'),
        p: pg.raw('sum / total'),
        pres: pg.raw('toUInt32(round(p * 100))')
      })

    const count = pg.from(byChat)
      .groupBy(['userId', 'pres'])
      .orderBy(['userId', 'pres'])
      .select({
        userId: 'userId',
        pres: 'pres',
        count: pg.raw('count(*)')
      })

    const users = sample.clone().distinct('userId')
    const right = pg.fromRaw('numbers(100)')
      .select({ pres: 'number', userId: 'userId' })
      .join(users.as('U'), t => t.on(pg.raw('true')))

    const joinded = pg.from(count.as('L'))
      .fullOuterJoin(right.as('R'), t => t.on('L.userId', 'R.userId').on('L.pres', 'R.pres'))
      .orderBy(['userId', 'pres'])
      .select({
        count: 'L.count',
        userID: pg.raw(`if(R.userId = '', L.userId, R.userId)`),
        pres: pg.raw(`if(L.pres = 0, R.pres, L.pres)`),
        cum: pg.raw(`sum(count) over (partition by userID order by userID, pres rows between unbounded preceding and current row)`),
        cumres: pg.raw(`if(cum = 0, 1, cum)`)
      })

    const all = pg.from(joinded)
      .groupBy('pres')
      .orderBy('pres')
      .select({ x: 'pres', y: pg.raw('avg(cumres)') })

    return selectRaw<{ x: number, y: string }>(all)
  }

  let user = null
  if (params.userId) {
    user = calc(sample.clone().where({ userId: params.userId }))
  }

  const result = await Promise.all([calc(sample), user])

  return {
    server: result[0].data,
    user: result[1] ? result[1].data : null,
    elapsed: result.filter(t => t).map(t => t!.statistics.elapsed).reduce((a, b) => a + b, 0)
  } as ChartResult
}

export default router

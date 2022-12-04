import { Router } from "express";
import { dbSelect as select } from "../db";

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

export default router

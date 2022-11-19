import { Router } from "express";
import { dbInsert, dbSelect } from "../db";

const router = Router();

router.get("/", (req, res) => {
  res.json({
    status: 'online',
    env: process.env.NODE_ENV
  })
})

export interface IWord {
  text: string
  debug: string
}

export interface ISendWordSession {
  words: IWord[]
  beginTime: Date
}

export interface IMessage {
  date: Date
  words: number
  symbols: number
  timeFromLastSend: number
  timeFromLastReceive: number
}

export interface ISendMessageSession {
  messages: IMessage[]
  chatId: string
  isChat: boolean
}

export interface ISendSession {
  messages: ISendMessageSession[]
  words: ISendWordSession[]
  userId: string
}


router.post('/send', async (req, res) => {
  const session: ISendSession = req.body

  const words = session.words.flatMap(t => {
    const time = (new Date(t.beginTime)).getTime()
    return t.words.map(w => ({
      text: w.text,
      dateTime: time,
      userId: session.userId,
      debug: w.debug,
    }))
  })

  const messages = session.messages.flatMap(t => {
    return t.messages.map(m => ({
      dateTime: (new Date(m.date)).getTime(),
      userId: session.userId,
      chatId: t.chatId,
      isChat: t.isChat,
      words: m.words,
      symbols: m.symbols,
      timeFromLastSend: m.timeFromLastSend,
      timeFromLastReceive: m.timeFromLastReceive,
    }))
  })

  await Promise.all([
    dbInsert('Word', words),
    dbInsert('Message', messages)
  ])

  res.status(200).json({ status: 'ok' })
})

router.get('/lastSend/:userId', async (req, res) => {
  const { userId } = req.params

  const result = await dbSelect<{ chatId: string, dateTime: string }>(knex =>
    knex.select('chatId')
      .max('dateTime as dateTime')
      .from('Message')
      .where({ userId })
      .groupBy('chatId'))

  const t = result.data.reduce((acc: Record<string, string>, t) => {
    acc[t.chatId] = t.dateTime
    return acc
  }, {})

  return res.json(t)
})

export default router

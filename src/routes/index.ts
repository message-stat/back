import { Router } from "express";
import { dbInsert } from "../db";

const router = Router();

router.get("/api", (req, res) => {
  res.json({
    status: 'online',
    env: process.env.NODE_ENV
  })
})

export interface IWord {
  text: string,
  date: Date,
  debug: string

}

export interface ISendSession {
  words: IWord[];
  beginTime: Date;
}


router.post('/api/send', async (req, res) => {
  const sessions: ISendSession[] = req.body

  const words = sessions.flatMap(session => {
    const time = (new Date(session.beginTime)).getTime()
    return session.words.map(w => ({
      text: w.text,
      debug: w.debug,
      dateTime: time
    }))
  })

  await dbInsert('Word', words)

  res.status(200).json({ status: 'ok' })
})

export default router

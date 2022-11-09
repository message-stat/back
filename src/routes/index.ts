import { Router } from "express";
import { IWord, ISendSession } from 'common/interfaces/IWord'

const router = Router();

router.get("/api", (req, res) => {
  res.json({
    status: 'online',
    env: process.env.NODE_ENV
  })
})

router.post('/api/sendWord', (req, res) => {
  const sessions: ISendSession[] = req.body
  sessions.forEach(session => {
    console.log(session.words.length);
  })

  res.status(200).json({ status: 'ok' })

})

export default router

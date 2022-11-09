import { Router } from "express";

const router = Router();

router.get("/api", (req, res) => {
  res.json({
    status: 'online',
    env: process.env.NODE_ENV
  })
})

export default router

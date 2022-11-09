import express from 'express'
import { json } from 'body-parser'
import cors from 'cors'
import dotenv from './dotenv'


import routes from './routes'

dotenv.setup()

const app = express();
app.use(json({ limit: '50mb' }))
app.use(cors());
app.options('*', cors());

app.use('/', routes)

async function Start() {
  const port = process.env.PORT;

  try {
    app.listen(port, () => {
      console.log(`App listening at http://localhost:${port}`)
    })
  }
  catch (e: any) {
    console.error(`Server error: ${e.message}`)
    process.exit(1)
  }
}

Start()


import express from 'express'
import { json } from 'body-parser'
import cors from 'cors'
import dotenv from './dotenv'
import routes from './routes'
import { connect } from './db'


const app = express();
app.use(json({ limit: '50mb' }))
app.use(cors());
app.options('*', cors());

app.use('/api', routes)

async function Start() {

  await connect()

  try {
    app.listen(dotenv.PORT, () => {
      console.log(`App listening at http://localhost:${dotenv.PORT}`)
    })
  }
  catch (e: any) {
    console.error(`Server error: ${e.message}`)
    process.exit(1)
  }
}

Start()


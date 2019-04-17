const express = require('express')
const app = express()
const cors = require('cors')
const elasticsearch = require('elasticsearch')

app.use(cors())
const client = new elasticsearch.Client({
  host:'localhost:9200'
})

async function search(index) {
  try {
    const response = await client.search({
      index: index,
      body: {
        "query": {
          "match_all": {}
        }
      }
    })
    return response
  } catch (err) {
    console.log('Error!', err)
    return null
  }
}

app.get('/status', (req, res) => {
  search('elastalert_status').then((response) => {
    res.send(response.hits.hits)
  })
})

app.get('/status_status', (req, res) => {
  search('elastalert_status_status').then((response) => {
    res.send(response.hits.hits)
  })
})


app.get('/status_status', (req, res) => {
  search('elastalert_status_status').then((response) => {
    res.send(response.hits.hits)
  })
})

app.listen(8000, () => {
  console.log('Start server at port 8000.')
})
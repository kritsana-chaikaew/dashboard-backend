const express = require('express')
const app = express()
const cors = require('cors')
const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })

app.use(cors())
async function search(index, body={}) {
  try {
    const response = await client.search({
      index: index,
      body: body
    })
    return response
  } catch (err) {
    console.log('Error!', err)
    return null
  }
}
async function create(index, body={}) {
  try {
    const response = await client.indices.create({
      index: index,
      body: body
    })
    return response
  } catch (err) {
    console.log('Error!', err)
    return null
  }
}
async function index(index, id, body={}) {
  try {
    const response = await client.index({
      index: index,
      id: id,
      body: body
    })
    return response
  } catch (err) {
    console.log('Error!', err)
    return null
  }
}
async function getGeo(user, frm, to, locations) {
  try {
    let index = 'login'
    let bodies = locations.map(loc => {
      return {
        "size": 1,
        "query": {
          "bool": {
            "must": [
              {
                "range": {
                  "login_timestamp": {
                    "format": "strict_date_optional_time",
                    "gte": frm,
                    "lte": to
                  }
                }
              },
              {
                "match": {
                  "user": user
                }
              },
              {
                "match": {
                  "location.Location": loc
                }
              }
            ]
          }
        }
      }
    })
    let response = await Promise.all(bodies.map(b => {
      return search(index, b)
    }))
    let geos = await Promise.all(response.map(res => { return res.body.hits.hits[0]._source }))
    return geos
  } catch (err) {
    console.log(err)
  }
}
async function getLastId() {
  try {
    index = 'alert'
    body = {
      "size": 1,
      "sort": { "_id": "desc"},
      "query": {
          "match_all": {}
      }
    }
    let response = await Promise.all([search(index, body)])
    let [id] = await Promise.all(response.map(res => { return res.body.hits.hits[0]._id}))
    console.log(id)
    return id
  } catch (err) {
    console.log(err)
  }
}
app.get('/status', (req, res) => {
  let index = 'elastalert_status'
  let body = {
    query: {
      match_all: {}
    }
  }
  search(index, body).then((response) => {
    res.send(response.hits.hits)
  })
})
app.get('/status_status', (req, res) => {
  let index = 'elastalert_status_status'
  let body = {
    query: {
      match_all: {}
    }
  }
  search(index, body).then((response) => {
    res.send(response.hits.hits)
  })
})
app.get('/', (req, res) => {
  res.send('hello')
})
app.get('/user', (req, res) => {
  let index = 'login'
  let body = {
    "size": 500,
    "query": {
      "bool": {
        "must": [
          {
            "range": {
              "login_timestamp": {
                "format": "strict_date_optional_time",
                "gte": "2019-02-11T00:00:00.000Z",
                "lte": "2019-02-11T00:30:00.000Z"
              }
            }
          }
        ],
        "filter": {
          "term": {
            "user": "yyURzzkn@guest.ku.ac.th"
          }
        }
      }
    }
  }
  search(index, body).then((response) => {
    res.json(response.body.hits.hits)
  })
})
app.get('/update', (req, res) => {
  index = 'alert',
  body = {
    "query": {
      "match_all": {}
    }
  }
  search(index, body).then(response => {
    res.json(response.body.hits.hits)
  })
})
app.get('/user-locations', (req, res) => {
  let user = req.query.user
  let frm = req.query.frm
  let to = req.query.to
  index = 'login'
  body = {
    "aggs": {
      "5": {
        "terms": {
          "field": "user",
          "size": 50,
          "order": {
            "1": "desc"
          }
        },
        "aggs": {
          "1": {
            "cardinality": {
              "field": "location.Location"
            }
          },
          "6": {
            "terms": {
              "field": "location.Location",
              "size": 40,
              "order": {
                "1": "desc"
              }
            },
            "aggs": {
              "1": {
                "cardinality": {
                  "field": "location.Location"
                }
              }
            }
          }
        }
      }
    },
    "size": 0,
    "query": {
      "bool": {
        "must": [
          {
            "bool": {
              "minimum_should_match": 1,
              "should": [
                {
                  "match_phrase": {
                    "agent_type": "login-page"
                  }
                },
                {
                  "match_phrase": {
                    "agent_type": "RE-LOGIN"
                  }
                }
              ]
            }
          },
          {
            "range": {
              "login_timestamp": {
                "format": "strict_date_optional_time",
                "gte": frm,
                "lte": to
              }
            }
          }
        ],
        "filter": {
            "match": {
              "user": user
            }
        },
        "must_not": [
          {
            "match_phrase": {
              "location.Location": {
                "query": "-"
              }
            }
          }
        ]
      }
    }
  }
  if (user && frm && to) {
    search(index, body).then(response => {
      if (response) {
        let locations = response.body.aggregations["5"].buckets[0]["6"].buckets.map(item => { return item.key })
        getGeo(user, frm, to, locations).then(geos => {
          res.json(geos)
        })
      } else {
        res.json({ status: false })
      }
    })
  } else {
    res.json({ status: false })
  }
})
app.get('/lastId', (req, res) => {
  getLastId().then(id => { 
    res.json({ 'id': id }) 
  })
})
// create('alert').then(() => {
//   for (let i=1; i<10; i++) {
//     index('alert', i+2, {
//     "alert-time": new Date(Date.now()),
//     "rule-name": "login with too many devices",
//     "from": "2019-02-11T00:00:00.000Z",
//     "to": "2019-02-11T00:30:00.000Z",
//     "user": "J8NYvy3AXT1@ku.ac.th"
//     })
//   }
// })

app.listen(8000, () => {
  console.log('Start server at port 8000.')
})
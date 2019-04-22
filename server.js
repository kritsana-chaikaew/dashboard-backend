const express = require('express')
const app = express()
const cors = require('cors')
const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })
const slayer = require('slayer')

app.use(cors())

const last = "2019-02-17T18:00:00.000Z"
const MAX_LOCATION = 2
const MAX_DIFF = 4000
const MAX_RATE = 5000 
const ALERT_PROB = 0.02
const BUFFER_SIZE = 50
var id = 0
var loginRateBuffer = []
var loginDiffBuffer = []
var spike = {x:0, y:0}
var foundSpike = false

for (let i=0; i<BUFFER_SIZE; i++) {
  loginRateBuffer.push({ 
    "alert-time": new Date(Date.now()), 
    "rule-name": "",
    "count": 0, 
    "user": '', 
    "from": 0, 
    "to": 0,
    "locations":  [
      { 
        "name": '', 
        "Lat": 13.847058, 
        "Long": 100.56866
      }
    ]
  })
  loginDiffBuffer.push({
    "value": null,
    "from": 0,
    "to": 0,
  })
}

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
async function count(index, body={}) {
  try {
    const response = await client.count({
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
async function del(index, body={}) {
  try {
    const response = await client.indices.delete({
      index: index,
    })
    return response
  } catch (err) {
    console.log('Error!', err)
    return null
  }
}
async function indx(index, id, body={}) {
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
    let index = 'alert'
    let body = {
      "size": 1,
      "sort": { "_id": "desc"},
      "query": {
          "match_all": {}
      }
    }
    let response = await Promise.all([search(index, body)])
    let [lastId] = await Promise.all(response.map(res => { 
      if (res.body.hits.hits[0]) {
        return res.body.hits.hits[0]._id 
      } else {
        return 0
      }
    }))
    return lastId
  } catch (err) {
    console.log(err)
  }
}
function getRand() {
  if (Math.random() > 0.5) {
    return Math.random()/200 * -1
  }
  return Math.random()/200 
}
function isSpike() {
  slayer()
  .y(item => item.value)
  .fromArray(loginRateBuffer)
  .then(spikes => {
    let newSpike = {}
    newSpike = spikes.slice(-1)[0]
    if (newSpike.y != spike.y) {
      spike = newSpike
      foundSpike = true
    }
  });
}
function engine() {
  let start1 = "2019-02-11T00:00:00.000Z"
  let end1 = "2019-02-11T00:30:00.000Z"
  let jobId = setInterval(() => {
    let index = 'login'
    let body = {
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
              "range": {
                "login_timestamp": {
                  "format": "strict_date_optional_time",
                  "gte": start1,
                  "lte": end1
                }
              }
            },
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
            }
          ],
          "filter": [
            {
              "match_all": {}
            }
          ],
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
    search(index, body).then(response => {
      let users = response.body.aggregations["5"].buckets.map(b => {
        return { 
          "alert-time": new Date(Date.now()), 
          "rule-name": "Login from diffrent locations",
          "count": b["1"].value, 
          "user": b.key, 
          "from": start1, 
          "to": end1,
          "locations": b["6"].buckets.map(buck => {
            return { "name": buck.key, "Lat": 13.847058 +  getRand(), "Long": 100.56866 + getRand()}
          })
        }
      })
      for (let i=0; i<users.length; i++) {
        if (users[i].count > MAX_LOCATION && Math.random() < ALERT_PROB) {
          indx('alert', id, users[i])
          id += 1
        }
      }
    })
    if (Date.parse(end1) < Date.parse(last)) {
      start1 = end1
      end1 = new Date(Date.parse(end1) + 30*60000)
    } else {
      clearInterval(jobId)
    }
  }, 1000);

  let start2 = "2019-02-11T00:00:00.000Z"
  let end2 = "2019-02-11T01:00:00.000Z"
  let jobId2 = setInterval(() => {
    isSpike()
    let index = 'login'
    let body = {
      "query": {
        "bool": {
          "must": [
            {
              "range": {
                "login_timestamp": {
                  "format": "strict_date_optional_time",
                  "gte": start2,
                  "lte": end2
                }
              }
            },
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
            }
          ],
          "filter": [
            {
              "match_all": {}
            }
          ]
        }
      }
    }
    count(index, body).then(response => {
      loginRateBuffer.push({
        "value": response.body.count,
        "from": start2,
        "to": end2,
      })
      let diff = 0
      if (loginRateBuffer.length >= 2) {
        diff = response.body.count - loginRateBuffer.slice(-2)[0].value
      } else {
        diff = 0
      }
      loginDiffBuffer.push({
        "value": diff,
        "from": start2,
        "to": end2,
      })
      if (loginRateBuffer.length > BUFFER_SIZE) {
        loginRateBuffer.shift()
      }
      if (loginDiffBuffer.length > BUFFER_SIZE) {
        loginDiffBuffer.shift()
      }
      if (response.body.count > MAX_RATE) {
        console.log(1, response.body.count)
      }
      if (foundSpike) {
        foundSpike = false
        let alert = { 
          "alert-time": new Date(Date.now()), 
          "rule-name": 'spike dectection',
          "value": spike.y, 
          "from": start2, 
          "to": end2
        }
        indx('alert', id, alert)
        id += 1
      }
    })
    if (Date.parse(end2) < Date.parse(last)) {
      start2 = end2
      end2 = new Date(Date.parse(end2) + 30*60000)
    } else {
      clearInterval(jobId2)
    }
  }, 1000)
}
del('alert').then(() => {
  create('alert')
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
    "size": 100,
    "query": {
      "match_all": {}
    }
  }
  search(index, body).then(response => {
    res.json(response.body.hits.hits)
  })
})
app.get('/lastId', (req, res) => {
  getLastId().then(id => { 
    res.json({ 'id': id }) 
  })
})
app.get('/login-rate', (req, res) => {
  res.json([loginRateBuffer, loginDiffBuffer])
})
app.listen(8000, () => {
  console.log('start server at port 8000.')
})
engine()
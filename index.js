'use strict'

const util = require('util')

const Promise = require('bluebird')
const async = Promise.coroutine

const access = require('safe-access')
const assert = require('assert')
const rp = require('request-promise')
var attr = require('dynamodb-data-types').AttributeValue

const createOrUpdateDynamoItem = async(function* (record, entityName) {
  var externalId = createExternalIdFromDynamoRecord(record)
  let propString = createPropertyUpdateStringFromDynamoRecord(record)

  let exists = yield checkVertexExists(externalId)

  let updateString = ""
  if (exists) {
    updateString += `g.V().has("externalId","${externalId}").next()${propString}`
  } else {
    updateString += `graph.addVertex(T.label,"${entityName}","externalId","${externalId}")${propString}`
  }

  let updateResultJson = yield callTitan(updateString)

  assert.equal(access(updateResultJson, 'status.code'), 200)

  return updateResultJson
})

const updateProperty = async(function* (externalId, propertyName, propertyVal) {
  let exists = yield checkVertexExists(externalId)

  if (!exists) {
    console.log(`Trying to set ${propertyName} to ${propertyVal} for ${externalId} but it does not exist`)
    return
  }
  let propString = createPropertyUpdateStringFromDynamoRecord(record)

  let exists = yield checkVertexExists(externalId)

  let updateString = `g.V().has("externalId","${externalId}").property("${propertyName}","${propertyVal}")`
  
  let updateResultJson = yield callTitan(updateString)

  assert.equal(access(updateResultJson, 'status.code'), 200)

  return updateResultJson
})

const checkVertexExists = async(function* (externalId) {
  let resultCheckJson = yield callTitan(`g.V().has("externalId","${externalId}")`)
  
  let resultCheckData = access(resultCheckJson, 'result.data')
  
  return resultCheckData && resultCheckData.length > 0
})

const getVertexTitanId = async(function* (id) {
  let gremlinQuery = `g.V().has("externalId","${id}").id()`
  
  let result = yield callTitan(gremlinQuery)
  
  let resultCheckData = access(result, 'result.data')
  
  return resultCheckData && resultCheckData.length > 0 ? resultCheckData[0] : undefined
})

// Some nasty escaping issues so I'm just removing quotes and will replace when rendered.
const escapeStr = (str) => {
  return typeof str !== "string" ? str : 
    str.replace(/"/g, '_quot_')
       .replace(/\r?\n|\r/g, "")
       .replace(/#/g, '_hash_')
       .replace(/�/g, ' ')
       .replace(/;/g, '_semicolon_')
       .replace(/\$/g, '_dollar_')
       .replace(/%/g, '_percentage_')
       .replace(/&/g, '_and_')
       .replace(/\\/g, '_backslash_')
}

const unescapeStr = (str) => {
  return typeof str !== "string" ? str : 
    str.replace(/_quot_/g, '"')
       .replace(/_hash_/g, '#')
       .replace(/_semicolon_/g, ';')
       .replace(/_percentage_/g, '%')
       .replace(/_dollar_/g, '$')
       .replace(/_and_/g, '&')
       .replace(/_backslash_/g, '\\')
}

const createPropertyUpdateStringFromDynamoRecord = (record) => {
  var flatObject = attr.unwrap(record.dynamodb.NewImage)

  let updateString = ""

  for (var property in flatObject) {  
    updateString += `.property("${escapeStr(property)}","${escapeStr(flatObject[property])}").element()`
  }

  return updateString
}
const createEdgePropertyUpdateStringFromObject = (propsObj) => {

  let updateString = ""

  for (var property in propsObj) {  
    updateString += `.property("${escapeStr(property)}","${escapeStr(propsObj[property])}")`
  }

  return updateString
}

const createExternalIdFromDynamoRecord = (record) => {
  var flatKeys = attr.unwrap(record.dynamodb.Keys)
  let keyString = ""

  for (var property in flatKeys) {  
    keyString += `${flatKeys[property]}-`
  }

  // remove trailing -
  return keyString.substring(0, keyString.length - 1)
}

const checkEdgeExists = async(function* (externalId1, externalId2, edgeLabel) {
  let gremlinQuery = `g.V().has("externalId","${externalId1}").out("${edgeLabel}").`
  gremlinQuery    += `has("externalId","${externalId2}")`

  let resultCheckJson = yield callTitan(gremlinQuery)
  
  let resultCheckData = access(resultCheckJson, 'result.data')
  
  return resultCheckData && resultCheckData.length > 0
})

const createEdgeBetween = async(function* (externalId1, externalId2, edgeLabel) {
  let gremlinQuery = `g.V().has("externalId","${externalId1}").next().`
  gremlinQuery    += `addEdge("${edgeLabel}",g.V().has("externalId","${externalId2}").next(),`
  gremlinQuery    += `"on",new Date())`
  
  let resultJson = yield callTitan(gremlinQuery)

  assert.equal(access(resultJson, 'status.code'), 200)

  return resultJson
})

const updateEdgeBetween = async(function* (externalId1, externalId2, edgeLabel, propString) {

  // we want to get the two entities and find the exact edge between them then update the properties
  let gremlinQuery = `g.V().has("externalId","${externalId1}").outE("${edgeLabel}").as("myEdge").`
  gremlinQuery    += `inV().has("externalId","${externalId2}").`
  gremlinQuery    += `select("myEdge")${propertyString}`
  
  let resultJson = yield callTitan(gremlinQuery)

  assert.equal(access(resultJson, 'status.code'), 200)

  return resultJson
})

const addEdgeBetween = async(function* (externalId1, externalId2, edgeLabel, props) {

  let exists = yield checkEdgeExists(externalId1, externalId2, edgeLabel)

  if (!exists) {
    return createEdgeBetween(externalId1, externalId2, edgeLabel)
  } 
  // If we need to update props later
  //   else {
    // let propString = createEdgePropertyUpdateStringFromObject(props)
  //   return updateEdgeBetween(externalId1, externalId2, edgeLabel, propString)
  // }
})

/**
  Make sure edgeLabels has a comma beforehand i.e. edgeLabels = ',"edgeLabel1","edgeLabel2"'
**/
const removeEdgeBetween = async(function* (externalId1, externalId2, edgeLabels) {
  var gremlinQuery = `g.V().has("externalId","${externalId1}").next().`
  gremlinQuery +=    `edges(Direction.OUT${edgeLabels}).`
  gremlinQuery +=    `findAll{it.inVertex().property("externalId").value()=="${externalId2}"}.each{it.remove()}`
  
  let result = yield callTitan(gremlinQuery)

  assert.equal(access(result, 'status.code'), 200)
  
  return result
})

const callTitan = async(function* (gremlinString) {
  let titanUrl = process.env.titan_url 
  let titanPort = process.env.titan_port
  
  let requestUrl = `http://${titanUrl}:${titanPort}/?gremlin=${gremlinString}`

  let result = yield rp(requestUrl)
  
  let resultJson = JSON.parse(result)

  console.log(`Titan Request: ${requestUrl}. \n Titan Response: ${util.inspect(resultJson, {depth: 2})}`)

  return resultJson
})

// Titan embeds props as ID'd objects. Just need to pluck out the actual data.
const convertTitanArrayToJson = function(titanArray) {
  return (titanArray || []).map((record) => {
    return record.reduce((accumulator, property) => {
      // Use the label as the key and the value as the... value
      accumulator[property.label] = property.value;
      return accumulator;
    }, {})
  })
}


module.exports = {
  escapeStr: escapeStr,
  unescapeStr: unescapeStr,
  callTitan: callTitan,
  addEdgeBetween: addEdgeBetween,
  removeEdgeBetween: removeEdgeBetween,
  convertTitanArrayToJson: convertTitanArrayToJson,
  createExternalIdFromDynamoRecord: createExternalIdFromDynamoRecord,
  createPropertyUpdateStringFromDynamoRecord: createPropertyUpdateStringFromDynamoRecord,
  createEdgePropertyUpdateStringFromObject: createEdgePropertyUpdateStringFromObject,
  checkVertexExists: checkVertexExists,
  getVertexTitanId: getVertexTitanId,
  updateProperty: updateProperty,
  createOrUpdateDynamoItem: createOrUpdateDynamoItem
}
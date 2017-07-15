'use strict'

const util = require('util')

const Promise = require('bluebird')
const async = Promise.coroutine

const access = require('safe-access')
const assert = require('assert')
const rp = require('request-promise')
var attr = require('dynamodb-data-types').AttributeValue


// TODO Refactor
const createOrUpdateDynamoItem = async(function* (record, entityName) {
  var externalId = createExternalIdFromDynamoRecord(record)
  let propString = createPropertyUpdateStringFromDynamoRecord(record)

  let exists = yield checkVertexExists(externalId)

  let updateString = ""
  if (exists) {
    updateString += `g.V().has("externalId","${externalId}")${propString}`
  } else {
    updateString += `g.addV(T.label,"${entityName}","externalId","${externalId}")${propString}`
  }

  return updateString
})

const createDynamoItem = function (record, entityName) {
  const externalId = createExternalIdFromDynamoRecord(record)
  const propString = createPropertyUpdateStringFromDynamoRecord(record)
  const createString = `g.addV(T.label,"${entityName}","externalId","${externalId}")${propString}`

  return createString
}

const updateDynamoItem = function (record, entityName) {
  const externalId = createExternalIdFromDynamoRecord(record)
  const propString = createPropertyUpdateStringFromDynamoRecord(record)
  const updateString = `g.V().has("externalId","${externalId}")${propString}`

  return updateString
}

// TODO Refactor
const updateProperty = async(function* (externalId, propertyName, propertyVal) {
  let exists = yield checkVertexExists(externalId)

  if (!exists) {
    console.log(`Trying to set ${propertyName} to ${propertyVal} for ${externalId} but it does not exist`)
    return
  }
  
  let updateString = `g.V().has("externalId","${externalId}").property("${propertyName}","${propertyVal}")`
  
  return updateString
})

const checkVertexExists = function (externalId) {
  return `g.V().has("externalId","${externalId}")`
}

const getVertexTitanId = function* (id) {
  let gremlinQuery = `g.V().has("externalId","${id}").id()`
  
  return gremlinQuery
}

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

const _escapeStringIfNeeded = (flatObject, property) => {
  return typeof flatObject[property] === "string" ? 
    `.property("${escapeStr(property)}","${escapeStr(flatObject[property])}")` :
    `.property("${escapeStr(property)}","${flatObject[property]}")`
}

const createPropertyUpdateStringFromDynamoRecord = (record) => {
  var flatObject = attr.unwrap(record.dynamodb.NewImage)

  let updateString = ""

  for (var property in flatObject) {  
    updateString += _escapeStringIfNeeded(flatObject, property)
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


const addEdgeBetween = function (externalId1, externalId2, edgeLabel) {
  const unixTime = (new Date()).getTime() / 1000 | 0

  let gremlinQuery = `g.V().has("externalId","${externalId1}").`
  gremlinQuery    += `addE("${edgeLabel}").`
  gremlinQuery    += `to(g.V().has("externalId","${externalId2}")).`
  gremlinQuery    += `property("on",${unixTime})`
  
  return gremlinQuery
}

/**
  Make sure edgeLabels are quoted i.e. edgeLabels = '"edgeLabel1","edgeLabel2"'
**/
const removeEdgeBetween = function (externalId1, externalId2, edgeLabels) {
  var gremlinQuery = `g.V().has("externalId","${externalId1}").`
  gremlinQuery +=    `outE(${edgeLabels}).`
  gremlinQuery +=    `where(otherV().has("externalId","${externalId2}")).`
  gremlinQuery +=    `drop()`

  return gremlinQuery
}


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
  addEdgeBetween: addEdgeBetween,
  removeEdgeBetween: removeEdgeBetween,
  convertTitanArrayToJson: convertTitanArrayToJson,
  createDynamoItem: createDynamoItem,
  updateDynamoItem: updateDynamoItem,
  createExternalIdFromDynamoRecord: createExternalIdFromDynamoRecord,
  createPropertyUpdateStringFromDynamoRecord: createPropertyUpdateStringFromDynamoRecord,
  createEdgePropertyUpdateStringFromObject: createEdgePropertyUpdateStringFromObject,
  checkVertexExists: checkVertexExists,
  getVertexTitanId: getVertexTitanId,
  updateProperty: updateProperty,
  createOrUpdateDynamoItem: createOrUpdateDynamoItem
}
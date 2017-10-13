'use strict'

const util = require('util')

const Promise = require('bluebird')
const async = Promise.coroutine

const access = require('safe-access')
const assert = require('assert')
const rp = require('request-promise')
var attr = require('dynamodb-data-types').AttributeValue


const createDynamoItem = function (record, entityName) {
  const externalId = createExternalIdFromDynamoRecord(record)
  const propString = createPropertyUpdateStringFromDynamoRecord(record)
  const createString = `g.addV(T.label,"${entityName}","externalId","${externalId}")${propString}.id()`

  return createString
}

const updateDynamoItem = function (record, entityName) {
  const externalId = createExternalIdFromDynamoRecord(record)
  const propString = createPropertyUpdateStringFromDynamoRecord(record)
  const updateString = `g.V().has("externalId","${externalId}")${propString}.id()`

  return updateString
}

const updateProperty = async(function* (externalId, propertyName, propertyVal) {
  let updateString = `g.V().has("externalId","${externalId}").property("${propertyName}","${propertyVal}")`
  
  return updateString
})

const checkVertexExists = function (externalId) {
  return `g.V().has("externalId","${externalId}").label()`
}

const getVertexTitanId = function* (id) {
  let gremlinQuery = `g.V().has("externalId","${id}").id()`
  
  return gremlinQuery
}

// Some nasty escaping issues so I'm just removing quotes and will replace when rendered.
const escapeStr = (str) => {
  return typeof str !== "string" ? str : 
    str.replace(/"/g, '_quot_')
       .replace(/'/g, '_singlequote_')
       .replace(/\+/g, '_plus_')
       .replace(/\r?\n|\r/g, "")
       .replace(/#/g, '_hash_')
       .replace(/�/g, ' ')
       .replace(/;/g, '_semicolon_')
       .replace(/\$/g, '_dollar_')
       .replace(/%/g, '_percentage_')
       .replace(/&/g, '_and_')
       .replace(/\\/g, '_backslash_')
       .replace(/–/g, '-')
       .replace(/’/g, '_singlequote_')
       .replace(/\u0013/g, '')
       
}

const unescapeStr = (str) => {
  return typeof str !== "string" ? str : 
    str.replace(/_quot_/g, '"')
       .replace(/_singlequote_/g, "'")
       .replace(/_plus_/g, '+')
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
    // Weird character that cosmos json response doesn't like
    var cleanedUpProp = typeof property !== "string" ? property : property.replace(/\u0013/g,'')
    updateString += _escapeStringIfNeeded(flatObject, cleanedUpProp)
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
}
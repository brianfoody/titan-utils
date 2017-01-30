'use strict'

const assert = require('assert')
const titanUtils = require('../index')

////////////////////////////////////////////////////////////////////////
// Test conversion
const data = [ [ { id: 'c9p-9ns-2e4l', value: 'Dealer2', label: 'DealerId' },
       { id: 'bvh-9ns-2ex1', value: 'Brian30', label: 'SKU' },
       { id: 'bh9-9ns-2upx',
         value: 'Brian30-Dealer2',
         label: 'externalId' },
       { id: 'cnx-9ns-2xvp', value: 'PropValue1', label: 'Prop1' },
       { id: 'd25-9ns-32md', value: 'PropValue2-New', label: 'Prop2' } ],
     [ { id: 'oi8-fwg-2e4l', value: 'Dealer2', label: 'DealerId' },
       { id: 'o40-fwg-2ex1', value: 'Brian25', label: 'SKU' },
       { id: 'he8-fwg-2upx',
         value: 'Brian25-Dealer2',
         label: 'externalId' },
       { id: 'owg-fwg-2xvp', value: 'PropValue1', label: 'Prop1' },
       { id: 'pao-fwg-32md', value: 'PropValue2-New', label: 'Prop2' } ],
     [ { id: 'lcm-fxs-2e4l', value: 'Dealer2', label: 'DealerId' },
       { id: 'kye-fxs-2ex1', value: 'Brian22', label: 'SKU' },
       { id: 'bh2-fxs-2upx',
         value: 'Brian22-Dealer2',
         label: 'externalId' },
       { id: 'lqu-fxs-2xvp', value: 'PropValue1', label: 'Prop1' },
       { id: 'm52-fxs-32md', value: 'PropValue2', label: 'Prop2' } ],
     [ { id: '9vq-j00-2e4l', value: 'Dealer2', label: 'DealerId' },
       { id: '9hi-j00-2ex1', value: 'Brian9', label: 'SKU' },
       { id: '93a-j00-2upx',
         value: 'Brian9-Dealer2',
         label: 'externalId' },
       { id: 'a9y-j00-2xvp', value: 'PropValue1', label: 'Prop1' } ],
     [ { id: '9vq-j00-2e4l', value: 'Dealer20', label: 'DealerId"' },
       { id: '9hi-j00-2ex1', value: 'Brian9', label: 'SKU' },
       { id: '93a-j00-2upx',
         value: 'Brian9-Dealer20',
         label: 'externalId' },
       { id: 'a9y-j00-2xvp', value: 'PropValue1', label: 'Prop1' } ] ]

const json = titanUtils.convertTitanArrayToJson(data)

// All records mapped
assert.equal(json.length, data.length)
// Values correctly extracted
assert.equal("DealerId", data[0][0].label)
assert.equal(json[0]["DealerId"], data[0][0].value)

assert.equal("DealerId", data[1][0].label)
assert.equal(json[1]["DealerId"], data[1][0].value)

////////////////////////////////////////////////////////////////////////
// Test empty array
const data2 = []
const json2 = titanUtils.convertTitanArrayToJson(data2)
assert.equal(json2.length, data2.length)


////////////////////////////////////////////////////////////////////////
// Test undefined
const data3 = []
const json3 = titanUtils.convertTitanArrayToJson(undefined)
assert.equal(json3.length, data3.length)

////////////////////////////////////////////////////////////////////////
// Test null
const data4 = []
const json4 = titanUtils.convertTitanArrayToJson(null)
assert.equal(json4.length, data4.length)



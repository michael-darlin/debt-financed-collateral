'use strict'
const es = require('ethers')
const abi = require('../utils/abi.js')
const addr = require('../utils/addr.js').main
const secrets = require('../utils/secrets.js')
const mysql = require('mysql2/promise')
const tools = require('../utils/eventLib.js')

getPairs(true)

/**
  * Obtain all tokens used in V2 analysis (from SQL). Find pair addresses (from Maker factory contract). Upload addresses to SQL
 * @param  {Boolean} inProd - whether function is in production and event will be inserted into SQL (true), or is only being tested (false)
 * @output String of SQL query that updated SQL database
 * @return {void}
 */
async function getPairs(inProd) {
    /* !---- 0. Initial connection ----! */
    // No progress bar needed because production is so quick

    // Create provider, connect to SQL database
    let provider = tools.startProvider(es)
    let con = await tools.startCon(mysql)

    // Announce what environment we're in
    if (inProd === false) {
        console.log('Testing only. Values will NOT be recorded to SQL database')
    } else {
        console.log('Production run. Values WILL be recorded to SQL database')
    }

    /* !---- 1. Download all tokens ----! */
    console.log('Step 1: Retrieve all V2 tokens from SQL')    
    let query1 = 'SELECT id, address FROM addrTokens'
    let results = (await con.query(query1))[0]

    /* !---- 2. Retrieve Uniswap pair addresses for each token ----! */
    console.log('Step 2: Retrieve pair addresses from UniswapV2 factory') 
    let factory = new es.Contract(addr.factoryV2, abi.factoryV2, provider)
    let sqlArr = []

    for (let i = 0; i < results.length; i++) {
        let currentTokenAddr = results[i].address
        let currentTokenId = results[i].id
        for (let j = i +1; j < results.length; j++) {
            let nextTokenAddr = results[j].address
            let nextTokenId = results[j].id

            let pairAddr = await factory.getPair(currentTokenAddr, nextTokenAddr)

            // NOTE: "token0 is guaranteed to be strictly less than token1 by sort order." (Uniswap V2 doc). token0 address
            // always first alphabetically
            if (currentTokenAddr < nextTokenAddr) {
                sqlArr.push([pairAddr, currentTokenId, nextTokenId])
            } else {
                sqlArr.push([pairAddr, nextTokenId, currentTokenId])
            }            
        }
    }

    /* !---- 2. Record pair adddress in SQL ----! */
    console.log('Step 3: Record pair addresses to SQL') 
    let query2 = mysql.format('INSERT INTO addrPairs (pairAddr, token0Id, token1Id) VALUES ?; ', [sqlArr])
    
    if (inProd === true) {
        await con.query(query2)
    }

    console.log(query2)
    con.close()
}
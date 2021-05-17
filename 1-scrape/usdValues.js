'use strict'
const secrets = require('../utils/secrets.js')
const mysql = require('mysql2/promise')
const cliProgress = require('cli-progress')
const axios = require('axios')
const tools = require('../utils/eventLib.js')

// getValues('USDC-USD', true)
// updateValues(true)
updateFrobAllValues(true)

/**
  * Get the most recent price for a particular record
  * @param  {Array} priceTable - Array of dictionaries from SQL, with price values for each currency at specific time intervals
  * @param  {Dictionary} record - Dictionary of SQL data for a specific row
  * @param  {Integer} tokenNum - Integer to show whether to update token1Amt or token2Amt
 * @return {Integer} USD value of the transaction
 */
function getRecentPrice(priceTable, record, tokenNum) {
    // Assign variables based on token
    if (tokenNum !== 1 && tokenNum !== 2) {
        console.log('Incorrect value. Token parameter must be either 1 or 2.')
        return
    }

    let tokenSymbol = record[`token${tokenNum}Symbol`]
    let tokenAmt = record[`token${tokenNum}Amt`]

    // No need to search array if amount is already zero
    if (tokenAmt == 0) {
        return 0
    }

    // recentTime auto converts to Unix timestamp in milliseconds
    // o.time also converts to milliseconds upon comparison
    let recentTime = Math.max.apply(
        Math, priceTable.map(
            function(o) { 
                if (o.tokenSymbol == tokenSymbol && o.time < record.blockTime) {
                    return o.time
                } else {
                    return 0
                }
            }
        )
    )

    let index = priceTable.findIndex(function(o) {
        return (new Date(o.time).getTime() == recentTime && o.tokenSymbol == tokenSymbol)
    })
    let price = priceTable[index].priceInUsd * tokenAmt
    return price
}

/**
  * Obtain all USD values for relevant currencies
 * @param  {Boolean} inProd - whether function is in production and event will be inserted into SQL (true), or is only being tested (false)
 * @output String of SQL query that updated SQL database
 * @return {void}
 */
async function getValues(pair, inProd) {
    /* !---- 0. Initial setup ----! */
    // Error checking
    let pairArr = ['BTC-USD', 'ETH-USD', 'USDC-USD', 'USDT-USD', 'DAI-USD']
    if (!pairArr.includes(pair)) {
        let msgStr = `${pair} is not a valid pair. Only valid pairs are: \n`
        for (let i = 0; i < pairArr.length; i++) {
            msgStr += `     ${pairArr[i]} \n`
        }
        console.log(msgStr)
        return
    }

    // Connect to SQL database
    let con = await tools.startCon(mysql)

    // Create bar
    const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)

    // Announce what environment we're in
    if (inProd === false) {
        console.log('Testing only. Values will NOT be recorded to SQL database')
    } else {
        console.log('Production run. Values WILL be recorded to SQL database')
    }

    /* !---- 1. Query API and parse JSON ----! */
    console.log('Step 1: Request and parse information from Coinbase API') 
    let startTime = "2020-05-04T12:00:00.000Z"  // Block 10M on 5/4/2020, 1:22:13 PM GMT (Unix: 1588612933). Start an hour before, so that 13:00
                                                // is included in the result-set
    let endTime = "2021-01-21T17:00:00.000Z"    // Block 11.7M on 1/21/2021, 4:50:27 PM GMT (Unix: 1611222627)
    let startUnix = Date.parse(startTime)       // Formatted in milliseconds
    let endUnix = Date.parse(endTime)           // Formatted in milliseconds
    // The maximum number of rows is 300. 24 hours per day = 12.5 days of request
    let increment = 12.5 * 24 * 60 * 60 * 1000

    bar1.start(endUnix - startUnix, 0)
    let sqlArr = []
    for (let i = startUnix; i < endUnix; i += increment) {
        // Coinbase limits to 3 req/second, so we wait 1/3 second (333 milliseconds)
        // If USDC pair, then wait twice as long, because we make two requests per loop
        if (pair == 'USDC-USD') {
            await tools.sleep(666)
        } else {
            await tools.sleep(333)
        }

        let internalStart = new Date(i).toISOString()
        let internalEnd = new Date(i + increment).toISOString()
        
        if (pair == 'USDC-USD') {
            const url1 = `https://api.pro.coinbase.com/products/BTC-USDC/candles?granularity=3600&start=${internalStart}&end=${internalEnd}`
            const url2 = `https://api.pro.coinbase.com/products/BTC-USD/candles?granularity=3600&start=${internalStart}&end=${internalEnd}`
            var result = await axios.get(url1)
            let result2 = await axios.get(url2)
            var records2 = result2.data
        } else {
            const url = `https://api.pro.coinbase.com/products/${pair}/candles?granularity=3600&start=${internalStart}&end=${internalEnd}`
            var result = await axios.get(url)
        }

        let records = result.data

        // Columns are: Unix timestamp, Low, High, Open, Close, Pair Volume
        for (let j = 0; j < records.length; j++) {
            let record = records[j]
            // take opening price, so that we get the price at the beginning of the hour, not the end
            if (pair == 'USDC-USD') {
                let record2 = records2[j]
                var recordPrice = record2[3] / record[3]
            } else {
                var recordPrice = record[3] 
            }

            // Convert into correct MySQL format
            let timeInMs = record[0] * 1000
            let recordTime = new Date(timeInMs).toISOString()
            let recordTimeSql = recordTime.slice(0, 19).replace('T', ' ');
            sqlArr.push([pair, recordTimeSql, recordPrice])
        }

        bar1.update(i + increment - startUnix)
    }
    bar1.stop()

    /* !---- 2. Record information in SQL ----! */
    console.log('Step 2: Record information in SQL') 
    let query2 = mysql.format('INSERT INTO priceData (pair, time, priceInUsd) VALUES ?; ', [sqlArr])
    
    if (inProd === true) {
        await con.query(query2)
    }

    console.log(query2)
    con.close()
}

/**
  * Update mergeRecordsCache with USD values
  * @param  {Boolean} inProd - whether function is in production and event will be inserted into SQL (true), or is only being tested (false)
 * @output String of SQL query that updated SQL database
 * @return {void}
 */
 async function updateAllValues(inProd) {
    /* !---- 0. Initial setup ----! */
    // Connect to SQL database
    let con = await tools.startCon(mysql)

    // Create bar
    const bar1 = new cliProgress.SingleBar({format: '{bar} {percentage}% | ETA: {eta}s | Duration: {duration}s | {value}/{total}'}, 
        cliProgress.Presets.shades_classic)

    /* !---- 2. Load price data into array ----! */  
    let query0a = 'SELECT id, tokenSymbol, time, priceInUsd FROM priceData;'
    let priceTable = (await con.query(query0a))[0]

    /* !---- 3. Get rows, parse information, update tables ----! */  
    // let query0b = 'SELECT id FROM mergeRecordsCache WHERE token1Usd IS NULL AND token2Usd IS NULL ORDER BY id ASC LIMIT 1;'
    // let results0b = (await con.query(query0b))[0]
    // let firstTableRow = results0b[0].id - 1
    let firstTableRow = 5212469
    let lastTableRow = 5383831
    let increment = 10000

    bar1.start(lastTableRow - firstTableRow, 0)

    for (let i = firstTableRow; i < lastTableRow; i += increment) {
        let maxRow = Math.min(i + increment, lastTableRow)
        let query1 = `SELECT * FROM mergeRecordsCache WHERE id > ${i} AND id <= ${maxRow};`
        let results1 = (await con.query(query1))[0]
        
        let sqlArr = []
        for (let j = 0; j < results1.length; j++) {
            let record = results1[j]
            // If no symbol for both token 1 and 2, then skip loop. 
            // If value already recorded for both token 1 and 2, then also skip
            if ((record.token1Symbol === null && record.token2Symbol === null) || (record.token1Usd !== null && record.token2Usd !== null)) {
                continue
            } else {
                // Both token1 and token2 need to be filled
                if ((record.token1Symbol !== null && record.token1Usd === null) && (record.token2Symbol !== null && record.token2Usd === null)) {
                    var token1Usd = getRecentPrice(priceTable, record, 1)
                    var token2Usd = getRecentPrice(priceTable, record, 2)
                } else {
                    // Only token1 needs to be filled
                    if (record.token1Symbol !== null && record.token1Usd === null) {
                        var token1Usd = getRecentPrice(priceTable, record, 1)
                        var token2Usd = null
                    } 
                    // Only token2 needs to be filled
                    else if (record.token2Symbol !== null && record.token2Usd === null) {
                        var token1Usd = null
                        var token2Usd = getRecentPrice(priceTable, record, 2)
                    }    
                }
            }
            sqlArr.push([record.id, token1Usd, token2Usd])
            bar1.update(record.id - firstTableRow)
        }
        
        let sqlStr = 'INSERT INTO mergeRecordsCache (id, token1Usd, token2Usd) VALUES ? ON DUPLICATE KEY UPDATE token1Usd = Values(token1Usd), token2Usd = Values(token2Usd);'
        let query3 = mysql.format(sqlStr, [sqlArr])
        if (inProd === true) {
            await con.query(query3)
        }
        
    }

    bar1.stop()
    con.close()
}

/**
  * Update mergeRecordsCache with USD values for frob transactions only (with token2Amts, which are DAI - these were previously interest-adjusted)
  * @param  {Boolean} inProd - whether function is in production and event will be inserted into SQL (true), or is only being tested (false)
 * @output String of SQL query that updated SQL database
 * @return {void}
 */
 async function updateFrobValues(inProd) {
    /* !---- 0. Initial setup ----! */
    // Connect to SQL database
    let con = await tools.startCon(mysql)

    // Create bar
    const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)

    if (inProd === true) {
        console.log('In production mode. Queries will be recorded to SQL.')
    } else {
        console.log('NOT in production mode. Queries will not be recorded to SQL.')
    }

    /* !---- 2. Load price data into array ----! */  
    let query1 = 'SELECT id, tokenSymbol, time, priceInUsd FROM priceData;'
    let priceTable = (await con.query(query1))[0]

    /* !---- 3. Get rows, parse information, update tables ----! */  
    let query2 = `SELECT * FROM mergeRecordsCache mrc WHERE token2Usd IS NULL AND trxType = 'frob' AND token2Amt <> 0;`
    let results = (await con.query(query2))[0]
    
    let sqlArr = []
    bar1.start(results.length,0)
    for (let i = 0; i < results.length; i++) {
        let record = results[i]
        let token2Usd = getRecentPrice(priceTable, record, 2)
        sqlArr.push([record.id, token2Usd])
        bar1.update(i+1)
    }
    
    let sqlStr = 'INSERT INTO mergeRecordsCache (id, token2Usd) VALUES ? ON DUPLICATE KEY UPDATE token2Usd = Values(token2Usd);'
    let query3 = mysql.format(sqlStr, [sqlArr])
    if (inProd === true) {
        await con.query(query3)
    }
    bar1.stop()
    console.log('\n', query3)
    con.close()
}
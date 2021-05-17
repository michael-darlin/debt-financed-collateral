'use strict'
const es = require('ethers')
const abi = require('../utils/abi.js')
const addr = require('../utils/addr.js').main
const secrets = require('../utils/secrets.js')
const mysql = require('mysql2/promise')
const cliProgress = require('cli-progress')
const pythonShell = require('python-shell').PythonShell
const util = require('util')
const tools = require('../utils/eventLib.js')
const aRunString = util.promisify(pythonShell.runString)

/**
 * Get the decimal value of a BigNumber in Ethereum, returned as a string
 * @param  {BigNumber} eventAmount - bigNumber amount from Ethereum data
 * @return {String} String version of the amount, with decimal places
 */
// Even though this function makes no calls to Ethereum blockchain, the function need to be async ("blocking") so that it can finish before being inserted into SQL array
async function getActualNum(amount, decimals, sign, es) {
    let amountBN = es.BigNumber.from(amount)
    let amountDecStr = sign + es.utils.formatUnits(amountBN, decimals)
	return amountDecStr
}

/**
 * Create a signature based on the function being called, for use in searching topics
 * @param  {BigNumber} functionCall - Ethereum function being called
 * @param  {String} type - whether signature needs to be in log format (only first 10 characters included) or in event format (all characters included)
 * @return {String} String version of the amount, with decimal places
 */
function createSig(functionCall, type) {
    let optionTypes = ['log', 'event']
    if (!optionTypes.includes(type)) {
        console.log("Incorrect type. Type parameter must be value of either 'log' or 'event'. ")
    }
    
    let fullSig = es.utils.id(functionCall)

    if (type == 'log') {
        let partSig = fullSig.substring(0,10).padEnd(66,"0")
        return partSig
    } else {
        return fullSig
    }
}

/**
 * Calculate interest rate-adjusted debt amount, in Python (to keep decimal precision)
 * @param  {Date} currentTime - time when transaction occurred
 * @param  {Date} prevTime - time when ilk rate was last adjusted
 * @param  {String} newDuty - duty (interest rate) most recently assigned, in string format (but with 27 characters)
 * @param  {String} prevCumulativeRate - the prior cumulative rate calculated by Maker
 * @return {String} String version of the new debt amount
 */
async function calcNewAmt(currentTime, prevTime, newDuty, prevCumulativeRate) {
    let currentTimestamp = currentTime.getTime() / 1000
    let prevTimestamp = prevTime.getTime() / 1000
    let secondsDiff = currentTimestamp - prevTimestamp

    let pythonString = `(Decimal(${newDuty})**Decimal(${secondsDiff}))*Decimal(${prevCumulativeRate})`
    let result = await aRunString(`from decimal import *; x=${pythonString}; print(x)`, null)
    let answer = result[0]

    return answer
}


/**
  * For each ilk (already inserted into SQL), get the timestamp when the ilk was created (the init function caclled on jug contract)
 * @param  {Boolean} inProd - whether function is in production and event will be inserted into SQL (true), or is only being tested (false)
 * @output String of SQL query that updated SQL database
 * @return {void}
 */
async function getFirstTimestamp(inProd) {
    let provider = tools.startProvider(es)
    let con = await tools.startCon(mysql)
    const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)

    let jug = new es.Contract(addr.jug, abi.jug, provider)
    let initSig = createSig('init(bytes32)', 'log')

    let query1 = 'SELECT id, ilkType, signature FROM makerV2Ilks'
    let ilks = (await con.query(query1))[0]

    let filter = {
        topics: [initSig], // init() function to start collecting stability fee
    }
    let logs = await jug.queryFilter(filter, firstBlock, 11700000)

    bar1.start(logs.length,0)
    let fullQuery = ''

    for (let i = 0; i < logs.length; i++) {
        let log = logs[i]
        for (let j = 0; j < ilks.length; j++) {
            if (log.topics[2] == ilks[j].signature) {
                let block = await provider.getBlock(log.blockNumber)
                let blockDate = new Date(block.timestamp * 1000)
                let blockTime = blockDate.toLocaleString('en-US', {timeZone: 'UTC'})

                let sqlArr = [blockTime, block.timestamp, log.transactionHash, ilks[j].id]
                let query = mysql.format(`UPDATE makerV2Ilks SET firstBlockTime = STR_TO_DATE(?, '%c/%e/%Y, %r'), firstBlockTimestamp = ?, initTrxHash = ? WHERE id = ?;`, sqlArr)
                if (inProd === true) {
                    await con.query(query)
                }
                fullQuery += `\n${query}`
            }
        }

        bar1.update(i+1)
    }
    
    console.log(fullQuery)
    bar1.stop()
    con.close()
}

/**
  * Find each new rate adjustment for every ilk (the file function called on the jug contract). Record new duty (interest rate accumulator) in SQL
 * @param  {Boolean} inProd - whether function is in production and event will be inserted into SQL (true), or is only being tested (false)
 * @output String of SQL query that updated SQL database
 * @return {void}
 */
async function gatherRates(inProd) {
    let provider = tools.startProvider(es)
    let con = await tools.startCon(mysql)
    const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
    let jug = new es.Contract(addr.jug, abi.jug, provider)

    if (inProd === true) {
        console.log('In production mode. Queries will be recorded to SQL.')
    } else {
        console.log('NOT in production mode. Queries will not be recorded to SQL.')
    }

    let query1 = 'SELECT id, ilkType, signature FROM makerV2Ilks'
    let ilks = (await con.query(query1))[0]

    let fileSig = createSig('file(bytes32,bytes32,uint256)', 'log')

    let fullQuery = ''

    let filter = {
        topics: [fileSig], // file() function to change duty, called on the specific ilk
    }
    let results = await jug.queryFilter(filter, firstBlock, 11700000)

    bar1.start(results.length, 0)
    for (let i = 0; i < results.length; i++) {
        let log = results[i]
        for (let j = 0; j < ilks.length; j++) {
            let ilk = ilks[j]
            if (log.topics[2] == ilk.signature) {
                let newDuty = log.data.substring(266, 330)
                let newDutyStr = await getActualNum("0x" + newDuty, 27, '', es)

                let block = await provider.getBlock(log.blockNumber)
                let blockDate = new Date(block.timestamp * 1000)
                let blockTime = blockDate.toLocaleString('en-US', {timeZone: 'UTC'})
                
                let sqlArr = [blockTime, block.number, log.transactionHash, ilk.ilkType, newDutyStr]
                let query = mysql.format(`INSERT INTO makerV2EthRates (blockTime, blockNumber, trxHash, ilkType, newDuty) VALUES (STR_TO_DATE(?, '%c/%e/%Y, %r'), ?, ?, ?, ?);`, sqlArr)
                if (inProd === true) {
                    await con.query(query)
                }
                fullQuery += `\n${query}`
            }
        }

        bar1.update(i+1)
    }
    bar1.stop()
    console.log(fullQuery)
    con.close()
}

/**
  * Calculate the cumulative rate after each new duty update. Record cumulative rate to SQL
 * @param  {Boolean} inProd - whether function is in production and event will be inserted into SQL (true), or is only being tested (false)
 * @output String of SQL query that updated SQL database
 * @return {void}
 */
async function calcCumulativeRates(inProd) {
    let provider = tools.startProvider(es)
    let con = await tools.startCon(mysql)
    const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)

    let query1 = 'SELECT id, ilkType, signature, firstBlockTime FROM makerV2Ilks'
    let ilks = (await con.query(query1))[0]

    let fullQuery = ''
    for (let i = 0; i < ilks.length; i++) {
        let ilk = ilks[i]
        let query2 = `SELECT id, blockTime, blockNumber, trxHash, newDuty, cumulativeRate FROM makerV2EthRates WHERE cumulativeRate IS NULL AND
                        ilkType = '${ilk.ilkType}' ORDER BY blockNumber ASC;`
        let results2 = (await con.query(query2))[0]
        let newCumulativeRate = 0
        bar1.start(results2.length,0)

        for (let j = 0; j < results2.length; j++) {
            let row = results2[j]
            let currentTime = row.blockTime

            // different logic for first row, because no duty set before
            if (j == 0) {
                var prevTime = ilk.firstBlockTime
                var prevCumulativeRate = 1
                var prevDuty = 1 // Assuming that initial contract set value to 1
            } else {
                var prevTime = results2[j-1].blockTime
                var prevCumulativeRate = newCumulativeRate // carried over from last loop
                var prevDuty = results2[j-1].newDuty
            }

            // NOTE: can't use BigNumber library of ethers.js, because doing exponentiation isn't possible with 27 digits (number will expand 
            // indefinitely). Javascript default library will lose precision (only up to 15 digits). 
            // Therefore, use Python script and Python's Decimal library to get answer
            newCumulativeRate = await calcNewAmt(currentTime, prevTime, prevDuty, prevCumulativeRate)

            let query2 = mysql.format('UPDATE makerV2EthRates SET cumulativeRate = ? WHERE id = ?; ', [newCumulativeRate, row.id])
            fullQuery += query2 + '\n'
            // 2. Execute query if we're in production mode
            if (inProd === true) {
                await con.query(query2)
            }

            bar1.update(j+1)
        }

        bar1.stop()
    }

    console.log(fullQuery)
    con.close()
}

/**
  * For every frob transaction in mergeRecordsCache, adjust the DAI amount for the cumulative rate (approx. 2-3% adjustment). Record to SQL in column newToken2Amt (will be changed to original token2Amt column later)
 * @param  {Boolean} inProd - whether function is in production and event will be inserted into SQL (true), or is only being tested (false)
 * @output String of SQL query that updated SQL database
 * @return {void}
 */
async function updateFrobTrx(inProd) {
    let provider = tools.startProvider(es)
    let con = await tools.tools.startCon(mysql)
    const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)

    // Initial query #1: Get number of rows left to update
    // NOTE: have to use mergeRecordsCache, not makerV2, because mergeRecordsCache has vaultID. Too much additional work to get vaultID for makerV2,
    // update makerV2, then update mergeRecordsCache as well. Therefore, makerV2 has unadjusted DAI amounts, and mergeRecordsCache has adjusted DAI
    // amounts
    let query1 = "SELECT count(1) rowCount FROM mergeRecordsCache mrc WHERE trxType = 'frob' AND token2Amt <> 0 AND newToken2AMT IS NULL;"
    let results1 = (await con.query(query1))[0]
    let totalRows = results1[0].rowCount
    let increment = 1000 // Update in chunks of 1000, because getting all 90K rows at once takes too long
    let totalRowsRounded = Math.ceil(totalRows / 1000) * 1000

    // Initial query #2: Get all cumulativeRates
    let query2 = "SELECT id, blockTime, blockNumber, trxHash, ilkType, newDuty, cumulativeRate FROM makerV2EthRates ORDER BY ilkType ASC, blockNumber DESC;"
    let rates = (await con.query(query2))[0]

    bar1.start(totalRows,0)
    let fullQuery = ''
    let totalCounter = 0
    for (let i = 0; i < totalRowsRounded / increment; i++) {
        let query3 = `SELECT mrc.id, mrc.blockTime, mrc.blockNumber, mrc.trxHash, mrc.token2Amt, mrc.vaultID, mv.ilkType FROM mergeRecordsCache mrc
                        LEFT JOIN makerVaults mv on mrc.vaultID = mv.vaultID
                        WHERE mrc.trxType = 'frob' AND mrc.token2Amt <> 0 AND mrc.newToken2Amt IS NULL LIMIT ${increment};`
        let results3 = (await con.query(query3))[0]
        
        for (let j = 0; j < results3.length; j++) {
            let row = results3[j]
            for (let k = 0; k < rates.length; k++) {
                let rateUpdate = rates[k]
                if (rateUpdate.ilkType == row.ilkType & rateUpdate.blockNumber < row.blockNumber) {
                    let newCumulativeRate = await calcNewAmt(row.blockTime, rateUpdate.blockTime, rateUpdate.newDuty, rateUpdate.cumulativeRate)
                    // NOTE: decimal precision not any better with BN library. Any loss of precision because original tokenAmt only stored with
                    // 18 decimals, not 27. Therefore, slight differences in between newDartAmt and actual amount (around 0.0009% difference)
                    let newDartAmt = newCumulativeRate * row.token2Amt
                    
                    let query2 = mysql.format('UPDATE mergeRecordsCache SET newToken2Amt = ? WHERE id = ?; ', [newDartAmt, row.id])
                    fullQuery += query2 + '\n'
                    if (inProd === true) {
                        await con.query(query2)
                    }
                    break
                }
            }

            totalCounter += 1
            bar1.update(totalCounter)
        }
    }

    bar1.stop()
    console.log(fullQuery)
    con.close()
}

var firstBlock = 8928160 // block when Jug contract first created

// getFirstTimestamp(true)
// gatherRates(false)
// calcCumulativeRates(true)
updateFrobTrx(false)


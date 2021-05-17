'use strict'
const es = require('ethers')
const abi = require('../utils/abi.js')
const addr = require('../utils/addr.js').main
const secrets = require('../utils/secrets.js')
const mysql = require('mysql2/promise')
const cliProgress = require('cli-progress')
const tools = require('../utils/eventLib.js')

// createVaults(true)
// addIlks(true)
// addOwner(true)
// addProxy(true)
addUrnHandler(false)

/**
  * Update SQL with total number of vaults, based on most recent results from CDP Manager
  * @param  {Boolean} inProd - whether function is in production and event will be inserted into SQL (true), or is only being tested (false)
 * @output String of SQL query that updated SQL database
 * @return {void}
 */
async function createVaults(inProd) {
    let con = await tools.startCon(mysql)
    let provider = tools.startProvider(es)
    let manager = new es.Contract(addr.cdpManager, abi.cdpManager, provider)

    let totalVaultsBN = await manager.cdpi()
    let totalVaults = totalVaultsBN.toNumber()
    console.log(totalVaults)

    console.log('Record blank Vault values to SQL') 
    let sqlArr = []
    for (let i = 0; i < totalVaults; i++) {
        let j = i + 1
        sqlArr.push([j])
    }

    let query1 = mysql.format('INSERT INTO makerVaults (vaultID) VALUES ?; ', [sqlArr])
    if (inProd === true) {
        await con.query(query1)
    }
    
    con.close()
}

/**
  * Retrieve the ilk for each vault, by querying the CDP Manager contract. Update SQL with ilk
  * @param  {Boolean} inProd - whether function is in production and event will be inserted into SQL (true), or is only being tested (false)
 * @output String of SQL query that updated SQL database
 * @return {void}
 */
async function addIlks(inProd) {
    /* !---- 0. Initial setup ----! */
    let con = await tools.startCon(mysql)
    let provider = tools.startProvider(es)
    let manager = new es.Contract(addr.cdpManager, abi.cdpManager, provider)
    const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
    
    /* !---- 1. Retrieve highest Vault #, lowest Vault without ilk type ----! */
    console.log('Step 1: Retrieve max Vault #')    
    let query1 = 'SELECT vaultID FROM makerVaults ORDER BY vaultID DESC LIMIT 1'
    let results1 = (await con.query(query1))[0]
    let maxVault = results1[0].vaultID

    let query2 = 'SELECT vaultID FROM makerVaults WHERE ilkType IS NULL ORDER BY vaultID ASC LIMIT 1;'
    let results2 = (await con.query(query2))[0]
    let minVault = results2[0].vaultID

    /* !---- 2. Get ilk for each Vault ----! */
    // Start progress bar
    bar1.start(maxVault, 0)
    let sqlArr = []
    let fullQuery = '\n'
    for (let i = minVault; i <= maxVault; i++) {
        // Pause, so that requests aren't canceled by Infura
        await tools.sleep(40)

        let vaultHex = es.utils.hexlify(i)

        let ilkHex = await manager.ilks(vaultHex)
        // If hex not terminated by zero, then some error in retrieving ilks
        // console.log('\n', ilkHex, ilkHex.charAt(31)
        let arrayHex = es.utils.arrayify(ilkHex)
        if (arrayHex[31] !== 0) {
            var ilkStr = null
        } else {
            var ilkStr = es.utils.parseBytes32String(ilkHex)
        }

        // 1. Append query to a string, to write to a file
        let query3 = mysql.format('UPDATE makerVaults SET ilkType = ? WHERE vaultID = ?; ', [ilkStr, i])
        fullQuery += query3 + '\n'
        // 2. Execute query if we're in production mode
        if (inProd === true) {
            await con.query(query3)
        }
        
        bar1.update(i+1)
    }

    // Ending actions
    bar1.stop()
    console.log(fullQuery)
    con.close()
}

/**
  * Retrieve the owner for each vault, by querying the CDP Manager contract. Update SQL with the owner's address.
  * @param  {Boolean} inProd - whether function is in production and event will be inserted into SQL (true), or is only being tested (false)
 * @output String of SQL query that updated SQL database
 * @return {void}
 */
async function addOwner(inProd) {
    /* !---- 0. Initial setup ----! */
    let con = await tools.startCon(mysql)
    let provider = tools.startProvider(es)
    let manager = new es.Contract(addr.cdpManager, abi.cdpManager, provider)
    const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
    
    /* !---- 1. Retrieve highest Vault #, lowest Vault without owner address ----! */
    console.log('Step 1: Retrieve max Vault #')    
    let query1 = 'SELECT vaultID FROM makerVaults ORDER BY vaultID DESC LIMIT 1'
    let results1 = (await con.query(query1))[0]
    let maxVault = results1[0].vaultID

    let query2 = 'SELECT vaultID FROM makerVaults WHERE ownerAddr IS NULL ORDER BY vaultID ASC LIMIT 1;'
    let results2 = (await con.query(query2))[0]
    let minVault = results2[0].vaultID

    /* !---- 2. Get address for each Vault ----! */
    // Start progress bar
    bar1.start(maxVault, 0)
    let sqlArr = []
    let fullQuery = '\n'
    for (let i = minVault; i <= maxVault; i++) {
        // Pause, so that requests aren't canceled by Infura
        await tools.sleep(40)

        let vaultHex = es.utils.hexlify(i)

        let ownerAddr = (await manager.owns(vaultHex)).toLowerCase()
        
        // 1. Append query to a string, to write to a file
        let query3 = mysql.format('UPDATE makerVaults SET ownerAddr = ? WHERE vaultID = ?; ', [ownerAddr, i])
        fullQuery += query3 + '\n'
        // 2. Execute query if we're in production mode
        if (inProd === true) {
            await con.query(query3)
        }
        
        bar1.update(i+1)
    }

    // Ending actions
    bar1.stop()
    console.log(fullQuery)
    con.close()
}

/**
  * If the owner's address is not an externally owned account (EOA), try to get proxyOwnerAddr via DS Proxy ABI. If not DS Proxy, or if owner 
  * address is EOA, then leave proxyOwnerAddr blank. Update SQL record.
  * @param  {Boolean} inProd - whether function is in production and event will be inserted into SQL (true), or is only being tested (false)
 * @output String of SQL query that updated SQL database
 * @return {void}
 */
async function addProxy(inProd) {
    /* !---- 0. Initial setup ----! */
    let con = await tools.startCon(mysql)
    let provider = tools.startProvider(es)
    const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
    
    /* !---- 1. Retrieve highest Vault #, lowest Vault without proxy address ----! */
    let query1 = 'SELECT vaultOwnerAddr FROM makerVaults WHERE proxyYN IS NULL GROUP BY vaultOwnerAddr;'
    let results = (await con.query(query1))[0]

    /* !---- 2. Get address for each Vault ----! */
    // Start progress bar
    bar1.start(results.length, 0)
    let fullQuery = '\n'
    for (let i = 0; i < results.length; i++) {
        // Pause, so that requests aren't canceled by Infura
        //await tools.sleep(40)

        let vaultOwnerAddr = results[i].vaultOwnerAddr
        let addrCode = await provider.getCode(vaultOwnerAddr)
        // Address is an EOA
        if (addrCode == "0x") {
            var proxyYN = false
            var proxyOwnerAddr = ''
        } else { // Address is a contract
            // Try to get the owner address
            try {
                let proxy = new es.Contract(vaultOwnerAddr, abi.dsProxy, provider)
                var proxyOwnerAddr = (await proxy.owner()).toLowerCase()
                var proxyYN = true
            // If call fails, then the contract must not be a DS Proxy. Therefore, just just leave owning address blank.
            } catch (e) {
                var proxyYN = false
                var proxyOwnerAddr = null
            }
        }
        
        // 1. Append query to a string, to write to a file
        let query2 = mysql.format('UPDATE makerVaults SET proxyYN = ?, proxyOwnerAddr = ? WHERE vaultOwnerAddr = ?; ', [proxyYN, proxyOwnerAddr, vaultOwnerAddr])
        fullQuery += query2 + '\n'
        // 2. Execute query if we're in production mode
        if (inProd === true) {
            await con.query(query2)
        }
        
        bar1.update(i+1)
    }

    // Ending actions
    bar1.stop()
    console.log(fullQuery)
    con.close()
}

/**
  * Retrieve the urnHandler address for each vault by querying CDP Manager contract. Update SQL record.
  * @param  {Boolean} inProd - whether function is in production and event will be inserted into SQL (true), or is only being tested (false)
 * @output String of SQL query that updated SQL database
 * @return {void}
 */
async function addUrnHandler(inProd) {
    /* !---- 0. Initial setup ----! */
    let con = await tools.startCon(mysql)
    let provider = tools.startProvider(es)
    let manager = new es.Contract(addr.cdpManager, abi.cdpManager, provider)
    const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
    
    /* !---- 1. Retrieve highest Vault #, lowest Vault without owner address ----! */
    console.log('Step 1: Retrieve max Vault #')    
    let query1 = 'SELECT vaultID FROM makerVaults ORDER BY vaultID DESC LIMIT 1'
    let results1 = (await con.query(query1))[0]
    let maxVault = results1[0].vaultID

    let query2 = 'SELECT vaultID FROM makerVaults WHERE urnHandlerAddr IS NULL ORDER BY vaultID ASC LIMIT 1;'
    let results2 = (await con.query(query2))[0]
    let minVault = results2[0].vaultID

    /* !---- 2. Get address for each Vault ----! */
    // Start progress bar
    bar1.start(maxVault, 0)
    let sqlArr = []
    let fullQuery = '\n'
    for (let i = minVault; i <= maxVault; i++) {
        // Pause, so that requests aren't canceled by Infura
        await tools.sleep(40)

        let vaultHex = es.utils.hexlify(i)

        let urnHandlerAddr = (await manager.urns(vaultHex)).toLowerCase()
        
        // 1. Append query to a string, to write to a file
        let query3 = mysql.format('UPDATE makerVaults SET urnHandlerAddr = ? WHERE vaultID = ?; ', [urnHandlerAddr, i])
        fullQuery += query3 + '\n'
        // 2. Execute query if we're in production mode
        if (inProd === true) {
            await con.query(query3)
        }
        
        bar1.update(i+1)
    }

    // Ending actions
    bar1.stop()
    console.log(fullQuery)
    con.close()
}
module.exports = {
	sleep,
	getBlockTime,
	getActualNum,
	startProvider,
	startCon
}

/**
 * Pause execution for a specified number of milliseconds
 * @param  {Number} Number of milliseconds to wait
 * @return {void}
 */
function sleep(ms) {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}

/**
 * Get the blocktime for a specific block
 * @param  {Provider} Provider - for Ethereum data
 * @param  {Number} blockNumber - specific block to query
 * @return {String} Formatted string of the block date
 */
async function getBlockTime(provider, blockNumber) {
	let block = await provider.getBlock(blockNumber)
	let initTime = block.timestamp
	let initDate = new Date(initTime * 1000)
	return initDate.toLocaleString('en-US', {timeZone: 'America/New_York'})
}

/**
 * Get the decimal value of a BigNumber in Ethereum
 * @param  {BigNumber} eventAmount - bigNumber amount from Ethereum data
 * @return {String} String version of the amount, with decimal places
 */
// Even though this function makes no calls to Ethereum blockchain, the function need to be async ("blocking") so that it can finish before being inserted into SQL array
async function getActualNum(eventAmount, es) {
    let amountFN = es.FixedNumber.from(eventAmount)
    let decimals18 = es.FixedNumber.from('1000000000000000000') // The values are given with 18 extra decimal places, so divide to get to the actual number
	let amountActual = amountFN.divUnsafe(decimals18)
	return amountActual.toString()
}

/**
 * Get an Infura Provider from ethers.js
 * @param  {Module} es ethers.js library
 * @return {Provider} ether.js Provider
 */
 function startProvider(es) {
    // Create provider
    let provider = new es.providers.InfuraProvider('homestead', secrets.infuraKey)
    return provider
}

/**
 * Get connection to live SQL database
 * @param  {Module} mysql MySQL object
 * @return {Connection} SQL connection
 */
async function startCon(mysql) {
    let con = await mysql.createConnection({
        host: secrets.sqlHost,
        user: secrets.sqlUser,
        password: secrets.sqlPass,
        database: 'defiData'
    })
    return con
}

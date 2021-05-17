# Import all needed modules. This could be done in the classes, but is done globally, in order to avoid duplicate calls.
# The overhead requirement is not significant

# Need to complete this action so that files can be imported from the parent directory structure
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from web3 import Web3
import mysql.connector
from google.cloud import bigquery
from tqdm import tqdm

# The pylint comment disables pylint on the next line, because it doesn't recognize a properly working import
from utils import secrets # pylint:disable=F0401

class DataValidationError(Exception):
    """
    A custom error class

    Attributes
    ----------
    message (String)        Error message (set at time the Exception is raised)

    Methods
    -------
    __init__                Saves the error message for later processing
    __repr__                Returns string output of the call by which the object was instantiated
    """
    def __init__(self, message):
        self.message = message
    
    def __repr__(self):
        return (f'{self.__class__.__name__}({self.message})')

class Record:
    """
    A class representing a record to be queried

    Attributes
    ----------
    name (String)                   The name of the record
    data_type (String)              The type of the record (event or log)
    signature (String)              The record signature, in plain text
    params (String)                 The parameters used to instantiate the object
    stored_method_name (String)     The record's signature, formatted for Ethereum

    Methods
    -------
    __init__                Performs data validation checks and creates all attributes
    __repr__                Returns string output of the call by which the object was instantiated
    """
    def __init__(self, record_name, protocol):
        record_found = False
        for record_in_protocol_list in protocol.valid_records:
            # If record name matches the list of valid records, then break the for loop and set the record equal to the entry in the
            # list of valid records
            if (record_in_protocol_list['name'] == record_name):
                temp_record = record_in_protocol_list
                record_found = True
                break
        
        # If record not found, then it must be invalid
        if (record_found == False):
            record_list_in_string = ''
            for record in protocol.valid_records:
                name = record['name']
                record_list_in_string += f'    {name} \n'
            raise DataValidationError(f'Record type "{record_name}" is invalid. Valid records for the {protocol.name} protocol are:\n{record_list_in_string}')
        
        # Otherwise, set the relevant attributes
        else:
            self.name = temp_record['name']
            self.data_type = temp_record['data_type']
            self.signature = temp_record['signature']
            # Protocol is not saved, so the text version of the protocol is saved, in order to show in __repr__
            self.params = '{self.name}, {protocol})' 

            # The format of the signature in the Ethereum data set depends on whether we are querying an event or a log
            if (self.data_type == 'event'):
                self.stored_method_name = Web3.toHex(Web3.keccak(text=self.signature))
            elif (self.data_type == 'log'):
                methodName = Web3.toHex(Web3.keccak(text=self.signature))
                self.stored_method_name = methodName[0:10].ljust(66,"0")

    def __repr__(self):
        return (f'{self.__class__.__name__}({self.params})')

class Protocol:
    """
    A class representing a specific version of a DeFi protocol

    Attributes
    ----------
    name (String)               The name of the protocol
    version (Integer)           The version of the protocol
    valid_records (Array)       An array of the records that can be queried for the protocol
    addr (String or Array)      A single address should be queried on the blockchain (can also be an array)
    insert_query (String)       String of SQL query that will be used to update the SQL table

    Methods
    -------
    __init__                        Performs data validation checks and creates all attributes
    process_results                 Sets the specific function that will process results, depending on protocol and version being used
     _process_results_maker1        Function to process results from BigQuery, for Maker version 1
     _process_results_maker2        Function to process results from BigQuery, for Maker version 2
     _process_results_compound1     Function to process results from BigQuery, for Compound version 1
     _process_results_compound2     Function to process results from BigQuery, for Compound version 2
     _process_results_uniswap1      Function to process results from BigQuery, for Uniswap version 1
     _process_results_uniswap2      Function to process results from BigQuery, for Uniswap version 2
     _process_results_aave1         Function to process results from BigQuery, for Aave version 1
    __repr__                        Returns string output of the call by which the object was instantiated
    """    
    def __init__(self, protocol_name, protocol_version, ex_sources):
        if (protocol_name not in ['Maker', 'Compound', 'Uniswap', 'Aave']):
            raise DataValidationError(f'Protocol type "{protocol_name}" is invalid". Only valid protocols are "Maker", "Compound", "Uniswap", or "Aave"')
        elif (protocol_version not in [1, 2]):
            raise DataValidationError(f'Protocol version "{protocol_version}" is invalid". Only valid protocol versions are 1 or 2')
        else:
            # 1. Set protocol name
            self.name = protocol_name
            self.version = protocol_version

            # 2. Set function that will process results
            func_name = f'_process_results_{self.name.lower()}{self.version}'
            self.process_results = getattr(self, func_name)

            # 3. A) Create array with valid records for each protocol, B) List address to query, C) Insert query
            if (self.name == 'Maker' and self.version == 1):
                self.valid_records = [
                    {'name': 'open', 'data_type': 'event', 'signature':'LogNewCup(address,bytes32)'}, 
                    {'name': 'give', 'data_type': 'log', 'signature':'give(bytes32,address)'}, 
                    {'name': 'shut', 'data_type': 'log', 'signature':'shut(bytes32)'}, 
                    {'name': 'lock', 'data_type': 'log', 'signature':'lock(bytes32,uint256)'}, 
                    {'name': 'free', 'data_type': 'log', 'signature':'free(bytes32,uint256)'}, 
                    {'name': 'bite', 'data_type': 'log', 'signature':'bite(bytes32)'}, 
                    {'name': 'draw', 'data_type': 'log', 'signature':'draw(bytes32,uint256)'}, 
                    {'name': 'wipe', 'data_type': 'log', 'signature':'wipe(bytes32,uint256)'}
                ]

                self.addr = '0x448a5065aebb8e423f0896e6c5d525c040f59af3' # SaiTub
                self.insert_query = "INSERT INTO makerV1(blockTime, blockNumber, trxHash, usrAddr, dinkAmount, dartAmount, cdpIndex, collateralName, trxType) VALUES(STR_TO_DATE(%s, '%Y-%m-%d %T'), %s, %s, %s, %s, %s, %s)"
            elif (self.name == 'Compound' and self.version == 1):
                self.valid_records = [
                    {'name': 'SupplyReceived', 'data_type': 'event', 'signature':'SupplyReceived(address,address,uint256,uint256,uint256)'}, 
                    {'name': 'SupplyWithdrawn', 'data_type': 'event', 'signature':'SupplyWithdrawn(address,address,uint256,uint256,uint256)'}, 
                    {'name': 'BorrowTaken', 'data_type': 'event', 'signature':'BorrowTaken(address,address,uint256,uint256,uint256,uint256)'}, 
                    {'name': 'BorrowRepaid', 'data_type': 'event', 'signature':'BorrowRepaid(address,address,uint256,uint256,uint256)'}, 
                    {'name': 'BorrowLiquidated', 'data_type': 'event', 'signature':'BorrowLiquidated(address,address,uint256,uint256,uint256,uint256,address,address,uint256,uint256,uint256,uint256)'}
                ]

                self.addr = '0x3fda67f7583380e67ef93072294a7fac882fd7e7' # MoneyMarket
                self.insert_query = """INSERT INTO compoundV1(blockTime, blockNumber, trxHash, usrAddr, liquidatorAddr, tokenAddr, amount, startingBalance, newBalance, borrowAmountWithFee, trxType) VALUES(STR_TO_DATE(%s, '%Y-%m-%d %T'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            elif (self.name == 'Uniswap' and self.version == 1):
                self.valid_records = [
                    {'name': 'EthPurchase', 'data_type': 'event', 'signature':'EthPurchase(address,uint256,uint256)'}, 
                    {'name': 'TokenPurchase', 'data_type': 'event', 'signature':'TokenPurchase(address,uint256,uint256)'}, 
                    {'name': 'AddLiquidity', 'data_type': 'event', 'signature':'AddLiquidity(address,uint256,uint256)'}, 
                    {'name': 'RemoveLiquidity', 'data_type': 'event', 'signature':'RemoveLiquidity(address,uint256,uint256)'}
                ]

                # For Uniswap V1, we need to query multiple exchanges, so we create an array for the addressess
                self.addr = []

                temp_query = f"SELECT exchangeAddr FROM tokensWithExchanges"
                ex_sources.cursor.execute(temp_query)
                temp_record = ex_sources.cursor.fetchall()
                for item in temp_record:
                    self.addr.append(item[0].lower())

                self.insert_query = "INSERT INTO uniswapV1(blockTime, blockNumber, trxHash, callingAddr, exchangeAddr, tokenAmount, ethAmount, trxType) VALUES(STR_TO_DATE(%s, '%Y-%m-%d %T'), %s, %s, %s, %s, %s, %s, %s)"
            elif (self.name == 'Aave' and self.version == 1):
                self.valid_records = [
                    {'name': 'Deposit', 'data_type': 'event', 'signature': 'Deposit(address,address,uint256,uint16,uint256)'},
                    {'name': 'RedeemUnderlying', 'data_type': 'event', 'signature': 'RedeemUnderlying(address,address,uint256,uint256)'},
                    {'name': 'Borrow', 'data_type': 'event', 'signature': 'Borrow(address,address,uint256,uint256,uint256,uint256,uint256,uint16,uint256)'},
                    {'name': 'Repay', 'data_type': 'event', 'signature': 'Repay(address,address,address,uint256,uint256,uint256,uint256)'},
                    {'name': 'OriginationFeeLiquidated', 'data_type': 'event', 'signature': 'OriginationFeeLiquidated(address,address,address,uint256,uint256,uint256)'},
                    {'name': 'LiquidationCall', 'data_type': 'event', 'signature': 'LiquidationCall(address,address,address,uint256,uint256,uint256,address,bool,uint256)'},
                ]

                self.addr = '0x398ec7346dcd622edc5ae82352f02be94c62d119' # Lending pool

                self.insert_query = "INSERT INTO aaveV1(blockTime, blockNumber, trxHash, usrAddr, liquidatorAddr, reserveAddr, tokenAmount, originationFee, liquidateCollateralAmt, liquidateCollateralAddr, trxType) VALUES(STR_TO_DATE(%s, '%Y-%m-%d %T'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            elif (self.name == 'Maker' and self.version == 2):
                self.valid_records = [
                    {'name': 'frob2', 'data_type': 'log', 'signature': 'frob(bytes32,address,address,address,int256,int256)'}
                    # {'name': 'frob', 'data_type': 'log', 'signature': 'frob(uint256,int256,int256)'},
                    # {'name': 'give', 'data_type': 'log', 'signature': 'give(uint256,address)'},
                    # {'name': 'newCdp', 'data_type': 'event', 'signature': 'NewCdp(address,address,uint256)'},
                    # {'name': 'open', 'data_type': 'log', 'signature': 'open(bytes32,address)'}
                    # NOTE: One function not covered
                    # A second flux log [signature: flux(bytes32,uint256,address,uint256)] is also allowed. However, it was never called 
                    # between 10M and 11.7M blocks. Therefore, we only gather data on the 'flux' function actually called
                ]

                self.addr = '0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b' # Vat
                self.insert_query = "INSERT INTO makerV2(blockTime, blockNumber, trxHash, usrAddr, dinkAmount, dartAmount, vaultID, trxType) VALUES(STR_TO_DATE(%s, '%Y-%m-%d %T'), %s, %s, %s, %s, %s, %s, %s)"
            elif (self.name == 'Compound' and self.version == 2):
                self.valid_records = [
                    {'name': 'Mint', 'data_type': 'event', 'signature': 'Mint(address,uint256,uint256)'},
                    {'name': 'Redeem', 'data_type': 'event', 'signature': 'Redeem(address,uint256,uint256)'},
                    {'name': 'Borrow', 'data_type': 'event', 'signature': 'Borrow(address,uint256,uint256,uint256)'},
                    {'name': 'RepayBorrow', 'data_type': 'event', 'signature': 'RepayBorrow(address,address,uint256,uint256,uint256)'},
                    {'name': 'LiquidateBorrow', 'data_type': 'event', 'signature': 'LiquidateBorrow(address,address,uint256,address,uint256)'},
                ]

                # For Compound V2, we need to query multiple cTokens, so we create an array for the addressess
                self.addr = []

                temp_query = f"SELECT cTokenAddr FROM addrTokens"
                ex_sources.cursor.execute(temp_query)
                temp_record = ex_sources.cursor.fetchall()
                for item in temp_record:
                    self.addr.append(item[0].lower())

                # Set a constant for the number of decimals in a cToken
                self._cTokenDecimals = 8

                self.insert_query = "INSERT INTO compoundV2(blockTime, blockNumber, trxHash, usrAddr, liquidateAddr, cTokenID, tokenAmount, cTokenAmount, liquidateCollateralAddr, accountBorrowBalance, totalBorrowBalance, trxType) VALUES(STR_TO_DATE(%s, '%Y-%m-%d %T'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            elif (self.name == 'Aave' and self.version == 2):
                # Likely won't gather data - as of 1/28/21, Aave V1 is more popular than V2
                pass
            else: # Uniswap V2
                self.valid_records = [
                    {'name': 'Swap', 'data_type': 'event', 'signature':'Swap(address,uint256,uint256,uint256,uint256,address)'}, 
                ]

                # For Uniswap V2, we need to query multiple exchanges, so we create an array for the addressess
                self.addr = []

                temp_query = f"SELECT pairAddr FROM addrUniPairs"
                ex_sources.cursor.execute(temp_query)
                temp_record = ex_sources.cursor.fetchall()
                for item in temp_record:
                    self.addr.append(item[0].lower())

                self.insert_query = "INSERT INTO uniswapV2(blockTime, blockNumber, trxHash, sendAddr, receiveAddr, pairID, token0In, token1In, token0Out, token1Out, trxType) VALUES(STR_TO_DATE(%s, '%Y-%m-%d %T'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    def _process_results_maker1(self, item, partial_list, record, sqlArr):
        # cdpIndex
        if (record.name == 'open'):
            cdpIndex = int(item['data'], 16)
        else:
            cdpIndex = int(item['topics'][2], 16)

        # callingAddr
        if(record.name == 'give'):
            # NOTE: the calling address is the original owner. To avoid needing two columns, we will just input the new column
            callingAddr = "0x" + item['topics'][3][26:]
        else: 
            # NOTE: for bite, the calling address is the biter, not the owner. Also, some calls are made through a proxy contract, so the 
            # calling address is the contract, not the actual owner. The real owner would need to be deduced from historical open/give
            # transactions
            callingAddr = "0x" + item['topics'][1][26:]
            
        # amount
        if (record.name == 'give' or record.name == 'shut' or record.name == 'bite' or record.name == 'open'):
            amount = 0 # NOTE: for bite/shut, need to find the amount through a separate process (BigQuery couldn't handle logic)
        else:
            weiAmount = int(item['topics'][3], 16)
            # Convert decimal amount into negative if freeing/wiping
            if(record.name == 'free' or record.name == 'wipe'):
                amount = -Web3.fromWei(weiAmount, 'ether')
            else:
                amount = Web3.fromWei(weiAmount, 'ether')

        # If cdpIndex is larger than largest unsigned int value (in SQL), then set to 0. The value must have been erroneously entered
        if (cdpIndex > 4294967295):
            cdpIndex = 0
        
        allFields = [partial_list['blockTime'], partial_list['blockNumber'], partial_list['trxHash'], callingAddr, amount, cdpIndex, record.name]

        # Skip any duplicate rows
        if (len(sqlArr) == 0):
            sqlArr.append(allFields)
        else:
            # Only append if the value is different than the previous row
            if (sqlArr[len(sqlArr)] != allFields):
                sqlArr.append(allFields)

        return sqlArr

    def _process_results_compound1(self, item, partial_list, record, sqlArr):
        chunks = self._chunk_data(item['data'])

        usrAddr = '0x' + chunks[0][24:]

        if (record.name == 'BorrowLiquidated'):
            # usrAddr = targetAddress (owner - already set above)
            # Liquidator address = liquidator
            liquidatorAddr = '0x' + chunks[6][24:]
            # borrowAmtWithFee
            borrowAmtWithFee = 0

            # 1. Borrow repaid (by liquidator) - BorrowRepaidLiquidate
            # amount = amountRepaid (neg.), tokenAddr = assetBorrow
            # startingBalance = borrowBalanceAccumulated, newBalance = borrowBalanceAfter
            weiAmount = int(chunks[4], 16)
            amount = -Web3.fromWei(weiAmount, 'ether')
            tokenAddr = '0x' + chunks[1][24:]
            weiStartBal = int(chunks[3], 16)
            startBal = Web3.fromWei(weiStartBal, 'ether')
            weiNewBal = int(chunks[5], 16)
            newBal = Web3.fromWei(weiNewBal, 'ether')

            allFields = [partial_list['blockTime'], partial_list['blockNumber'], partial_list['trxHash'], usrAddr, liquidatorAddr, tokenAddr, amount, startBal, newBal, borrowAmtWithFee, 'BorrowRepaidLiquidate']
            sqlArr.append(allFields)

            # 2. Collateral withdrawn (forcibly from owner) - SupplyWithdrawnLiquidate
            # amount = amountSeized (neg.), tokenAddr = assetCollateral
            # startingBalance = collateralBalanceAccumulated, newBalance = collateralBalanceAfter
            weiAmount = int(chunks[10], 16)
            amount = -Web3.fromWei(weiAmount, 'ether')
            tokenAddr = '0x' + chunks[7][24:]
            weiStartBal = int(chunks[9], 16)
            startBal = Web3.fromWei(weiStartBal, 'ether')
            weiNewBal = int(chunks[11], 16)
            newBal = Web3.fromWei(weiNewBal, 'ether')

            allFields = [partial_list['blockTime'], partial_list['blockNumber'], partial_list['trxHash'], usrAddr, liquidatorAddr, tokenAddr, amount, startBal, newBal, borrowAmtWithFee, 'SupplyWithdrawnLiquidate']
            sqlArr.append(allFields)

            # 3. Collateral locked (given to liquidator) - SupplyReceivedLiquidate
            # Same as #2, but positive amount. #2 and #3 will offset each other
            # The collateralBalance before/after is not the liquidator's actual balance, it's the owner's balance. There's no way to know the 
            # liquidator's balance from the event or other logs. Therefore, we insert as 0
            weiAmount = int(chunks[10], 16)
            amount = Web3.fromWei(weiAmount, 'ether')
            startBal = 0
            newBal = 0

            allFields = [partial_list['blockTime'], partial_list['blockNumber'], partial_list['trxHash'], usrAddr, liquidatorAddr, tokenAddr, amount, startBal, newBal, borrowAmtWithFee, 'SupplyReceivedLiquidate']
            sqlArr.append(allFields)
        else:
            # Addresses
            tokenAddr = '0x' + chunks[1][24:]
            # Starting balance
            weiStartBal = int(chunks[3], 16)
            startBal = Web3.fromWei(weiStartBal, 'ether')
            
            # Amount
            weiAmount = int(chunks[2], 16)
            # Record as negative for SupplyWithdrawn and BorrowRepaid
            if (record.name == 'SupplyWithdrawn' or record.name == 'BorrowRepaid'):
                amount = -Web3.fromWei(weiAmount, 'ether')
            else:
                amount = Web3.fromWei(weiAmount, 'ether')
            
            # borrowAmtWithFee
            # BorrowTaken has different fields in the last two slots
            if (record.name == 'BorrowTaken'):
                weiBorrowAmtWithFee = int(chunks[4], 16)
                borrowAmtWithFee = Web3.fromWei(weiBorrowAmtWithFee, 'ether')

                weiNewBal = int(chunks[5], 16)
                newBal = Web3.fromWei(weiNewBal, 'ether')
            else:
                borrowAmtWithFee = 0
                weiNewBal = int(chunks[4], 16)
                newBal = Web3.fromWei(weiNewBal, 'ether')
            
            # Append row only once, for all non-liquidation transactions
            allFields = [partial_list['blockTime'], partial_list['blockNumber'], partial_list['trxHash'], usrAddr, None, tokenAddr, amount, startBal, newBal, borrowAmtWithFee, record.name]
            # No need to search for duplicates, because the events are all unique (unlike logs in Maker)
            sqlArr.append(allFields)
        
        return sqlArr

    def _process_results_uniswap1(self, item, partial_list, record, sqlArr):        
        # callingAddr
        callingAddr = "0x" + item['topics'][1][26:]

        # exchangeAddr
        exchangeAddr = item['address']

        # tokenAmount and ethAmount
        if (record.name == 'ethPurchase'):
            tokenPosInArray = 2
            ethPosInArray = 3
            tokenSign = -1
            ethSign = 1
        elif (record.name == 'TokenPurchase'):
            tokenPosInArray = 3
            ethPosInArray = 2
            tokenSign = 1
            ethSign = -1
        elif (record.name == 'AddLiquidity'):
            tokenPosInArray = 3
            ethPosInArray = 2
            tokenSign = -1
            ethSign = -1
        else: # RemoveLiquidity
            tokenPosInArray = 3
            ethPosInArray = 2
            tokenSign = 1
            ethSign = 1

        weiTokenAmount = int(item['topics'][tokenPosInArray], 16)
        tokenAmount = Web3.fromWei(weiTokenAmount, 'ether') * tokenSign
        weiEthAmount = int(item['topics'][ethPosInArray], 16)
        ethAmount = Web3.fromWei(weiEthAmount, 'ether') * ethSign

        # Append row and return the array
        allFields = [partial_list['blockTime'], partial_list['blockNumber'], partial_list['trxHash'], callingAddr, exchangeAddr, tokenAmount, ethAmount, record.name]
        sqlArr.append(allFields)
        return sqlArr

    def _process_results_aave1(self, item, partial_list, record, sqlArr, tokenArr):
        chunks = self._chunk_data(item['data'])

        # 1. Reserve asset address
        #   For liquidations, the reserve address is for the debt being repaid by liquidator. 
        #   For liquidations, the liquidation address is for collateral being liquidated and sent to the liquidator
        # 2. usrAddr
        # 3. liquidatorAddr
        # 4. iquidateCollateralAddr
        if (record.name == 'LiquidationCall'):
            reserveAddr = "0x" + item['topics'][2][26:]
            usrAddr = "0x" + item['topics'][3][26:]
            liquidatorAddr = "0x" + chunks[3][24:]
            liquidateCollateralAddr = "0x" + item['topics'][1][26:]
        else:
            reserveAddr = "0x" + item['topics'][1][26:]
            usrAddr = "0x" + item['topics'][2][26:]
            liquidatorAddr = None
            liquidateCollateralAddr = None
        
        # Find the decimals for the token and liquidation currencies
        # NOTE: can't convert address to a token ID, because we're getting all token transactions, not just for the ones we're concerned with
        # Compound can be easily filtered for specific addresses (specific contracts for each cToken), but Aave has one address, and filtering
        # would be harder.
        found1 = False
        found2 = False
        for token in tokenArr:
            if (reserveAddr == token[1]):
                reserveDecimals = token[2]
                found1 = True
            if (liquidateCollateralAddr == token[1]):
                liquidateDecimals = token[2]
                found2 = True
        # If corresponding token not found, then just assume 18 decimals. May not always be correct, but will be sufficient to record amounts
        if not found1:
            reserveDecimals = 18
        if not found2:
            liquidateDecimals = 18

        # tokenAmount
        # Must come after search loops above
        tokenAmount = self._convert_dec(chunks[0], reserveDecimals)

        # liquidateCollateralAmt
        # Must come after search loops above
        if (record.name == 'LiquidationCall'):
            liquidateCollateralAmt = self._convert_dec(chunks[1], liquidateDecimals)
        else:
            liquidateCollateralAmt = 0

        # Origination Fee
        if (record.name == 'Borrow'):
            originationFee = self._convert_dec(chunks[3], 27) # Convert to ray (take out 9 decimals)
        else:
            originationFee = 0
        
        # Append row and return the array
        allFields = [partial_list['blockTime'], partial_list['blockNumber'], partial_list['trxHash'], usrAddr, liquidatorAddr, reserveAddr, tokenAmount, originationFee, liquidateCollateralAmt, liquidateCollateralAddr, record.name]
        sqlArr.append(allFields)
        return sqlArr

    def _process_results_maker2(self, item, partial_list, record, sqlArr):
        # cdpIndex
        if (record.name == 'newCdp'):
            vaultID = int(item['topics'][3], 16)
            if (vaultID > 4294967295):
                vaultID = 0
        else:
            vaultID = None
        # If cdpIndex is larger than largest unsigned int value (in SQL), then set to 0. The value must have been erroneously entered
        
        # usrAddr
        if (record.name == 'give'):
            usrAddr = "0x" + item['topics'][3][26:]
        else:
            usrAddr = "0x" + item['topics'][2][26:]

        # amount
        if (record.name == 'frob2'):
            chunks = self._chunk_data(item['data'], [64, 64, 8, 64, 64, 64, 64, 64, 64])
            dinkWei, dinkSign = self._twos_comp(chunks[7])
            dartWei, dartSign = self._twos_comp(chunks[8])

            dinkAmt = Web3.fromWei(dinkWei, 'ether') * dinkSign
            dartAmt = Web3.fromWei(dartWei, 'ether') * dartSign
        else: # Add amounts later
            dinkAmt = 0
            dartAmt = 0

        allFields = [partial_list['blockTime'], partial_list['blockNumber'], partial_list['trxHash'], usrAddr, dinkAmt, dartAmt, vaultID, record.name]

        # Skip any duplicate rows
        if (len(sqlArr) == 0):
            sqlArr.append(allFields)
        else:
            # Only append if the value is different than the previous row
            if (sqlArr[len(sqlArr) - 1] != allFields):
                sqlArr.append(allFields)

        return sqlArr

    def _process_results_compound2(self, item, partial_list, record, sqlArr, tokenArr):
        chunks = self._chunk_data(item['data'])

        # cTokenID
        cTokenAddr = item['address']
        for token in tokenArr:
            if (cTokenAddr == token[1]):
                cTokenID = token[0]
                tokenDecimals = token[2]
                break

        # usrAddr and tokenAmount
        # NOTE: We could convert tokenAmount to negative. However, actions are simultaneously positive and negative
        # For example, depositing tokens to Compound would be negative for user's wallet, positive for Compound wallet
        # Therefore, we leave as all amounts as positive. Algorithm will decide how to handle amounts
        if (record.name in ['RepayBorrow', 'LiquidateBorrow']):
            usrAddr = "0x" + chunks[1][24:]
            tokenAmount = self._convert_dec(chunks[2], tokenDecimals)
        else:
            usrAddr = "0x" + chunks[0][24:]
            tokenAmount = self._convert_dec(chunks[1], tokenDecimals)
        
        # liquidatorAddr and liquidateCollateralAddr
        if (record.name == 'LiquidateBorrow'):
            liquidatorAddr = "0x" + chunks[0][24:]
            liquidateCollateralAddr = "0x" + chunks[3][24:]
        else:
            liquidatorAddr = None
            liquidateCollateralAddr = None           

        # cTokenAmount
        if (record.name in ['Mint', 'Redeem']):
            cTokenAmount = self._convert_dec(chunks[2], self._cTokenDecimals)
        elif (record.name == 'LiquidateBorrow'):
            cTokenAmount = self._convert_dec(chunks[4], self._cTokenDecimals) # cTokens go to liquidator
        else:
            cTokenAmount = 0

        # accountBorrowBalance, totalBorrowBalance
        if (record.name == 'Borrow'):
            accountBorrowBalance = self._convert_dec(chunks[2], tokenDecimals)
            totalBorrowBalance = self._convert_dec(chunks[3], tokenDecimals)
        elif (record.name == 'RepayBorrow'):
            accountBorrowBalance = self._convert_dec(chunks[3], tokenDecimals)
            totalBorrowBalance = self._convert_dec(chunks[4], tokenDecimals)
        else:
            accountBorrowBalance = 0
            totalBorrowBalance = 0

        # Append row and return the array
        allFields = [
            partial_list['blockTime'], partial_list['blockNumber'], partial_list['trxHash'], usrAddr, liquidatorAddr, cTokenID, 
            tokenAmount, cTokenAmount, liquidateCollateralAddr, accountBorrowBalance, totalBorrowBalance, record.name
        ]
        sqlArr.append(allFields)
        return sqlArr

    def _process_results_uniswap2(self, item, partial_list, record, sqlArr, pairArr):
        # pairID
        pairAddr = item['address']
        for pair in pairArr:
            if pairAddr == pair[1]:
                pairID = pair[0]
                token0Decimals = pair[2]
                token1Decimals = pair[3]
        
        # send/receiveAddr
        sendAddr = "0x" + item['topics'][1][26:]
        receiveAddr = "0x" + item['topics'][2][26:]

        # split data into chunks, for processing
        chunks = self._chunk_data(item['data'])

        # amounts
        amount0In = self._convert_dec(chunks[0], token0Decimals)
        amount1In = self._convert_dec(chunks[1], token1Decimals)
        amount0Out = self._convert_dec(chunks[2], token0Decimals)
        amount1Out = self._convert_dec(chunks[3], token1Decimals)

        # Append row and return the array
        allFields = [partial_list['blockTime'], partial_list['blockNumber'], partial_list['trxHash'], sendAddr, receiveAddr, pairID, amount0In, amount1In, amount0Out, amount1Out, record.name]
        sqlArr.append(allFields)
        return sqlArr

    def _process_results_aave2(self):
        pass

    def _convert_dec(self, value, decimals, positive = True):
        value1 = int(value, 16)                         # Convert hexadecimal to integer
        value2 = str(value1)                            # Convert to string
        pad_len = 18 - decimals                         # Determine how many characters to pad
        total_string_len = len(value2) + pad_len        # Pad to the right
        if decimals > 18:                               # Delete the excess values past 18 decimals. We lose ray precision, but can get decimals 
            if (total_string_len <= 0):                 
                value3 = 0                              # If the string length less than excess decimals, we set to zero
            else:
                value3 = value2[:total_string_len]
        else:
            value3 = value2.ljust(total_string_len,"0")
        value4 = int(value3)                            # Convert back to integer
        if (positive):                                  # Convert to decimal
            value5 = Web3.fromWei(value4, 'ether')      # Positive conversion
        else:
            value5 = -Web3.fromWei(value4, 'ether')     # Negative conversion

        return value5

    def _chunk_data(self, item_data, custom_length = []):
        dataField = item_data
        dataMod = dataField[2:]
        if (len(custom_length) == 0):
            n = 64
            chunks = [dataMod[k:k+n] for k in range(0, len(dataMod), n)]
        else:
            startPos = 0
            chunks = []
            for k in custom_length:
                chunks.append(dataMod[startPos:startPos+k])
                startPos += k

        return chunks

    def _twos_comp(self, hex_string):
        """compute the 2's complement of int value val"""
        decoded_value = int(hex_string, 16)
        # Only change string if negative. Positive values returned as is
        if decoded_value & (1 << (256 - 1)):
            # fromWei can only handle positive values. Therefore, we need to get the negative value (which is enclosed in parentheses), but then 
            # convert to positive. After using fromWei, the "sign" variable will be used to convert back to negative.
            decoded_value = abs(decoded_value - (1 << 256))
            sign = -1
        else:
            sign = 1
        return decoded_value, sign
    
    def __repr__(self):
        return (f'{self.__class__.__name__}({self.name}, {self.version})')

class ExternalSources:
    """
    A class representing the connection to both SQL and BigQuery

    Attributes
    ----------
    _db (Connector)             Connection to MySQL
    cursor (Cursor)             MySQL cursor
    _bq_client (Client)         Connection to BigQuery
    temp_used                   Whether the BigQuery data is already present as a temporary table
    _query (String)             Text of query to execute on Google BigCloud
    
    Methods
    -------
    __init__                    Connects to the SQL database
    create_bq_query             Creates the BigQuery query
    execute_bq_query            Executes the BigQuery query
    _job_config                 Parameters for BigQuery query
    __repr__                    Returns string output of the call by which the object was instantiated
    """
    def __init__(self):
        # Start MySQL connection
        print('1. Connecting to SQL database')
        self._db = mysql.connector.connect(
            host = secrets.sqlHost,
            user = secrets.sqlUser,
            password = secrets.sqlPass,
            database = 'defiData',
            autocommit = True
        )
        self.cursor = self._db.cursor()

        # Start the Google BigCloud connection
        self._bq_client = bigquery.Client()
    
    def create_bq_query(self, last_block, decrement, record, protocol, stage):
        # 1. Determine whether a temp table is set up for this record type
        temp_query = f"SELECT tempTable FROM bqTempTables WHERE trxType = '{record.name}'"
        self.cursor.execute(temp_query)
        temp_record = self.cursor.fetchall()

        # A. If yes, then query the temp table
        if (len(temp_record) > 0):
            print('2. Querying temp table')
            bq_table_name = temp_record[0][0]
            self.temp_used = True
        # B. If no, then execute query on live table
        else:
            print('2. Querying live table')
            bq_table_name = 'bigquery-public-data.crypto_ethereum.logs'
            self.temp_used = False

        # 2. Add custom parameters
        # If querying a temp table, and in Stage 0 or 1, then we need to LIMIT
        if (stage < 2 and self.temp_used):
            # BorrowLiquidated for Compound produces 3 rows for every record, so only use 4 records
            if (protocol.name == 'Compound' and protocol.version == 2 and record.name == 'BorrowLiquidated'):
                limit_param = ' LIMIT 4'
            # For all other records, use LIMIT 10
            else:
                limit_param = ' LIMIT 10'
        # If querying a live table, or in stage 1, then do not LIMIT
        else:
            limit_param = ''

        # If querying Maker V1 or Uniswap V1, we only need the log
        # For all others, we need the data field as well
        if (
            (protocol.name == 'Uniswap' and protocol.version == 1) or 
            (protocol.name == 'Maker' and protocol.version == 1)
        ):
            data_param = ''
        else:
            data_param = ', data'
        
        # Uniswap queries multiple addresses, so we need to append an array of addresses properly, searching with the IN condition
        if isinstance(protocol.addr, list):
            address = '('
            for addr in protocol.addr:
                address += f"address = '{addr}' OR "

            address = address[:-4] + ')'
        else:
            address = f"address = '{protocol.addr}'"

        select_params = {
            'firstBlock': last_block - decrement,
            'lastBlock': last_block,
            'signature': record.stored_method_name,
            'address': address.lower(),
            'tableName': bq_table_name,
            'limit': limit_param,
            'data': data_param,
        }

        # 3. Create query text
        self._query = """
            SELECT transaction_hash, address, topics, block_timestamp, block_number {data}
            FROM `{tableName}`
            WHERE block_number >= {firstBlock} AND block_number <= {lastBlock}
            AND {address}
            AND topics[SAFE_OFFSET(0)] = {signature}
            {limit};
        """.format(**select_params)
        print(self._query)

    def execute_bq_query(self, protocol):
        self.results = self._bq_client.query(self._query)

    def __repr__(self):
        return (f'{self.__class__.__name__}()')

class RecordExplorer:
    """
    A class representing the information we need to query the blockchain

    Attributes
    ----------
    protocol (Protocol)             An object representing a DeFi protocol
    record (record)                 An object representing the record being queried
    ex_sources (ExternalSources)    An object representing the SQL connection, as well as the SQL query that will be executed
    stage (Int)                     An integer representing the testing stage

    Methods
    -------
    set_protocol            Creates the protocol object
    set_record              Creates the record object
    set_stage               Creates the testing stage
    connect                 Creates the query object and connects to SQL database
    run_query               Creates the SQL query text and executes the query
    transform_results       Processes the results and adds information to SQL database
    print_results           Prints information about the SQL query that was processed
    print_environ           Prints information about the testing environment
    __repr__                Returns string output of the call by which the object was instantiated
    """    
    def set_protocol(self, protocol_name, protocol_version):
        self.protocol = Protocol(protocol_name, protocol_version, self.ex_sources)

    def set_record(self, record_name):
        self.record = Record(record_name, self.protocol)

    def set_stage(self, stage):
        # Filter out incorrect values for stage
        if (isinstance(stage, int)):
            if (stage <= 3 and stage >= 0):
                self.stage = stage
            else:
                raise DataValidationError('Incorrect value for "stage" variable. Please choose an integer between 0 and 3.')    
        else:
            raise DataValidationError('Incorrect, non-numeric value for "stage" variable. Please choose an integer between 0 and 3.')

    def connect(self):
        self.ex_sources = ExternalSources()
    
    def run_bq_query(self, last_block, decrement):
        # We call run_query so that we can internally pass the right parameters to the Query object, and not have to do it on the external call
        self.ex_sources.create_bq_query(last_block, decrement, self.record, self.protocol, self.stage)
        self.ex_sources.execute_bq_query(self.protocol)
        
    def transform_results(self):
        results = self.ex_sources.results

        # Pair info for UniswapV2
        if (self.protocol.name == 'Uniswap' and self.protocol.version == 2):
            temp_query = f"SELECT pairID, LOWER(pairAddr), token0Decimals, token1Decimals FROM pairsTokens"
            self.ex_sources.cursor.execute(temp_query)
            pairArr = self.ex_sources.cursor.fetchall()

        # Token info for CompoundV2
        if (self.protocol.name == 'Compound' and self.protocol.version == 2):
            temp_query = f"SELECT id, LOWER(cTokenAddr), decimals FROM addrTokens"
            self.ex_sources.cursor.execute(temp_query)
            tokenArr = self.ex_sources.cursor.fetchall()

        # Token info for AaveV1
        if (self.protocol.name == 'Aave' and self.protocol.version == 1):
            temp_query = f"SELECT id, LOWER(aReserveAddr), decimals FROM addrTokens"
            self.ex_sources.cursor.execute(temp_query)
            tokenArr = self.ex_sources.cursor.fetchall()

        # Variables needs to be set first, so we can access even if calling a live table
        j = 0          # tracks number of rows processed from BQ
        sqlArr = []    # sqlArr will be added to SQL tables

        # A. If we queried the temp table, then add the rows to SQL
        if (self.ex_sources.temp_used):
            print('3. Processing results from temp table')

            # Process events
            for item in results:
                partial_list = {
                    'blockTime': item['block_timestamp'],
                    'blockNumber': item['block_number'],
                    'trxHash': item['transaction_hash']
                }

                args = [item, partial_list, self.record, sqlArr]
                # Additional info for certain protocols
                if (self.protocol.name == 'Uniswap' and self.protocol.version == 2):
                    args.append(pairArr)
                elif (
                    (self.protocol.name == 'Compound' and self.protocol.version == 2) or 
                    (self.protocol.name == 'Aave' and self.protocol.version == 1)
                ):
                    args.append(tokenArr)
                
                sqlArr = self.protocol.process_results(*args)

                # Update # of rows (no way to know how many rows in total)
                j = j + 1
                print(j)

            # Only log query if we're in initial testing mode
            if (self.stage == 0 or self.stage == 1):
                print(sqlArr)
            # Only insert if in Stage 1 (LIMIT 10) or Stage 3 (all)
            if (self.stage == 1 or self.stage == 3):
                print('4. Recording results to local database')
                # Could do one insert query (and may be more efficient), but it would be impossible to track progress, and MySQL may time out while
                # processing very large query
                step = 100 # Optimal balance between too large queries and too many queries, per testing

                # Do SQL queries in batches. 
                for i in tqdm(range(0, len(sqlArr), step)):
                    self.ex_sources.cursor.executemany(self.protocol.insert_query, sqlArr[i:i+step])
               
        # B. If we query the live database, then just add the destination to SQL database
        else: 
            print('3. Retrieving destination from live table')
            destTable = str(results.destination)
            insertParams = {'trxType': self.record.name,
                            'tempTable': destTable}
            insert_query = "INSERT INTO bqTempTables (trxType, tempTable) VALUES ('{trxType}', '{tempTable}')".format(**insertParams)
            
            print('4. Recording destination table to local database')
            self.ex_sources.cursor.execute(insert_query)
        
        self.ex_sources.cursor.close()
        self.ex_sources.results.j = j
        self.ex_sources.results.len = len(sqlArr)
    
    def print_results(self):
        results = self.ex_sources.results

        print('-'*60)
        print(f'Created: {results.created}')
        print(f'Ended:   {results.ended}')
        print(f'Bytes:   {results.total_bytes_processed}')
        if (self.ex_sources.temp_used):
                print('-'*60)
                print(f'Number of rows processed: {results.j}')
                print(f'Number of rows added to local DB: {results.len}')

    def print_environ(self):
        noteText = f'---- Production environment for {self.protocol.name} ----\n    Live table:\n\tQueries on live table (BQ) have no limits (because of high resource utilization for live queries), and the destination table is added to tempTables (local DB)'
        # Set "Added or Not" variable, on whether values are recorded to a local database
        if (self.stage == 0 or self.stage == 2):
            added_or_not = 'are NOT'
        else: # stage 1 or 3
            added_or_not = 'are'

        # Set limit
        if (self.stage == 0 or self.stage == 1):
            # Set variables
            if (self.protocol.name == 'Compound'):
                limit_size = 'a LIMIT (4 for Liquidate, 10 for all other events)'
            else:
                limit_size = 'a LIMIT 10'
        else: # stage 2 or 3
            limit_size = 'no limit'

        noteText += f'\n    Temp table:\n\tQueries on temp table (BQ) have {limit_size}. Values from BQ {added_or_not} added to the records table (local DB)'
        print(noteText)

    def __repr__(self):
        return (f'{self.__class__.__name__}()')
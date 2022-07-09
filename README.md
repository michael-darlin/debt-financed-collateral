# Debt-Financed Collateral
## Overview
This repository documents the data analysis performed for the paper "Debt-Financed Collateral and Stability Risks in the DeFi Ecosystem" ([ArXiv](https://arxiv.org/abs/2204.11107)). All relevant data was stored in a MariaDB database; an export of the table data is available [here](https://drive.google.com/drive/folders/1QGkh0MTT-lXiGh7ofzswA7042KVGEqq_).

## Files/folders

**1-scrape**

- **Large-scale data collection (Python)** - The majority of event data was collected through Google's BigQuery service, using Python. 
    - *defiEvents.py*: collection of classes used for data collection.
    - *collectEvents.py*: script to collect data. This script was run once for each project. Code was written to collect data across multiple protocol versions (e.g. both Version 1 and Version 2 of Uniswap), but only data from the most recent protocol was used.
- **One-off data collection (NodeJS)** - These scripts were used to collect more targeted information.
    - *mkrVaults.js*: Collect data on all existing Maker vaults, including all addresses associated to the vault (owner, DSProxy, and UrnHandler addresses).
    - *mkrRateAdjust.js*: Collect information to adjust DAI amounts recorded in frob transactions. Frob transaction amounts (specifically for debt withdrawal/repayment) are recorded without interest rate adjustments. This file collects information to adjust DAI amounts according to the prevailing cumulative interest rate in the Maker Vat contract.
    - *uniExchanges.js*: Collect pair addresses from Uniswap for all tokens used in the final analysis.
    - *usdValues.js*: Collect hourly USD pricing data from Coinbase and update each transaction with the most recent USD price.

**2-transform**

*algo.ipynb* contains the algorithm used to estimate the percentage of debt-financed collateral

**3-analyze**

*analysis.r* contains the R code used to create charts and tables for the final paper.

**utils**

This folder contains several files used by NodeJS scripts:
- *abi.js*: ABIs for relevant contracts
- *addr.js*: relevant addresses
- *eventLib.js*: helper functions

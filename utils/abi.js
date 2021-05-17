/**
 * @file Records the ABI for addresses used to gather information
 */

const factoryV2 = [
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "_feeToSetter",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "constructor"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address",
          "name": "token0",
          "type": "address"
        },
        {
          "indexed": true,
          "internalType": "address",
          "name": "token1",
          "type": "address"
        },
        {
          "indexed": false,
          "internalType": "address",
          "name": "pair",
          "type": "address"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "name": "PairCreated",
      "type": "event"
    },
    {
      "constant": true,
      "inputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "name": "allPairs",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [],
      "name": "allPairsLength",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "address",
          "name": "tokenA",
          "type": "address"
        },
        {
          "internalType": "address",
          "name": "tokenB",
          "type": "address"
        }
      ],
      "name": "createPair",
      "outputs": [
        {
          "internalType": "address",
          "name": "pair",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [],
      "name": "feeTo",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [],
      "name": "feeToSetter",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        },
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "name": "getPair",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "address",
          "name": "_feeTo",
          "type": "address"
        }
      ],
      "name": "setFeeTo",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "address",
          "name": "_feeToSetter",
          "type": "address"
        }
      ],
      "name": "setFeeToSetter",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    }
  ]

  const cdpManager = [
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "vat_",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "constructor"
    },
    {
      "anonymous": true,
      "inputs": [
        {
          "indexed": true,
          "internalType": "bytes4",
          "name": "sig",
          "type": "bytes4"
        },
        {
          "indexed": true,
          "internalType": "address",
          "name": "usr",
          "type": "address"
        },
        {
          "indexed": true,
          "internalType": "bytes32",
          "name": "arg1",
          "type": "bytes32"
        },
        {
          "indexed": true,
          "internalType": "bytes32",
          "name": "arg2",
          "type": "bytes32"
        },
        {
          "indexed": false,
          "internalType": "bytes",
          "name": "data",
          "type": "bytes"
        }
      ],
      "name": "LogNote",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address",
          "name": "usr",
          "type": "address"
        },
        {
          "indexed": true,
          "internalType": "address",
          "name": "own",
          "type": "address"
        },
        {
          "indexed": true,
          "internalType": "uint256",
          "name": "cdp",
          "type": "uint256"
        }
      ],
      "name": "NewCdp",
      "type": "event"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "uint256",
          "name": "cdp",
          "type": "uint256"
        },
        {
          "internalType": "address",
          "name": "usr",
          "type": "address"
        },
        {
          "internalType": "uint256",
          "name": "ok",
          "type": "uint256"
        }
      ],
      "name": "cdpAllow",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        },
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        },
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "name": "cdpCan",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [],
      "name": "cdpi",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "name": "count",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "address",
          "name": "src",
          "type": "address"
        },
        {
          "internalType": "uint256",
          "name": "cdp",
          "type": "uint256"
        }
      ],
      "name": "enter",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "name": "first",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "bytes32",
          "name": "ilk",
          "type": "bytes32"
        },
        {
          "internalType": "uint256",
          "name": "cdp",
          "type": "uint256"
        },
        {
          "internalType": "address",
          "name": "dst",
          "type": "address"
        },
        {
          "internalType": "uint256",
          "name": "wad",
          "type": "uint256"
        }
      ],
      "name": "flux",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "uint256",
          "name": "cdp",
          "type": "uint256"
        },
        {
          "internalType": "address",
          "name": "dst",
          "type": "address"
        },
        {
          "internalType": "uint256",
          "name": "wad",
          "type": "uint256"
        }
      ],
      "name": "flux",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "uint256",
          "name": "cdp",
          "type": "uint256"
        },
        {
          "internalType": "int256",
          "name": "dink",
          "type": "int256"
        },
        {
          "internalType": "int256",
          "name": "dart",
          "type": "int256"
        }
      ],
      "name": "frob",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "uint256",
          "name": "cdp",
          "type": "uint256"
        },
        {
          "internalType": "address",
          "name": "dst",
          "type": "address"
        }
      ],
      "name": "give",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "name": "ilks",
      "outputs": [
        {
          "internalType": "bytes32",
          "name": "",
          "type": "bytes32"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "name": "last",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "name": "list",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "prev",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "next",
          "type": "uint256"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "uint256",
          "name": "cdp",
          "type": "uint256"
        },
        {
          "internalType": "address",
          "name": "dst",
          "type": "address"
        },
        {
          "internalType": "uint256",
          "name": "rad",
          "type": "uint256"
        }
      ],
      "name": "move",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "bytes32",
          "name": "ilk",
          "type": "bytes32"
        },
        {
          "internalType": "address",
          "name": "usr",
          "type": "address"
        }
      ],
      "name": "open",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "name": "owns",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "uint256",
          "name": "cdp",
          "type": "uint256"
        },
        {
          "internalType": "address",
          "name": "dst",
          "type": "address"
        }
      ],
      "name": "quit",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "uint256",
          "name": "cdpSrc",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "cdpDst",
          "type": "uint256"
        }
      ],
      "name": "shift",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "internalType": "address",
          "name": "usr",
          "type": "address"
        },
        {
          "internalType": "uint256",
          "name": "ok",
          "type": "uint256"
        }
      ],
      "name": "urnAllow",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        },
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "name": "urnCan",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "name": "urns",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [],
      "name": "vat",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    }
  ]

const dsProxy = [
    {
      "constant": false,
      "inputs": [
        {
          "name": "owner_",
          "type": "address"
        }
      ],
      "name": "setOwner",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "name": "_target",
          "type": "address"
        },
        {
          "name": "_data",
          "type": "bytes"
        }
      ],
      "name": "execute",
      "outputs": [
        {
          "name": "response",
          "type": "bytes32"
        }
      ],
      "payable": true,
      "stateMutability": "payable",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "name": "_code",
          "type": "bytes"
        },
        {
          "name": "_data",
          "type": "bytes"
        }
      ],
      "name": "execute",
      "outputs": [
        {
          "name": "target",
          "type": "address"
        },
        {
          "name": "response",
          "type": "bytes32"
        }
      ],
      "payable": true,
      "stateMutability": "payable",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [],
      "name": "cache",
      "outputs": [
        {
          "name": "",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "name": "authority_",
          "type": "address"
        }
      ],
      "name": "setAuthority",
      "outputs": [],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [],
      "name": "owner",
      "outputs": [
        {
          "name": "",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": false,
      "inputs": [
        {
          "name": "_cacheAddr",
          "type": "address"
        }
      ],
      "name": "setCache",
      "outputs": [
        {
          "name": "",
          "type": "bool"
        }
      ],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [],
      "name": "authority",
      "outputs": [
        {
          "name": "",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "name": "_cacheAddr",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "nonpayable",
      "type": "constructor"
    },
    {
      "payable": true,
      "stateMutability": "payable",
      "type": "fallback"
    },
    {
      "anonymous": true,
      "inputs": [
        {
          "indexed": true,
          "name": "sig",
          "type": "bytes4"
        },
        {
          "indexed": true,
          "name": "guy",
          "type": "address"
        },
        {
          "indexed": true,
          "name": "foo",
          "type": "bytes32"
        },
        {
          "indexed": true,
          "name": "bar",
          "type": "bytes32"
        },
        {
          "indexed": false,
          "name": "wad",
          "type": "uint256"
        },
        {
          "indexed": false,
          "name": "fax",
          "type": "bytes"
        }
      ],
      "name": "LogNote",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "name": "authority",
          "type": "address"
        }
      ],
      "name": "LogSetAuthority",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "name": "owner",
          "type": "address"
        }
      ],
      "name": "LogSetOwner",
      "type": "event"
    }
  ]

const jug = [{
  "inputs": [{
    "internalType": "address",
    "name": "vat_",
    "type": "address"
  }],
  "payable": false,
  "stateMutability": "nonpayable",
  "type": "constructor"
}, {
  "anonymous": true,
  "inputs": [{
    "indexed": true,
    "internalType": "bytes4",
    "name": "sig",
    "type": "bytes4"
  }, {
    "indexed": true,
    "internalType": "address",
    "name": "usr",
    "type": "address"
  }, {
    "indexed": true,
    "internalType": "bytes32",
    "name": "arg1",
    "type": "bytes32"
  }, {
    "indexed": true,
    "internalType": "bytes32",
    "name": "arg2",
    "type": "bytes32"
  }, {
    "indexed": false,
    "internalType": "bytes",
    "name": "data",
    "type": "bytes"
  }],
  "name": "LogNote",
  "type": "event"
}, {
  "constant": true,
  "inputs": [],
  "name": "base",
  "outputs": [{
    "internalType": "uint256",
    "name": "",
    "type": "uint256"
  }],
  "payable": false,
  "stateMutability": "view",
  "type": "function"
}, {
  "constant": false,
  "inputs": [{
    "internalType": "address",
    "name": "usr",
    "type": "address"
  }],
  "name": "deny",
  "outputs": [],
  "payable": false,
  "stateMutability": "nonpayable",
  "type": "function"
}, {
  "constant": false,
  "inputs": [{
    "internalType": "bytes32",
    "name": "ilk",
    "type": "bytes32"
  }],
  "name": "drip",
  "outputs": [{
    "internalType": "uint256",
    "name": "rate",
    "type": "uint256"
  }],
  "payable": false,
  "stateMutability": "nonpayable",
  "type": "function"
}, {
  "constant": false,
  "inputs": [{
    "internalType": "bytes32",
    "name": "ilk",
    "type": "bytes32"
  }, {
    "internalType": "bytes32",
    "name": "what",
    "type": "bytes32"
  }, {
    "internalType": "uint256",
    "name": "data",
    "type": "uint256"
  }],
  "name": "file",
  "outputs": [],
  "payable": false,
  "stateMutability": "nonpayable",
  "type": "function"
}, {
  "constant": false,
  "inputs": [{
    "internalType": "bytes32",
    "name": "what",
    "type": "bytes32"
  }, {
    "internalType": "uint256",
    "name": "data",
    "type": "uint256"
  }],
  "name": "file",
  "outputs": [],
  "payable": false,
  "stateMutability": "nonpayable",
  "type": "function"
}, {
  "constant": false,
  "inputs": [{
    "internalType": "bytes32",
    "name": "what",
    "type": "bytes32"
  }, {
    "internalType": "address",
    "name": "data",
    "type": "address"
  }],
  "name": "file",
  "outputs": [],
  "payable": false,
  "stateMutability": "nonpayable",
  "type": "function"
}, {
  "constant": true,
  "inputs": [{
    "internalType": "bytes32",
    "name": "",
    "type": "bytes32"
  }],
  "name": "ilks",
  "outputs": [{
    "internalType": "uint256",
    "name": "duty",
    "type": "uint256"
  }, {
    "internalType": "uint256",
    "name": "rho",
    "type": "uint256"
  }],
  "payable": false,
  "stateMutability": "view",
  "type": "function"
}, {
  "constant": false,
  "inputs": [{
    "internalType": "bytes32",
    "name": "ilk",
    "type": "bytes32"
  }],
  "name": "init",
  "outputs": [],
  "payable": false,
  "stateMutability": "nonpayable",
  "type": "function"
}, {
  "constant": false,
  "inputs": [{
    "internalType": "address",
    "name": "usr",
    "type": "address"
  }],
  "name": "rely",
  "outputs": [],
  "payable": false,
  "stateMutability": "nonpayable",
  "type": "function"
}, {
  "constant": true,
  "inputs": [],
  "name": "vat",
  "outputs": [{
    "internalType": "contract VatLike",
    "name": "",
    "type": "address"
  }],
  "payable": false,
  "stateMutability": "view",
  "type": "function"
}, {
  "constant": true,
  "inputs": [],
  "name": "vow",
  "outputs": [{
    "internalType": "address",
    "name": "",
    "type": "address"
  }],
  "payable": false,
  "stateMutability": "view",
  "type": "function"
}, {
  "constant": true,
  "inputs": [{
    "internalType": "address",
    "name": "",
    "type": "address"
  }],
  "name": "wards",
  "outputs": [{
    "internalType": "uint256",
    "name": "",
    "type": "uint256"
  }],
  "payable": false,
  "stateMutability": "view",
  "type": "function"
}]

module.exports = {
    factoryV2,
    cdpManager,
    dsProxy,
    jug
}
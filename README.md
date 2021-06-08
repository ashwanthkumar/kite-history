# kite-history

NodeJS script to download historical candle data from [kite.trade](https://kite.trade/docs/connect/v3/historical/) API. The idea is to build a local (personal) copy of all the data as CSV files which can later be consumed for analysis.

## Usage
When running for the first time
```
$ npm install
```

Auth token is generated as per the documentation at https://kite.trade/docs/connect/v3/user/.
```
$ KITE_AUTH="<authToken>" node --unhandled-rejections=strict index.js
```

## License
MIT

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

## Notes
1. We write a CSV file in a folder even if there isn't any data to write. We would write an empty CSV file with just headers. That is by design. We would like to know if a particular instrument is not being traded on a given day is good to know.
2. The output is written in a `data/` folder in the current folder.
3. These scripts in this folder are shared as is without any warranty. I'll probably not accept PRs unless things are broken. Please feel free to fork for any custom modifications.

## License
MIT

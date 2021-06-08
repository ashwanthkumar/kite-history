#!/usr/bin/env node

import path from 'path'
import * as fs from 'fs'
import Throttle from 'p-throttle'
import yargs from 'yargs'
import {
    hideBin
} from 'yargs/helpers'

const argv = yargs(hideBin(process.argv)).argv

import fetch from 'node-fetch'
import {
    parse
} from 'fast-csv'
import {
    writeToPath
} from '@fast-csv/format';
import dayjs from 'dayjs'

import {
    fileURLToPath
} from 'url'
const __filename = fileURLToPath(
    import.meta.url)
const __dirname = path.dirname(__filename)
const DATE_FORMAT = 'YYYY-MM-DD'
const today = dayjs()
const todayAsStr = today.format(DATE_FORMAT)

const authToken = argv.auth || process.env.KITE_AUTH
if (!authToken) {
    console.warn("Auth token not found / specified. Consider passing KITE_AUTH environment variable or --auth while running the program")
    process.exit(1)
}
const maxDays = args.maxDays || 1
if (maxDays > 60) {
    console.warn("We can't fetch data more than 60 days")
    process.exit(1)
}

// make sure the output directory exists for today
const directory = path.resolve(__dirname, "data", todayAsStr)
fs.mkdirSync(directory, {
    recursive: true
})

const getAsText = async (url) => {
    const result = await fetch(url, {
        headers: {
            "Authorization": authToken
        }
    })
    return result.text();
}

const get = async (url) => {
    console.log("Fetching " + url)
    const result = await fetch(url, {
        headers: {
            "Authorization": authToken
        }
    })
    return result.json();
}

const parseCsvWithHeaders = async (csvAsString) => {
    return new Promise((resolve, reject) => {
        const rows = []
        const stream = parse({
                headers: true
            })
            .on('error', error => reject(error))
            .on('data', row => {
                delete row.last_price // this is always 0 from the KITE API
                delete row.exchange // this is always inferred to be NFO
                delete row.exchange_token // we don't need this for history as it is useless
                rows.push(row)
            })
            .on('end', () => {
                resolve(rows)
            })

        stream.write(csvAsString)
        stream.end()
    })
}

const filterOnlyNiftyBankNiftyAndFinNiftyData = async (rows) => {
    return rows.filter(row => {
        return row.name === 'NIFTY' || row.name === 'BANKNIFTY' || row.name === 'FINNIFTY'
    })
}

const fetchAndWriteData = async (rows) => {
    // don't change these values, these are as per the limits for Kite
    // Ref - https://kite.trade/forum/discussion/2760/no-of-request-to-api#latest
    // While the max is 3 requests / second for historical API, we do 2 just to be under the radar
    const throttledFetch = Throttle({
        limit: 2,
        interval: 1000
    })(async (row, token, fromDate, toDate) => {
        const result = await get(`https://api.kite.trade/instruments/historical/${token}/minute?from=${fromDate}&to=${toDate}&oi=1`);
        if (!result.data) {
            throw new Error(result.message)
        }

        return {
            ...result,
            ...row,
        }
    })

    const requests = rows.map(row => {
        // This 60 days would be translated to 1 day after the initial dump
        const fromDate = today.subtract(60, 'days').format(DATE_FORMAT)
        const toDate = todayAsStr
        return throttledFetch(row, row.instrument_token, fromDate, toDate).then(writeDataToCsv)
    })

    // Don't remove the tickers which doesn't have any data.
    // The fact that there aren't any data available is also a good information to record.
    return Promise.all(requests)
}

// unlike the other method we call this after each fetch so we can write
// as we fetch and not wait unitl we've fetched everything.
// reduces the memory usage of the program
const writeDataToCsv = async (instrument) => {
    const filename = `${instrument.name}_${instrument.tradingsymbol}_${instrument.expiry}.csv`
    const fullPath = path.resolve(directory, filename)

    await new Promise((resolve, reject) => {
        writeToPath(fullPath, instrument.data.candles, {
                alwaysWriteHeaders: true,
                headers: ["timestamp", "open", "high", "low", "close", "volume", "oi"]
            })
            .on('error', err => reject(err))
            .on('finish', () => resolve())
    })

    console.log(`Done writing ${filename}`)
}


console.log(`Authorization: ${authToken}`)

getAsText("https://api.kite.trade/instruments/NFO")
    .then(parseCsvWithHeaders)
    .then(filterOnlyNiftyBankNiftyAndFinNiftyData)
    .then(fetchAndWriteData)

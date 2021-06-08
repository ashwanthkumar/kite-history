#!/usr/bin/env node --unhandled-rejections=strict

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
const maxDays = argv.maxDays || 1
if (maxDays > 60) {
    console.warn("We can't fetch data more than 60 days")
    process.exit(1)
}
const maxFetchRetries = argv.maxFetchRetries || 3
if (maxFetchRetries < 1) {
    console.warn("--maxFetchRetries can't be a negative number")
    process.exit(1)
}

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
        const instruments = []
        const stream = parse({
                headers: true
            })
            .on('error', error => reject(error))
            .on('data', row => {
                delete row.last_price // this is always 0 from the KITE API
                delete row.exchange // this is always inferred to be NFO
                delete row.exchange_token // we don't need this for history as it is useless after it's expiry
                instruments.push(row)
            })
            .on('end', () => {
                resolve(instruments)
            })

        stream.write(csvAsString)
        stream.end()
    })
}

const filterOnlyNiftyBankNiftyAndFinNiftyData = async (instruments) => {
    return instruments.filter(instrument => {
        return instrument.name === 'NIFTY' || instrument.name === 'BANKNIFTY' || instrument.name === 'FINNIFTY'
    })
}

const fetchAndWriteData = async (instruments) => {
    // don't change these values, these are as per the limits for Kite
    // Ref - https://kite.trade/forum/discussion/2760/no-of-request-to-api#latest
    // While the max is 3 requests / second for historical API, we do 2 just to be under the radar
    const throttledFetch = Throttle({
        limit: 2,
        interval: 1000
    })(async (row, token, fromDate, toDate, fetchAttempt) => {
        const url = `https://api.kite.trade/instruments/historical/${token}/minute?from=${fromDate}&to=${toDate}&oi=1`
        try {
            const result = await get(url);
            if (!result.data) {
                throw new Error(result.message)
            }

            return {
                ...result,
                ...row,
            }
        } catch (err) {
            if (fetchAttempt < 3) {
                console.warn(`Attempting to fetch ${url} once again. We'll try it ${3 - fetchAttempt} more times, before bailing out.`)
                return await throttledFetch(row, token, fromDate, toDate, fetchAttempt + 1 || 2)
            } else {
                throw err
            }
        }
    })

    const requests = instruments.slice(0, 5).filter(instrument => {
        const directory = directoryForInstrument(instrument)
        const markerFile = markerFileForInstrument(instrument)
        return !fs.existsSync(path.resolve(directory, markerFile))
    }).map(instrument => {
        // TODO: We should at somepoint support ability to fetch more data more than 60 days by spliting it into multiple requests of 60 days each?
        const fromDate = today.subtract(maxDays, 'days').format(DATE_FORMAT)
        const toDate = todayAsStr
        return throttledFetch(instrument, instrument.instrument_token, fromDate, toDate, 1).then(writeData)
    })

    // Don't remove the tickers which doesn't have any data.
    // The fact that there aren't any data available is also a good information to record.
    return Promise.all(requests)
}

const directoryForInstrument = (instrument) => path.resolve(__dirname, "data", instrument.name, todayAsStr)
const dataFileForInstrument = (instrument) => `${instrument.name}_${instrument.tradingsymbol}_${instrument.expiry}.csv`
const markerFileForInstrument = (instrument) => `${dataFileForInstrument(instrument)}.done`

// unlike the other method we call this after each fetch so we can write
// as we fetch and not wait unitl we've fetched everything.
// reduces the memory usage of the program
const writeData = async (instrument) => {
    const directory = directoryForInstrument(instrument)
    fs.mkdirSync(directory, {
        recursive: true
    })

    const dataFilename = dataFileForInstrument(instrument)
    const markerFilename = path.resolve(directory, markerFileForInstrument(instrument))
    const dataFullPath = path.resolve(directory, dataFilename)

    await new Promise((resolve, reject) => {
        writeToPath(dataFullPath, instrument.data.candles, {
                alwaysWriteHeaders: true,
                headers: ["timestamp", "open", "high", "low", "close", "volume", "oi"]
            })
            .on('error', err => reject(err))
            .on('finish', () => {
                // write the marker once the data is written properly to disk
                fs.closeSync(fs.openSync(markerFilename, 'w'))
                resolve()
            })
    })

    console.log(`Done writing ${dataFilename}`)
}

// Entry point of the program
getAsText("https://api.kite.trade/instruments/NFO")
    .then(parseCsvWithHeaders)
    .then(filterOnlyNiftyBankNiftyAndFinNiftyData)
    .then(fetchAndWriteData)
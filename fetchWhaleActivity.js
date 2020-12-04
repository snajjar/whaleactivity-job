const mongoose = require("mongoose");
const axios = require("axios");
const moment = require("moment");
const _ = require("lodash");

/**
 *  DB Connection
 */

const DB_USER = "web1";
const DB_SEC = "<stakepoolfrM>";
const MONGO_URI = `mongodb+srv://${DB_USER}:${DB_SEC}@stakepool.sijbk.mongodb.net/spfr?retryWrites=true&w=majority`;

const dbConnect = () => {
    return new Promise((resolve) => {
        mongoose.connect(MONGO_URI, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
            useFindAndModify: false,
            useCreateIndex: true,
        });
        let db = mongoose.connection;
        db.on("error", (err) => {
            console.error("MongoDB connection error: ", err);
        });
        db.once("open", function callback() {
            console.log("MongoDB Connected.");
            resolve();
        });
    });
}




/**
 *   WhaleActivity Mongoose schema
 */
const Schema = mongoose.Schema;
const WhaleActivitySchema = new Schema({
    // DETAILS
    hash: { type: String, required: true, index: { unique: true } },
    bockchain: { type: String, required: true },
    symbol: { type: String, required: true },
    transactionType: { type: String, required: true },
    timestamp: { type: Number, required: true },
    amount: { type: Number, required: true },
    amountUSD: { type: Number, required: true },

    // FROM
    fromAddress: { type: String, required: true },
    fromOwner: { type: String },
    fromOwnerType: { type: String, required: true },

    // TO
    toAddress: { type: String, required: true },
    toOwner: { type: String },
    toOwnerType: { type: String, required: true },
});

// document expires after 15 days
WhaleActivitySchema.index({ createdAt: 1 }, { expireAfterSeconds: 15 * 24 * 60 * 60 });
const WhaleActivity = mongoose.model('WhaleActivity', WhaleActivitySchema);


/**
 *  Fetcher code
 */

const WHALE_ALERT_API_KEY = "QyCRjXy8wfo3VSfTFX2xqOnFa6aC6rzM";
const whaleAlertBaseAPIUrl = `https://api.whale-alert.io/v1/transactions?api_key=${WHALE_ALERT_API_KEY}&min_value=500000`;


const sleep = async (n) => {
    return new Promise((r) => setTimeout(r, n * 1000));
};

const fetchWhaleData = async (start) => {
    let end = moment(start).add(10, "minutes");

    // fetch 10 min of data, the max we have on that API0
    let startTimestamp = start.unix();
    let endTimestamp = end.unix();

    console.log(`Fetching transactions from ${start.format("HH:mm")} to ${end.format("HH:mm")}`);

    // build url
    let params = [`&start=${startTimestamp}`, `&end=${endTimestamp}`].join("");
    let url = whaleAlertBaseAPIUrl + params;

    // fetch data for all cursors
    let transactions = [];
    let cursor = null;
    do {
        let cursorUrl = cursor === null ? url : url + `&cursor=${cursor}`;
        let response = await axios.get(cursorUrl);
        let result = _.get(response, ["data", "result"]);
        if (result === "success") {
            let data = response.data;
            transactions = transactions.concat(data.transactions || []);

            if (data.cursor && data.cursor != cursor) {
                cursor = data.cursor;
                await sleep(6);
            } else {
                cursor = null;
            }
        } else {
            console.error(result.data);
        }
    } while (cursor !== null);

    console.log("Got " + transactions.length + " transactions");
    return transactions;
};

const saveWhaleData = async (transactions) => {
    for (let t of transactions) {
        let existingActivity = await WhaleActivity.findOne({ hash: t.hash });
        if (!existingActivity) {
            try {
                let activity = new WhaleActivity({
                    hash: t.hash,
                    bockchain: t.blockchain,
                    symbol: t.symbol,
                    transactionType: t.transaction_type,
                    timestamp: t.timestamp,
                    amount: t.amount,
                    amountUSD: t.amount_usd,
                    // FROM
                    fromAddress: _.get(t, ["from", "address"]),
                    fromOwner: _.get(t, ["from", "owner"]),
                    fromOwnerType: _.get(t, ["from", "owner_type"]),
                    // TO
                    toAddress: _.get(t, ["to", "address"]),
                    toOwner: _.get(t, ["to", "owner"]),
                    toOwnerType: _.get(t, ["to", "owner_type"]),
                });

                await activity.save();
            } catch (e) {
                console.log("Error saving WhaleActivity " + err);
                console.log(t);
            }
        }
    }
};

let lastTimestampFetched = null;
const handleDataFetching = async () => {
    let now = moment();

    // first, set the start timestamp
    if (lastTimestampFetched === null) {
        lastTimestampFetched = await getFetchBeginTimestamp();
    }

    // if last fetch was more than 10 minutes ago
    if (now - lastTimestampFetched > 20 * 60 * 1000) {
        let transactions = await fetchWhaleData(lastTimestampFetched);
        await saveWhaleData(transactions);
        lastTimestampFetched = lastTimestampFetched.add(10, "minutes");
        console.log("fetching again in 6s");
        setTimeout(handleDataFetching, 6 * 1000); // 6s timeout
    } else {
        console.log("fetching again in 20m");
        setTimeout(handleDataFetching, 20 * 60 * 1000); // 20min timeout
    }
};

// return the best date between last fetch and (now - 1 hour)
const getFetchBeginTimestamp = async () => {
    return moment().subtract(59, "minutes"); // max history for free plan: 1h
};

const start = async () => {
    await dbConnect();
    await handleDataFetching();
};

start();

import fs from "fs";
import path from "path";
import * as csv from "fast-csv";
import moment from "moment";
import sql from "mssql";
import * as emoji from "node-emoji";
import vader from "vader-sentiment";

function extractColumns(filename, destination, columns, separator = ",") {
    const rs = fs.createReadStream(filename);
    const parser = csv.parse({ headers: true, delimiter: separator });
    rs.pipe(parser);
    parser.on("data", (row) => {
        const record = {
            date: null,
            user: null,
            text: null,
            cleanedText: null,
            score: null,
            polarity: null,
        };
        Object.keys(columns).forEach((column) => {
            record[column] = row[columns[column]];
        });
        record.date = moment(new Date(record.date)).format("YYYY-MM-DD HH:mm:ss");
        record.user = record.user.toLowerCase();
        record.cleanedText = clean(record.text);
        const sentiment = evaluateSentiment(record.cleanedText);
        record.score = sentiment.score;
        record.polarity = sentiment.polarity;
        const text = `${record.date},${record.user},${record.text},${record.cleanedText},${record.score},${record.polarity}\n`;
        fs.appendFileSync(destination, text);
    });
    return new Promise((resolve, reject) => {
        parser.on("end", resolve);
        parser.on("error", (error) => reject(error));
    });
}

function clean(text) {
    const emojiRegex = /[\u{1F300}-\u{1F6FF}\u{1F900}-\u{1F9FF}\u{2600}-\u{26FF}\u{2700}-\u{27BF}]/gu;
    const matches = text.match(emojiRegex);
    matches &&
        matches.forEach((match) => {
            const name = emoji.which(match);
            text = text.replace(match, `:${name}:`);
        });
    return text
        .replace(/'/g, "`")
        .replace(/"/g, "``")
        .replace(/,/g, " ")
        .replace(/#(\w+)/g, (_, word) => `#${word.toLowerCase()}`)
        .replace(/https?:\/\/\S+/g, "")
        .replace(/@(\w+)/g, (_, word) => `@${word.toLowerCase()}`)
        .replace(/\$(\w+)/g, (_, word) => `$${word.toLowerCase()}`)
        .split("\n")
        .join(" ")
        .replace(/\s+/g, " ")
        .replace(/\.{3,}/g, "…")
        .replace(/…+/g, "…")
        .trim();
}

function evaluateSentiment(text) {
    let sentiment = vader.SentimentIntensityAnalyzer.polarity_scores(text);
    return { score: (sentiment.compound + 1) / 2, polarity: sentiment.compound };
}

const finalDestination = path.resolve(process.cwd(), "datasets", "final.csv");
fs.writeFileSync(finalDestination, "date,user,text,cleaned_text,score,polarity\n");
await extractColumns("datasets/tweets01.csv", finalDestination, {
    date: "Date",
    user: "Screen_name",
    text: "Tweet",
});
await extractColumns(
    "datasets/tweets02.csv",
    finalDestination,
    {
        date: "timestamp",
        user: "user",
        text: "text",
    },
    ";"
);
await extractColumns(
    "datasets/tweets03.csv",
    finalDestination,
    {
        date: "date",
        user: "user_name",
        text: "text",
    },
    ";"
);

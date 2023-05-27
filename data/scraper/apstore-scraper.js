const appStore = require('app-store-scraper');
const fs = require('fs');
const config = require('./config.js');
const MongoClient = require('mongodb').MongoClient;

async function getReviews(appId) {
    const mongoUrl = config.mongoDB.url;
    const dbName = config.mongoDB.dbName;
    const collectionName = config.mongoDB.collectionName;

    let reviews = [];
    let page = 0;

    try {
        const client = await MongoClient.connect(mongoUrl);
        const db = client.db(dbName);
        const collection = db.collection(collectionName);

        // Check if the collection is empty (no existing documents)
        const count = await collection.countDocuments();
        if (count === 0) {
        console.log('No existing documents in the collection. Starting scraping...');
        } else {
        console.log('Existing documents found in the collection. Skipping scraping...');
        }

        while (true) {
        const result = await appStore.reviews({
            appId: appId,
            page: page,
        });

        if (result.length === 0) {
            break;
        }

        const existingReviews = await collection.find({
            id: { $in: result.map(review => review.id) },
            source: 'apple-app-store'
        }).toArray();

        if (existingReviews.length > 0) {
            console.log('Existing reviews found. Stopping scraping...');
            break;
        }

        const newReviews = result.filter(review => {
            return !existingReviews.some(existingReview =>
            existingReview.id === review.id &&
            existingReview.source === 'apple-app-store'
            );
        });

        const reviewsWithMetadata = newReviews.map(review => ({
            id: review.id,
            userName: review.userName,
            userUrl: review.userUrl,
            version: review.version,
            score: review.score,
            title: review.title,
            text: review.text,
            url: review.url,
            scrapeDatetime: new Date(),
            source: 'apple-app-store'
        }));

        if (reviewsWithMetadata.length > 0) {
            console.log(`Inserting ${reviewsWithMetadata.length} new reviews into MongoDB...`);
            await collection.insertMany(reviewsWithMetadata);
        } else {
            console.log('No new reviews to insert into MongoDB.');
        }

        reviews = reviews.concat(newReviews);
        page++;
        }

        client.close();
    } catch (error) {
        console.log("Error fetching or inserting reviews:", error);
    }

    return reviews;
}

getReviews(config.appleAppStore.appId).then((reviews) => {
  fs.writeFileSync('reviews_appstore.json', JSON.stringify(reviews));
});

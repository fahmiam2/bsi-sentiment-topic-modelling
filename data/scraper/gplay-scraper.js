const gplay = require('google-play-scraper');
const fs = require('fs');
const config = require('./config.js');
const MongoClient = require('mongodb').MongoClient;

async function getReviews(appId) {
  const mongoUrl = config.mongoDB.mongoUrl;
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
      const result = await gplay.reviews({
        appId: appId,
        page: page,
        sort: gplay.sort.NEWEST,
      });

      if (result.length === 0) {
        break;
      }

      const existingReviews = await collection.find({
        date: { $in: result.map(review => review.date) },
        source: 'google-play-store'
      }).toArray();

      if (existingReviews.length > 0) {
        console.log('Existing reviews found. Stopping scraping...');
        break;
      }

      const newReviews = result.filter(review => {
        return !existingReviews.some(existingReview =>
          existingReview.date === review.date &&
          existingReview.source === 'google-play-store'
        );
      });

      const reviewsWithMetadata = newReviews.map(review => ({
        ...review,
        scrapeDatetime: new Date(),
        source: 'google-play-store'
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

getReviews(config.googlePlay.appId).then((reviews) => {
  fs.writeFileSync('./data/raw/reviews_gplay.json', JSON.stringify(reviews));
});
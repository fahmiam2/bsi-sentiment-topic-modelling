const gplay = require('google-play-scraper');
const fs = require('fs');
const config = require('./config.js');
const MongoClient = require('mongodb').MongoClient;
const path = require('path');

// Function to invoke the first scraper when the document is not available in the database
async function firstScraper() {
  try {
    const result = await gplay.reviews({
      appId: config.googlePlay.appId,
      sort: gplay.sort.NEWEST,
      num: 30000,
      country: 'id'
    });

    return result;
  } catch (error) {
    console.log('Error during first scraper:', error);
    throw error;
  }
}

// Function to change column names and add additional columns to the reviews data
function transformData(result, sourceApp, scrapedAt) {
  const reviews = result.data; // Access the array of reviews from the 'data' property

  console.log('Reviews:', reviews); // Log the reviews data
  console.log('Type of reviews:', typeof reviews); // Log the type of reviews

  return reviews.map(review => ({
    reviewAppId: review.id,
    reviewDatetime: review.date,
    userUrl: review.url,
    replyDatetime: review.replyDate,
    sourceApp,
    scrapedAt,
    ...review
  }));
}

// Function to ingest data into MongoDB and export JSON file
async function ingestData(reviews, sourceApp, scrapedDateAt) {
  const mongoUrl = config.mongoDB.mongoUrl;
  const dbName = config.mongoDB.dbName;
  const collectionName = config.mongoDB.collectionName;
  const filePath = `./data/raw/reviews_gplay_${scrapedDateAt}.json`;

  try {
    const client = await MongoClient.connect(mongoUrl);
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const reviewIds = reviews.map(review => review.reviewAppId); // Extracting the review IDs from the reviews

    // Find existing documents with the same IDs in the collection
    const existingDocuments = await collection.find({ reviewAppId: { $in: reviewIds } }).toArray();

    // Filter out the reviews that have already been ingested
    const filteredReviews = reviews.filter(review => !existingDocuments.some(doc => doc.reviewAppId === review.reviewAppId));

    // Insert new documents into the collection
    await collection.insertMany(filteredReviews);

    try {
      // Create the directory if it doesn't exist
      const directory = path.dirname(filePath);
      if (!fs.existsSync(directory)) {
        fs.mkdirSync(directory, { recursive: true });
      }
  
      // Export reviews data as JSON
      fs.writeFileSync(filePath, JSON.stringify(filteredReviews));
  
      console.log(`Data exported as JSON: ${filePath}`);
    } catch (error) {
      console.log('Error during data ingestion:', error);
    }
    console.log(`Data ingested into MongoDB and exported as JSON: ${filePath}`);

    client.close(); // Close the MongoDB connection

  } catch (error) {
    console.log('Error during data ingestion:', error);
  }
}

// Function to get the last datetime from the database for a given source
async function getLastDatetime(sourceApp) {
  const mongoUrl = config.mongoDB.mongoUrl;
  const dbName = config.mongoDB.dbName;
  const collectionName = config.mongoDB.collectionName;

  try {
    const client = await MongoClient.connect(mongoUrl);
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // Find the most recent document with the given source
    const query = { sourceApp };
    const sort = { reviewDatetime: -1 };
    const projection = { reviewDatetime: 1 };
    const options = { limit: 1 };
    const result = await collection.findOne(query, { sort, projection, ...options });

    if (result) {
      return result.reviewDatetime;
    }
  } catch (error) {
    console.log('Error while fetching last datetime from MongoDB:', error);
  }

  return null;
}

// Function to scrape reviews data using pagination
async function paginationScraper(lastDatetime) {
  let page = 0;
  let stopScraping = false;
  const sourceApp = 'google play store';
  const scrapedAt = new Date().toISOString();

  while (!stopScraping) {
    try {
      const result = await gplay.reviews({
        appId: config.googlePlay.appId,
        page,
        sort: gplay.sort.NEWEST,
      });

      if (!Array.isArray(result)) {
        console.log('Invalid result from pagination scraper. Expected an array:', result);
        throw new Error('Invalid result from pagination scraper');
      }

      const transformedData = transformData(result, sourceApp, scrapedAt);

      // Check if the current review's datetime is less than or equal to the last datetime in the database
      if (result.length > 0 && result[result.length - 1].date <= lastDatetime) {
        stopScraping = true;
        console.log('Reached the last datetime in the database. Stopping pagination scraping.');
      } else {
        // Ingest the transformed data into MongoDB
        await ingestData(transformedData, sourceApp, scrapedDateAt);
        page++;
      }
    } catch (error) {
      console.log('Error during pagination scraper:', error);
      throw error;
    }
  }
}

// Main function to check document availability and perform scraping accordingly
async function scrapeReviews() {
  try {
    const sourceApp = 'google play store';
    const lastDatetime = await getLastDatetime(sourceApp);

    console.log('Last datetime in the database:', lastDatetime);

    let scrapedAt, scrapedDateAt, result, transformedData;

    if (lastDatetime) {
      console.log('Existing documents found in the collection. Continuing scraping...');
      scrapedAt = new Date().toISOString();
      scrapedDateAt = scrapedAt.split('T')[0];
      result = await paginationScraper(lastDatetime);
    } else {
      console.log('No existing documents in the collection. Starting scraping...');
      scrapedAt = new Date().toISOString();
      scrapedDateAt = scrapedAt.split('T')[0];
      result = await firstScraper();
    }

    transformedData = transformData(result, sourceApp, scrapedAt);

    // Ingest the transformed data into MongoDB
    await ingestData(transformedData, sourceApp, scrapedDateAt);

    console.log('Scraping completed.');
  } catch (error) {
    console.log('Error during scraping:', error);
    throw error;
  }
}

// Invoke the main function to start scraping
scrapeReviews().catch(error => console.log('Error during scraping:', error));
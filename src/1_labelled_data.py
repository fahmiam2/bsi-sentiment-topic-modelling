import pandas as pd
import numpy as np
import logging
import json
import re
import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory
from pymongo import MongoClient

# Set-up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataLabeller:
    def __init__(self, unlabelled_data_file, positive_lexicon_file, negative_lexicon_file, mongodb_uri):
        self.unlabelled_data_file = unlabelled_data_file
        self.positive_lexicon_file = positive_lexicon_file
        self.negative_lexicon_file = negative_lexicon_file
        self.mongodb_uri = mongodb_uri

    def load_unlabelled_data(self):
        with open(self.unlabelled_data_file, "r") as f:
            json_file = json.load(f)
        df = pd.DataFrame(json_file)
        return df

    def preprocess_text(self, text):
        text = text.lower()
        text = re.sub(r'@[A-Za-z0-9]+', '', text)
        text = re.sub(r'#[A-Za-z0-9]+', '', text)
        text = re.sub(r'RT[\s]', '', text)
        text = re.sub(r"http\S+", '', text)
        text = re.sub(r'[0-9]+', '', text)
        text = text.replace('\n', ' ')
        text = text.translate(str.maketrans('', '', string.punctuation))
        text = text.strip(' ')
        return text

    def tokenize_text(self, text):
        tokens = nltk.word_tokenize(text)
        return tokens

    def filter_stopwords(self, tokens):
        stop_words = set(stopwords.words('indonesian'))
        filtered_tokens = [token for token in tokens if token not in stop_words]
        return filtered_tokens

    def stem_text(self, tokens):
        factory = StemmerFactory()
        stemmer = factory.create_stemmer()
        stemmed_tokens = [stemmer.stem(token) for token in tokens]
        return stemmed_tokens

    def load_lexicon(self, lexicon_file):
        lexicon = pd.read_csv(lexicon_file, delimiter='\t')
        lexicon['word'] = lexicon['word'].apply(self.stem_word)
        weights = dict(zip(lexicon['word'], lexicon['weight']))
        return weights

    def stem_word(self, word):
        factory = StemmerFactory()
        stemmer = factory.create_stemmer()
        return stemmer.stem(word)

    def calculate_polarity(self, tokens, positive_weights, negative_weights):
        polarity_score = sum(positive_weights.get(token, 0) + negative_weights.get(token, 0) for token in tokens)
        return polarity_score

    def label_data(self, df, positive_weights, negative_weights):
        df['polarity_inset'] = df['text_preprocessed_stemmed'].apply(
            lambda tokens: self.calculate_polarity(tokens, positive_weights, negative_weights)
        )
        df["label_inset"] = np.where(df["polarity_inset"] > 0, "positive", "negative")
        df["label_inset"] = np.where(df["polarity_inset"] == 0, "neutral", df["label_inset"])

        df["label_inset_score"] = np.where(df["polarity_inset"] > 0, "positive", "negative")
        df["label_inset_score"] = np.where(df["polarity_inset"] == 0, "neutral", df["label_inset_score"])
        df["label_inset_score"] = np.where((df["polarity_inset"] > 0) & (df["score"] <= 3), "neutral",
                                           df["label_inset_score"])
        df["label_inset_score"] = np.where((df["polarity_inset"] < 0) & (df["score"] >= 4), "neutral",
                                           df["label_inset_score"])
        df["label_inset_score"] = np.where((df["polarity_inset"] == 0) & (df["score"] == 5), "positive",
                                           df["label_inset_score"])
        df["label_inset_score"] = np.where((df["polarity_inset"] == 0) & (df["score"] == 1), "negative",
                                           df["label_inset_score"])

        return df

    def convert_to_json(self, df):
        return df.to_json(orient="records")

    def ingest_data_to_mongodb(self, json_data, collection_name):
        client = MongoClient(self.mongodb_uri)
        db = client.get_default_database()
        collection = db[collection_name]
        collection.insert_many(json_data)

    def label_data_pipeline(self):
        try:
            df = self.load_unlabelled_data()

            df['text_clean'] = df['text'].apply(self.preprocess_text)
            df['text_preprocessed'] = df['text_clean'].apply(self.tokenize_text)
            df['text_preprocessed'] = df['text_preprocessed'].apply(self.filter_stopwords)
            df["text_preprocessed_stemmed"] = df["text_preprocessed"].apply(self.stem_text)

            positive_weights = self.load_lexicon(self.positive_lexicon_file)
            negative_weights = self.load_lexicon(self.negative_lexicon_file)

            df = self.label_data(df, positive_weights, negative_weights)  # Update this line

            label_inset_counts = df.groupby("label_inset")["id"].count()
            label_inset_score_counts = df.groupby("label_inset_score")["id"].count()

            return df, label_inset_counts, label_inset_score_counts
        
        except Exception as e:
            logging.error("An error occurred during the label_data_pipeline: %s", str(e))
            return None, None, None

if __name__ == "__main__":
    try:
        # Connection details
        mongo_uri = "mongodb://localhost:27017"  # Replace with your MongoDB URI
        db_name = "your_database_name"  # Replace with your database name
        collection_name = "your_collection_name"  # Replace with your collection name

        # Example usage
        data_labeller = DataLabeller("../data/raw/unlabelled_data.json", "../data/inset-lexicon/positive.tsv",
                                     "../data/inset-lexicon/negative.tsv", mongo_uri)
        df, label_inset_counts, label_inset_score_counts = data_labeller.label_data_pipeline()

        if df is not None:
            # Ingest data into MongoDB
            json_data = data_labeller.convert_to_json(df)
            data_labeller.ingest_data_to_mongodb(json_data, collection_name)
            logging.info("Data ingested into MongoDB successfully.")

            # Print the resulting DataFrame and counts
            print(df)
            print(label_inset_counts)
            print(label_inset_score_counts)
        else:
            logging.error("Labeling process failed. Check the error logs for more details.")

    except Exception as e:
        logging.error("An error occurred during the execution: %s", str(e))
import json
from elasticsearch7 import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch(["http://localhost:9200"])

# Index name
index_name = 'autocomplete_index'

# Vocabulary file available here: https://github.com/dwyl/english-words/blob/master/words_dictionary.json
vocabulary_file = 'words_dictionary.json'

def create_elasticsearch_index():
    # Create the index
    settings = {
        "analysis": {
            "filter": {
                "autocomplete_filter": {
                    "type": "edge_ngram",
                    "min_gram": 1,
                    "max_gram": 20
                }
            },
            "analyzer": {
                "autocomplete_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "autocomplete_filter"]
                }
            }
        }
    }

    mappings = {
        "properties": {
            "suggest_field": {
                "type": "completion",
                "analyzer": "autocomplete_analyzer",
                "search_analyzer": "standard"
            }
        }
    }

    # Create an index with the defined settings and mappings
    try:
        es.indices.create(index=index_name, body={"settings": settings, "mappings": mappings})
        print(f"Index '{index_name}' created successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")

def index_data():
    with open(vocabulary_file, 'r') as f:
        vocabulary = json.load(f)
        bulk_data = []
        bulk_data.append({"index": {"_index": index_name}})
        bulk_data.append({
             "suggest_field": {
                 "input": word
              }
            })
        try:
            # Use the Elasticsearch bulk API to efficiently index all the words
            res = es.bulk(index=index_name, body=bulk_data, refresh=True)
            print(f"Indexed {res['items']}")
        except Exception as e:
            print(f"An error occurred while indexing data: {e}")

def autocomplete_query(prefix):
    query = {
        "suggest": {
            "word_suggestion": {
                "prefix": prefix,
                "completion": {
                    "field": "suggest_field",
                    "size": 10
                }
            }
        }
    }
    try:
        response = es.search(index=index_name, body=query)
        suggestions = [option['text'] for option in response['suggest']['word_suggestion'][0]['options']]
        return suggestions
    except Exception as e:
        print(f"An error occurred while executing autocomplete query: {e}")

es.indices.delete(index='autocomplete_index', ignore=[400, 404])

# Create the index
create_elasticsearch_index()

# Index the data
index_data()

# Example autocomplete query
prefix = "happ"
results = autocomplete_query(prefix)
print(f"Autocomplete suggestions for '{prefix}': {results}")

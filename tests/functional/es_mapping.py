from typing_extensions import TypedDict

ESIndexMapping = TypedDict('ESIndexMapping', {
    'mappings': dict,
    'settings': dict
})

def get_es_index_mapping() -> ESIndexMapping:
    return {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "title": {"type": "text", "analyzer": "standard"},
                "description": {"type": "text", "analyzer": "standard"},
                "genre": {
                    "type": "nested",
                    "properties": {
                        "id": {"type": "keyword"},
                        "name": {"type": "text", "analyzer": "standard"},
                    },
                },
                "actors": {
                    "type": "nested",
                    "properties": {
                        "id": {"type": "keyword"},
                        "full_name": {"type": "text", "analyzer": "standard"},
                    },
                },
                "writers": {
                    "type": "nested",
                    "properties": {
                        "id": {"type": "keyword"},
                        "full_name": {"type": "text", "analyzer": "standard"},
                    },
                },
                "directors": {
                    "type": "nested",
                    "properties": {
                        "id": {"type": "keyword"},
                        "full_name": {"type": "text", "analyzer": "standard"},
                    },
                },
                "imdb_rating": {"type": "float"},
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        },
    }
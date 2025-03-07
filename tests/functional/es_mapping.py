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


def get_persons_index_mapping() -> ESIndexMapping:
    return {
        "mappings": {
            "properties": {
                "uuid": {
                    "type": "keyword",
                    "description": "Уникальный идентификатор персоны"
                },
                "full_name": {
                    "type": "text",
                    "description": "Полное имя персоны",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    }
                },
                "films": {
                    "type": "nested",
                    "description": "Роли персоны в фильмах",
                    "properties": {
                        "uuid": {
                            "type": "keyword",
                            "description": "Уникальный идентификатор фильма"
                        },
                        "roles": {
                            "type": "nested",
                            "description": "Роли персоны в фильме",
                            "properties": {
                                "role": {
                                    "type": "keyword"
                                }
                            }
                        }
                    }
                }
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        },
    }

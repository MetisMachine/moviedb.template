KEYSPACE = "ea28b544e93cff97e42b770e"

INFO_SCHEMA = {
    "namespace": KEYSPACE,
    "table_name": "movie_info",
    "options": {
        "primary_key": ["movie_id"],
    },
    "columns": {
        "movie_id": "text",
        "imdb_id": "text",
        "movie_title": "text",
        "release_date": "date",
        "language": "text",
        "length": "double",
        "poster_path": "text",
        "adult": "boolean",
        "genres_id": "set<text>",
        "description": "text"
    }
}

MOVIE_SCHEMA = {
    "namespace": KEYSPACE,
    "table_name": "movie_list_pop_sorted",
    "options": {
        "primary_key": ["ingest_date", "popularity", "movie_id"],
        "order_by": ["popularity desc"]
    },
    "columns": {
        "movie_id": "text",
        "ingest_date": "date",
        "popularity": "float",
        "movie_title": "text",
        "adult": "boolean",
        "video": "boolean"
    }
}

RECOMMEND_SCHEMA = {
    "namespace": KEYSPACE,
    "table_name": "recommendations",
    "options": {
        "primary_key": ["user_id", "rank"],
        "order_by": ["rank asc"]
    },
    "columns": {
        "user_id": "text",
        "rank": "int",
        "movie_id": "text",
        "pred_rating": "float",
        "pred_time": "timestamp"
    }
}

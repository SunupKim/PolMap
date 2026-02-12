# dataframe_utils.py

from src.config import RAW_COLUMNS, CANONICAL_COLUMNS
import pandas as pd

# global_sim_cluster 칼럼은 run_probe_global_similarity.py에서 만드는 것.
GLOBAL_SIMILARITY_COLUMNS = ["search_keyword", "news_id", "source_keyword",
                      "global_sim_cluster", "pubDate,collected_at",
                      #"title_id", "body_id", "is_canon", "replaced_by", "cluster_id",
                     ]

def _save_df(df, path, columns):
    df = df.copy()        
    df = df[[c for c in columns if c in df.columns]
            + [c for c in df.columns if c not in columns]]
    df.to_csv(path, index=False, encoding="utf-8-sig")

def raw_df_save(df, path):
    _save_df(df, path, RAW_COLUMNS)

def canonical_df_save(df, path):
    _save_df(df, path, CANONICAL_COLUMNS)

def global_similarity_df_save(df, path):
    _save_df(df, path, GLOBAL_SIMILARITY_COLUMNS)


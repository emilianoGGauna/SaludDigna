import json
import logging

import pandas as pd
from sqlalchemy import text
from sentence_transformers import SentenceTransformer

# Import your Azure EDA connector to get the SQLAlchemy engine
from DatabaseEDA import DatabaseEDA

# Configure module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
handler.setFormatter(formatter)
logger.addHandler(handler)


class DescriptionsEmbeddings:
    """
    Class to vectorize and persist metadata descriptions from a SQL table.
    """

    def __init__(
        self,
        engine,
        model_name: str = 'all-MiniLM-L6-v2',
    ):
        """
        :param engine: SQLAlchemy engine instance
        :param model_name: SentenceTransformer model identifier
        """
        self.engine = engine
        try:
            self.model = SentenceTransformer(model_name)
            logger.info("Loaded SentenceTransformer model '%s'", model_name)
        except Exception as e:
            logger.exception(
                "Failed to load SentenceTransformer model '%s': %s", model_name, e
            )
            raise

    def embed_and_persist(
        self,
        source_table: str = 'Descripcion_datos',
        key_col: str = 'Dato',
        text_col: str = 'Descripcion',
        dest_table: str = 'Descripcion_datos_embeddings',
        batch_size: int = 500,
    ) -> pd.DataFrame:
        """
        Embed descriptions and store embeddings in dest_table.
        Returns DataFrame with embeddings.

        :param source_table: table with metadata descriptions
        :param key_col: primary key column in source_table
        :param text_col: column with text to embed
        :param dest_table: destination table for storing embeddings as JSON
        :param batch_size: number of rows to process at once to limit memory
        """
        # Query source data
        query = text(f"SELECT {key_col}, {text_col} FROM [{source_table}]")
        try:
            df = pd.read_sql(query, self.engine)
        except Exception as e:
            logger.exception(
                "Failed to read from source table '%s': %s", source_table, e
            )
            raise

        if df.empty:
            logger.warning(
                "Source table '%s' is empty, nothing to embed.", source_table
            )
            return df

        # Ensure text column is string
        df[text_col] = df[text_col].astype(str)

        # Compute embeddings in batches to avoid memory spikes
        embeddings_list = []
        texts = df[text_col].tolist()
        for start in range(0, len(texts), batch_size):
            end = start + batch_size
            batch_texts = texts[start:end]
            try:
                batch_emb = self.model.encode(batch_texts, show_progress_bar=True)
            except Exception as e:
                logger.exception(
                    "Failed to encode batch %d:%d: %s", start, end, e
                )
                raise
            embeddings_list.extend(batch_emb)

        # Attach embeddings
        df['embedding'] = [emb.tolist() for emb in embeddings_list]

        # Prepare copy for SQL
        to_sql = df.copy()
        to_sql['embedding'] = to_sql['embedding'].apply(json.dumps)

        # Persist to destination table
        try:
            to_sql.to_sql(dest_table, self.engine, if_exists='replace', index=False)
            logger.info(
                "Persisted embeddings to table '%s' (%d rows)",
                dest_table,
                len(to_sql),
            )
        except Exception as e:
            logger.exception(
                "Failed to write embeddings to table '%s': %s", dest_table, e
            )
            raise

        return df


if __name__ == '__main__':
    # Instantiate DatabaseEDA to load .env and connect to Azure SQL
    db = DatabaseEDA()
    engine = db.engine

    # Preview available tables
    tables = db.list_tables()
    logger.info("Available tables: %s", tables)

    # Optional: quick missing-value report
    if 'Descripcion_datos' not in tables:
        logger.error(
            "Source table 'Descripcion_datos' not found. Available tables: %s", tables
        )
    else:
        embedder = DescriptionsEmbeddings(engine)
        df_embeddings = embedder.embed_and_persist(
            source_table='Descripcion_datos',
            key_col='Dato',
            text_col='Descripcion',
            dest_table='Descripcion_datos_embeddings',
            batch_size=500,
        )
        print(df_embeddings.head())
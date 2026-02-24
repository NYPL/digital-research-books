"""Configuration management for vector indexing pipeline.

Priority order:
1. Environment variables [highest precedence]
2. YAML configuration files (base + environment-specific overrides)
3. Code defaults (GlobalConfig dataclass defaults) [lowest precedence]
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Self


@dataclass(frozen=True)
class GlobalConfig:
    """Configuration for the vector indexing pipeline.

    Frozen to prevent accidental mutation after initialization.
    Use dataclasses.replace() to create modified copies.
    """

    # Environment
    environment: str = "local"

    # S3 settings
    s3_bucket: str = "vra-experiments-dev"
    s3_prefix: str = "data/experiment_books"
    s3_max_workers: int = 30

    # Local paths
    data_dir: Path = field(default_factory=lambda: Path("./data"))
    book_cache_dir: Path | None = None  # Defaults to data_dir / "books" if None
    embedding_cache_dir: Path | None = (
        None  # Defaults to data_dir / "embeddings" if None
    )

    # Elasticsearch settings
    es_index: str = "drb_chunks"
    es_host: str = "localhost"
    es_port: int = 9200
    es_scheme: str = "http"
    es_user: str | None = None
    es_password: str | None = None
    es_timeout: int = 60
    es_bulk_batch_size: int = 500

    # PostgreSQL settings
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_user: str | None = None
    pg_password: str | None = None
    pg_database: str = "drb"

    # Embedding settings
    embedding_model: str = "text-embedding-004"
    embedding_batch_size: int = 100
    embedding_dimensions: int = 768

    # Qwen local embedder settings
    qwen_scheme: str = "http"
    qwen_host: str = "localhost"
    qwen_port: int = 1234
    qwen_model: str = "qwen3-embedding-8b-fp16"

    # Chunking settings
    chunk_size: int = 512
    chunk_overlap: int = 50

    # Pipeline settings
    max_failures: int = 5
    log_dir: Path = field(default_factory=lambda: Path("./logs"))

    # Fields that should be masked in repr/str output
    _SENSITIVE_FIELDS: frozenset[str] = frozenset(
        {
            "es_password",
            "es_user",
            "pg_password",
            "pg_user",
        }
    )

    def __post_init__(self):
        # Convert string paths to Path objects and resolve relative paths
        def resolve_path(p) -> Path:
            path = Path(p)
            return path.resolve()

        object.__setattr__(self, "data_dir", resolve_path(self.data_dir))
        object.__setattr__(self, "log_dir", resolve_path(self.log_dir))

        if self.book_cache_dir is not None:
            object.__setattr__(
                self, "book_cache_dir", resolve_path(self.book_cache_dir)
            )
        if self.embedding_cache_dir is not None:
            object.__setattr__(
                self, "embedding_cache_dir", resolve_path(self.embedding_cache_dir)
            )

    def __repr__(self) -> str:
        """Return repr with sensitive fields masked."""
        fields = []
        for f in self.__dataclass_fields__:
            value = getattr(self, f)
            if f in self._SENSITIVE_FIELDS and value is not None:
                fields.append(f"{f}='***'")
            else:
                fields.append(f"{f}={value!r}")
        return f"{self.__class__.__name__}({', '.join(fields)})"

    def __str__(self) -> str:
        """Return str with sensitive fields masked."""
        return self.__repr__()

    @property
    def resolved_book_cache_dir(self) -> Path:
        """Book cache directory, defaulting to data_dir/books."""
        return self.book_cache_dir or self.data_dir / "books"

    @property
    def resolved_embedding_cache_dir(self) -> Path:
        """Embedding cache directory, defaulting to data_dir/embeddings."""
        return self.embedding_cache_dir or self.data_dir / "embeddings"

    @property
    def es_url(self) -> str:
        """Full Elasticsearch URL."""
        return f"{self.es_scheme}://{self.es_host}:{self.es_port}"

    @property
    def pg_connection_url(self) -> str:
        """PostgreSQL connection URL for SQLAlchemy."""
        if self.pg_user and self.pg_password:
            return f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        return f"postgresql://{self.pg_host}:{self.pg_port}/{self.pg_database}"

    @property
    def qwen_url(self) -> str:
        """Full Qwen embedder URL."""
        return f"{self.qwen_scheme}://{self.qwen_host}:{self.qwen_port}"

    @classmethod
    def from_env(cls) -> Self:
        """Load configuration from environment variables.

        Tries vector-indexing-specific env vars first, then falls back to
        existing etl-pipeline env vars:

        - S3_BUCKET -> FILE_BUCKET
        - ELASTICSEARCH_* -> DRB_ELASTICSEARCH_*
        - POSTGRES_* (same in both)
        """

        def get_env(env_keys: list[str], default):
            """Get env var, trying keys in order."""
            for key in env_keys:
                value = os.getenv(key)
                if value is not None:
                    # Type coercion based on default type
                    if isinstance(default, bool):
                        return value.lower() in ("true", "1", "yes")
                    if isinstance(default, int):
                        return int(value)
                    if isinstance(default, Path):
                        return Path(value)
                    return value
            return default

        # Get defaults from class
        defaults = cls()

        return cls(
            environment=get_env(["ENVIRONMENT", "ENV"], defaults.environment),
            # S3: try S3_BUCKET first, fall back to FILE_BUCKET (etl-pipeline)
            s3_bucket=get_env(["S3_BUCKET", "FILE_BUCKET"], defaults.s3_bucket),
            s3_prefix=get_env(["S3_PREFIX"], defaults.s3_prefix),
            s3_max_workers=get_env(["S3_MAX_WORKERS"], defaults.s3_max_workers),
            data_dir=get_env(["DATA_DIR"], defaults.data_dir),
            book_cache_dir=get_env(["BOOK_CACHE_DIR"], defaults.book_cache_dir),
            embedding_cache_dir=get_env(
                ["EMBEDDING_CACHE_DIR"], defaults.embedding_cache_dir
            ),
            # ES: try ELASTICSEARCH_* first, fall back to DRB_ELASTICSEARCH_* (etl-pipeline)
            es_index=get_env(
                ["ELASTICSEARCH_INDEX", "DRB_ELASTICSEARCH_INDEX"], defaults.es_index
            ),
            es_host=get_env(
                ["ELASTICSEARCH_HOST", "DRB_ELASTICSEARCH_HOST"], defaults.es_host
            ),
            es_port=get_env(
                ["ELASTICSEARCH_PORT", "DRB_ELASTICSEARCH_PORT"], defaults.es_port
            ),
            es_scheme=get_env(
                ["ELASTICSEARCH_SCHEME", "DRB_ELASTICSEARCH_SCHEME"], defaults.es_scheme
            ),
            es_user=get_env(
                ["ELASTICSEARCH_USER", "DRB_ELASTICSEARCH_USER"], defaults.es_user
            ),
            es_password=get_env(
                [
                    "ELASTICSEARCH_PSWD",
                    "ELASTICSEARCH_PASSWORD",
                    "DRB_ELASTICSEARCH_PSWD",
                ],
                defaults.es_password,
            ),
            es_timeout=get_env(
                ["ELASTICSEARCH_TIMEOUT", "DRB_ELASTICSEARCH_TIMEOUT"],
                defaults.es_timeout,
            ),
            es_bulk_batch_size=get_env(
                ["ELASTICSEARCH_BULK_BATCH_SIZE"], defaults.es_bulk_batch_size
            ),
            # PostgreSQL: same names in both
            pg_host=get_env(["POSTGRES_HOST"], defaults.pg_host),
            pg_port=get_env(["POSTGRES_PORT"], defaults.pg_port),
            pg_user=get_env(["POSTGRES_USER"], defaults.pg_user),
            pg_password=get_env(
                ["POSTGRES_PSWD", "POSTGRES_PASSWORD"], defaults.pg_password
            ),
            pg_database=get_env(["POSTGRES_NAME", "POSTGRES_DB"], defaults.pg_database),
            embedding_model=get_env(["EMBEDDING_MODEL"], defaults.embedding_model),
            embedding_batch_size=get_env(
                ["EMBEDDING_BATCH_SIZE"], defaults.embedding_batch_size
            ),
            embedding_dimensions=get_env(
                ["EMBEDDING_DIMENSIONS"], defaults.embedding_dimensions
            ),
            qwen_scheme=get_env(["QWEN_SCHEME"], defaults.qwen_scheme),
            qwen_host=get_env(["QWEN_HOST"], defaults.qwen_host),
            qwen_port=get_env(["QWEN_PORT"], defaults.qwen_port),
            qwen_model=get_env(["QWEN_MODEL"], defaults.qwen_model),
            chunk_size=get_env(["CHUNK_SIZE"], defaults.chunk_size),
            chunk_overlap=get_env(["CHUNK_OVERLAP"], defaults.chunk_overlap),
            max_failures=get_env(["MAX_FAILURES"], defaults.max_failures),
            log_dir=get_env(["LOG_DIR"], defaults.log_dir),
        )

    @classmethod
    def from_yaml(cls, path: Path) -> Self:
        """Load configuration from a YAML file."""
        import yaml

        with open(path) as f:
            data = yaml.safe_load(f) or {}

        # Filter to only known fields
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known_fields}

        return cls(**filtered)


# Module-level singleton
_default_config: GlobalConfig | None = None


def get_config() -> GlobalConfig:
    """Get the global configuration singleton.

    Lazy-initializes from environment variables on first access.

    Returns:
        GlobalConfig instance
    """
    global _default_config
    if _default_config is None:
        _default_config = GlobalConfig.from_env()
    return _default_config


def set_config(config: GlobalConfig) -> None:
    """Override the global configuration singleton.

    Args:
        config: New configuration to use
    """
    global _default_config
    _default_config = config


def reset_config() -> None:
    """Reset the global configuration (for testing)."""
    global _default_config
    _default_config = None

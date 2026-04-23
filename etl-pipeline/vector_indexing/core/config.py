"""Configuration management with hybrid loading.

Priority level:
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


def _get_project_root() -> Path:
    """Find project root by looking for setup.py or pyproject.toml."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "setup.py").exists() or (parent / "pyproject.toml").exists():
            return parent
    # Fallback: vector_indexing/core/config.py -> up 3 levels to etl-pipeline
    return current.parent.parent.parent


# Root of the etl-pipeline project
PROJECT_ROOT = _get_project_root()

# Root of the vector_indexing package (for loading schemas etc.)
VECTOR_INDEXING_ROOT = Path(__file__).resolve().parent.parent

# Config paths - using existing etl-pipeline conventions
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = VECTOR_INDEXING_ROOT / "data"


@dataclass(frozen=True)
class GlobalConfig:
    """Global configuration for the v2 pipeline.

    Frozen to prevent accidental mutation after initialization.
    Use dataclasses.replace() to create modified copies.
    """

    # Environment
    environment: str = "local"

    # S3 settings
    s3_bucket: str = "drb-files-limited-production"
    s3_prefix: str = "grin"
    s3_max_workers: int = 30

    # GRIN decryption key (for encrypted archives)
    grin_access_key: str | None = None

    # Local paths
    data_dir: Path = field(default_factory=lambda: DATA_DIR)
    book_cache_dir: Path | None = None  # Defaults to data_dir / "books" if None
    embedding_cache_dir: Path | None = (
        None  # Defaults to data_dir / "embeddings" if None
    )

    # Elasticsearch settings
    es_index: str = "vra_chunks_gemini-embedding-001"
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
    pg_database: str = "vra"

    # Embedding settings
    embedding_model: str = "gemini-embedding-001"
    embedding_batch_size: int = 100
    embedding_dimensions: int = 768

    # Qwen local embedder settings
    qwen_scheme: str = "http"
    qwen_host: str = "localhost"
    qwen_port: int = 1234
    qwen_model: str = "qwen3-embedding-8b-fp16"

    # Turbopuffer settings
    turbopuffer_api_key: str | None = None
    turbopuffer_region: str = "aws-us-east-1"

    # Chunking settings
    chunk_size: int = 512
    chunk_overlap: int = 50

    # Pipeline settings
    max_failures: int = 5
    log_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "logs" / "v2")

    # Fields that should be masked in repr/str output
    _SENSITIVE_FIELDS: frozenset[str] = frozenset(
        {
            "es_password",
            "es_user",
            "pg_password",
            "pg_user",
            "turbopuffer_api_key",
            "grin_access_key",
        }
    )

    def __post_init__(self):
        # Convert string paths to Path objects and resolve relative paths against PROJECT_ROOT
        def resolve_path(p) -> Path:
            path = Path(p)
            if not path.is_absolute():
                path = PROJECT_ROOT / path
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

        Environment variable names:
        - ELASTICSEARCH_* for ES settings
        - POSTGRES_* for PostgreSQL settings
        - S3_*, DATA_DIR, etc. for other settings
        """

        def get_env(env_keys: list[str], default):
            """Get env var, trying keys in order."""
            for key in env_keys:
                value = os.getenv(key)
                if value is not None and value != "":
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
            environment=get_env(["VRA_ENV"], defaults.environment),
            s3_bucket=get_env(["S3_BUCKET"], defaults.s3_bucket),
            s3_prefix=get_env(["S3_PREFIX"], defaults.s3_prefix),
            s3_max_workers=get_env(["S3_MAX_WORKERS"], defaults.s3_max_workers),
            grin_access_key=get_env(["GRIN_ACCESS_KEY"], defaults.grin_access_key),
            data_dir=get_env(["DATA_DIR"], defaults.data_dir),
            book_cache_dir=get_env(["BOOK_CACHE_DIR"], defaults.book_cache_dir),
            embedding_cache_dir=get_env(
                ["EMBEDDING_CACHE_DIR"], defaults.embedding_cache_dir
            ),
            es_index=get_env(["ELASTICSEARCH_INDEX"], defaults.es_index),
            es_host=get_env(["ELASTICSEARCH_HOST"], defaults.es_host),
            es_port=get_env(["ELASTICSEARCH_PORT"], defaults.es_port),
            es_scheme=get_env(["ELASTICSEARCH_SCHEME"], defaults.es_scheme),
            es_user=get_env(["ELASTICSEARCH_USER"], defaults.es_user),
            es_password=get_env(
                ["ELASTICSEARCH_PSWD", "ELASTICSEARCH_PASSWORD"], defaults.es_password
            ),
            es_timeout=get_env(["ELASTICSEARCH_TIMEOUT"], defaults.es_timeout),
            es_bulk_batch_size=get_env(
                ["ELASTICSEARCH_BULK_BATCH_SIZE"], defaults.es_bulk_batch_size
            ),
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
            turbopuffer_api_key=get_env(
                ["TURBOPUFFER_API_KEY"], defaults.turbopuffer_api_key
            ),
            turbopuffer_region=get_env(
                ["TURBOPUFFER_REGION"], defaults.turbopuffer_region
            ),
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

    @classmethod
    def from_layered(cls, base_path: Path, override_path: Path | None = None) -> Self:
        """Load base config, then apply environment-specific overrides.

        Args:
            base_path: Path to base configuration YAML
            override_path: Optional path to override YAML (merged on top of base)

        Returns:
            GlobalConfig with merged settings
        """
        import yaml

        # Load base
        with open(base_path) as f:
            data = yaml.safe_load(f) or {}

        # Layer overrides
        if override_path and override_path.exists():
            with open(override_path) as f:
                overrides = yaml.safe_load(f) or {}
            data.update(overrides)

        # Filter to only known fields
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known_fields}

        return cls(**filtered)

    @classmethod
    def for_environment(cls, env: str | None = None) -> Self:
        """Load configuration for a specific environment.

        Priority order (highest to lowest):
        1. Environment variables (VRA_ENV or legacy prefixes)
        2. YAML files (config/v2/{env}.yaml layered on base.yaml)
        3. Code defaults in this dataclass

        Args:
            env: Environment name. If None, reads from VRA_ENV (default: "local")

        Returns:
            GlobalConfig for the environment
        """
        if env is None:
            env = os.getenv("VRA_ENV", "local")

        # Start with code defaults
        defaults = cls(environment=env)

        # Load YAML if available (overrides code defaults)
        yaml_values: dict = {}
        env_config_path = CONFIG_DIR / f"{env}.yaml"
        base_config_path = CONFIG_DIR / "base.yaml"

        if base_config_path.exists():
            import yaml

            with open(base_config_path) as f:
                yaml_values = yaml.safe_load(f) or {}
            if env_config_path.exists():
                with open(env_config_path) as f:
                    yaml_values.update(yaml.safe_load(f) or {})
        elif env_config_path.exists():
            import yaml

            with open(env_config_path) as f:
                yaml_values = yaml.safe_load(f) or {}

        # only use known fields
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        yaml_values = {k: v for k, v in yaml_values.items() if k in known_fields}

        # load environment variables (highest priority - override everything)
        env_values = cls._get_env_overrides()

        # merge: defaults <- yaml <- env
        final_values = {
            field_name: getattr(defaults, field_name) for field_name in known_fields
        }
        final_values.update(yaml_values)
        final_values.update(env_values)
        final_values["environment"] = env  # Always use the resolved env

        return cls(**final_values)

    @classmethod
    def _get_env_overrides(cls) -> dict:
        """Get all config values that are set via environment variables.

        Returns:
            Dict of field_name -> value for any env vars that are set
        """
        overrides = {}
        defaults = cls()

        # Mapping of field name -> list of env var names to try (in order)
        env_mappings = {
            "environment": ["VRA_ENV"],
            "s3_bucket": ["S3_BUCKET"],
            "s3_prefix": ["S3_PREFIX"],
            "s3_max_workers": ["S3_MAX_WORKERS"],
            "grin_access_key": ["GRIN_ACCESS_KEY"],
            "data_dir": ["DATA_DIR"],
            "book_cache_dir": ["BOOK_CACHE_DIR"],
            "embedding_cache_dir": ["EMBEDDING_CACHE_DIR"],
            "es_index": ["ELASTICSEARCH_INDEX"],
            "es_host": ["ELASTICSEARCH_HOST"],
            "es_port": ["ELASTICSEARCH_PORT"],
            "es_scheme": ["ELASTICSEARCH_SCHEME"],
            "es_user": ["ELASTICSEARCH_USER"],
            "es_password": ["ELASTICSEARCH_PSWD", "ELASTICSEARCH_PASSWORD"],
            "es_timeout": ["ELASTICSEARCH_TIMEOUT"],
            "es_bulk_batch_size": ["ELASTICSEARCH_BULK_BATCH_SIZE"],
            "pg_host": ["POSTGRES_HOST"],
            "pg_port": ["POSTGRES_PORT"],
            "pg_user": ["POSTGRES_USER"],
            "pg_password": ["POSTGRES_PSWD", "POSTGRES_PASSWORD"],
            "pg_database": ["POSTGRES_NAME", "POSTGRES_DB"],
            "embedding_model": ["EMBEDDING_MODEL"],
            "embedding_batch_size": ["EMBEDDING_BATCH_SIZE"],
            "embedding_dimensions": ["EMBEDDING_DIMENSIONS"],
            "qwen_scheme": ["QWEN_SCHEME"],
            "qwen_host": ["QWEN_HOST"],
            "qwen_port": ["QWEN_PORT"],
            "qwen_model": ["QWEN_MODEL"],
            "chunk_size": ["CHUNK_SIZE"],
            "chunk_overlap": ["CHUNK_OVERLAP"],
            "max_failures": ["MAX_FAILURES"],
            "log_dir": ["LOG_DIR"],
        }

        for field_name, env_keys in env_mappings.items():
            value = None
            for key in env_keys:
                value = os.getenv(key)
                if value is not None and value != "":
                    break
                value = None  # Reset if empty string

            if value is not None:
                # Type coercion based on default type
                default_val = getattr(defaults, field_name)
                if isinstance(default_val, bool):
                    overrides[field_name] = value.lower() in ("true", "1", "yes")
                elif isinstance(default_val, int):
                    overrides[field_name] = int(value)
                elif isinstance(default_val, Path):
                    overrides[field_name] = Path(value)
                else:
                    overrides[field_name] = value

        return overrides


# Module-level singleton
_default_config: GlobalConfig | None = None


def get_config() -> GlobalConfig:
    """Get the global configuration singleton.

    Lazy-initializes from VRA_ENV on first access.

    Returns:
        GlobalConfig instance
    """
    global _default_config
    if _default_config is None:
        _default_config = GlobalConfig.for_environment()
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

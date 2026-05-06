"""Configuration for vector indexing pipeline."""

from __future__ import annotations

import os
from copy import deepcopy
from dataclasses import dataclass, field, fields
from pathlib import Path

from utils.common import require_env


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


def resolve_path(p) -> Path:
    """Resolve a path, making relative paths absolute against PROJECT_ROOT."""
    path = Path(p)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def masked_dataclass_repr(instance: object) -> str:
    """Return repr for a dataclass instance with secret fields masked.

    To mask a field, set field(<value>, metadata={"secret": True})
    """
    reprs = []
    for f in fields(instance):
        value = getattr(instance, f.name)
        if f.metadata.get("secret") and value is not None:
            reprs.append(f"{f.name}='***'")
        else:
            reprs.append(f"{f.name}={value!r}")
    return f"{instance.__class__.__name__}({', '.join(reprs)})"


@dataclass
class PostgresConfig:
    host: str = field(default_factory=lambda: require_env("POSTGRES_HOST"))
    port: int = field(default_factory=lambda: int(require_env("POSTGRES_PORT")))
    user: str = field(
        default_factory=lambda: require_env("POSTGRES_USER"), metadata={"secret": True}
    )
    password: str = field(
        default_factory=lambda: require_env("POSTGRES_PSWD"), metadata={"secret": True}
    )
    database: str = field(default_factory=lambda: require_env("POSTGRES_NAME"))

    @property
    def connection_url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def __repr__(self) -> str:
        return masked_dataclass_repr(self)


@dataclass
class ElasticsearchConfig:
    host: str = field(default_factory=lambda: require_env("VRA_ELASTICSEARCH_HOST"))
    port: int = field(
        default_factory=lambda: int(require_env("VRA_ELASTICSEARCH_PORT"))
    )
    user: str | None = field(
        default_factory=lambda: os.environ.get("VRA_ELASTICSEARCH_USER"),
        metadata={"secret": True},
    )
    password: str | None = field(
        default_factory=lambda: os.environ.get("VRA_ELASTICSEARCH_PSWD"),
        metadata={"secret": True},
    )
    scheme: str = field(
        default_factory=lambda: os.environ.get("VRA_ELASTICSEARCH_SCHEME", "http")
    )
    timeout: int = 60

    @property
    def url(self) -> str:
        if self.user and self.password:
            return (
                f"{self.scheme}://{self.user}:{self.password}@{self.host}:{self.port}"
            )
        return f"{self.scheme}://{self.host}:{self.port}"

    def __repr__(self) -> str:
        return masked_dataclass_repr(self)


@dataclass
class QwenConfig:
    host: str = "localhost"
    port: int = 1234
    scheme: str = "http"
    model: str = "qwen3-embedding-8b-fp16"

    @property
    def url(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}"

    def __repr__(self) -> str:
        return masked_dataclass_repr(self)


####### Index Config ########


def load_from_module(class_name: str, module) -> type:
    """Load a class by name from a module.

    Args:
        class_name: The name of the class to load
        module: The module object to search in

    Returns:
        The class object

    Raises:
        ValueError: If the class is not found in the module
    """
    cls = getattr(module, class_name, None)
    if cls is None:
        raise ValueError(f"Unknown class: {class_name!r} in module {module.__name__}")
    return cls


def get_default_schema_with_dims(dims: str):
    from vector_indexing.components.backends.turbopuffer import load_default_schema

    schema = load_default_schema()
    schema["vector"]["type"] = f"[{dims}]f16"
    return schema


HARRIER_OSS_V1_DIMENSIONS = 1024

QWEN3_EMBEDDING_8B_DIMENSIONS = 1024


def _index_config_data():
    from vector_indexing.components.backends.turbopuffer import (
        load_default_schema,
    )

    return [
        {  # Google
            "names": [
                "vra-dev",
                "vra_test-sketches_of_the_north_river-gemini-001",
            ],
            "embedder": {
                "class": "GoogleEmbedder",
                "params": {
                    # All are default and unnecessary
                    "model": "gemini-embedding-001",
                    "dimensions": 768,
                    "task_type": "RETRIEVAL_QUERY",
                },
            },
            "schema": load_default_schema(),
        },
        {  # Harrier
            "names": [
                "vra_test-sketches_of_the_north_river-harrier_oss_v1_.6b",
                "vra_test-10k-harrier_oss_v1_.6b",
            ],
            "embedder": {
                "class": "SageMakerEmbedder",
                "params": {
                    "endpoint_name": "hf-tei-harrier-oss-v1-0-6b-ml-g6-2xlarge-20260424-011130",  # pragma: allowlist secret
                    "aws_profile": "sandbox",
                    "concurrency": 41,
                },
            },
            "schema": get_default_schema_with_dims(HARRIER_OSS_V1_DIMENSIONS),
        },
        {  # Qwen
            "names": [
                "vra_test-sketches_of_the_north-qwen3_embedding_8b"  # pragma: allowlist secret
            ],
            "embedder": {
                "class": "SageMakerEmbedder",
                "params": {
                    "endpoint_name": "hf-tei-qwen3-embedding-8b-ml-g6e-xlarge-20260428-235752",  # pragma: allowlist secret
                    "aws_profile": "sandbox",
                    "concurrency": 14,
                    "dimensions": QWEN3_EMBEDDING_8B_DIMENSIONS,
                },
            },
            "schema": get_default_schema_with_dims(QWEN3_EMBEDDING_8B_DIMENSIONS),
        },
    ]


def get_index_config_dict(index_name):
    """Return the raw index config dictionary for index_name."""
    entry = next((e for e in _index_config_data() if index_name in e["names"]), None)
    if entry is None:
        raise ValueError(f"No index config found for index name: {index_name!r}")
    return deepcopy(entry)


def get_index_config(index_name):
    from vector_indexing.components.backends.turbopuffer import TurbopufferBackend
    from vector_indexing.components import embedders as embedders_module

    entry = get_index_config_dict(index_name)

    embedder_class_name = entry["embedder"]["class"]
    embedder_class = load_from_module(embedder_class_name, embedders_module)
    embedder = embedder_class(**entry["embedder"]["params"])
    backend = TurbopufferBackend(index_name=index_name, schema=entry["schema"])
    return {"embedder": embedder, "backend": backend}

import yaml
from pathlib import Path
from typing import Any

from core.base import Extractor, Transformer, Loader
from core.pipeline import Pipeline
from core.history import RunHistory
from plugins.extractors import CSVExtractor, HTTPExtractor, JSONFileExtractor
from plugins.transformers import (
    FilterTransformer,
    RenameTransformer,
    CastTransformer,
    AddFieldTransformer,
    DropFieldTransformer,
)
from plugins.loaders import JSONFileLoader, SQLiteLoader, CSVLoader, StdoutLoader

PLUGIN_REGISTRY: dict[str, type] = {
    "CSVExtractor":      CSVExtractor,
    "HTTPExtractor":     HTTPExtractor,
    "JSONFileExtractor": JSONFileExtractor,
    "FilterTransformer":    FilterTransformer,
    "RenameTransformer":    RenameTransformer,
    "CastTransformer":      CastTransformer,
    "AddFieldTransformer":  AddFieldTransformer,
    "DropFieldTransformer": DropFieldTransformer,
    "JSONFileLoader": JSONFileLoader,
    "SQLiteLoader":   SQLiteLoader,
    "CSVLoader":      CSVLoader,
    "StdoutLoader":   StdoutLoader,
}

def load_pipeline(
    path: str | Path,
    history: RunHistory | None = None,
) -> Pipeline:
    raw = _read_yaml(path)

    name = raw["name"]
    extractor = _build_plugin(raw["extractor"], Extractor)
    loader = _build_plugin(raw["loader"], Loader)
    if "transformers" in raw:
        transformers = [_build_plugin(t, Transformer) for t in raw["transformers"]]
        transformer = _chain(transformers)
    elif "transformer" in raw:
        transformer = _build_plugin(raw["transformer"], Transformer)
    else:
        raise KeyError(f"Pipeline '{name}' must define 'transformer' or 'transformers'")

    retry = raw.get("retry", {})

    return Pipeline(
        name=name,
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        max_attempts=retry.get("max_attempts", 3),
        base_delay_s=retry.get("base_delay_s", 2.0),
        backoff_factor=retry.get("backoff_factor", 2.0),
        history=history or RunHistory(),
    )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _read_yaml(path: str | Path) -> dict:
    with open(path) as fh:
        return yaml.safe_load(fh)


def _build_plugin(spec: dict, expected_base: type) -> Any:
    class_name = spec["class"]
    config = spec.get("config", {})

    cls = PLUGIN_REGISTRY.get(class_name)
    if cls is None:
        raise ValueError(
            f"Unknown plugin class: {class_name!r}. "
            f"Available: {sorted(PLUGIN_REGISTRY)}"
        )
    if not issubclass(cls, expected_base):
        raise TypeError(
            f"{class_name} is not a subclass of {expected_base.__name__}"
        )
    return cls(config)


def _chain(transformers: list[Transformer]) -> Transformer:
    class ChainedTransformer(Transformer):
        def __init__(self, steps: list[Transformer]):
            super().__init__({})
            self.steps = steps

        def transform(self, records):
            stream = records
            for step in self.steps:
                stream = step.transform(stream)
            return stream

        def __repr__(self):
            return f"ChainedTransformer({self.steps})"

    return ChainedTransformer(transformers)
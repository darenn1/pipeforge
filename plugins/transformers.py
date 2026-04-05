import logging
from typing import Any, Iterator

from core.base import Transformer

logger = logging.getLogger(__name__)


class FilterTransformer(Transformer):
    _OPS = {
        "eq":       lambda a, b: a == b,
        "ne":       lambda a, b: a != b,
        "gt":       lambda a, b: float(a) > float(b),
        "gte":      lambda a, b: float(a) >= float(b),
        "lt":       lambda a, b: float(a) < float(b),
        "lte":      lambda a, b: float(a) <= float(b),
        "contains": lambda a, b: b in str(a),
        "exists":   lambda a, _: a is not None and a != "",
    }

    def transform(self, records: Iterator[Any]) -> Iterator[Any]:
        rules = self.config.get("rules", [])
        kept = dropped = 0

        for record in records:
            if self._passes(record, rules):
                kept += 1
                yield record
            else:
                dropped += 1

        logger.info(
            "FilterTransformer: kept %d, dropped %d", kept, dropped
        )

    def _passes(self, record: dict, rules: list[dict]) -> bool:
        for rule in rules:
            field = rule["field"]
            op    = rule["op"]
            value = rule.get("value")
            fn    = self._OPS.get(op)
            if fn is None:
                raise ValueError(f"Unknown filter op: {op!r}")
            if not fn(record.get(field), value):
                return False
        return True


class RenameTransformer(Transformer):
    def transform(self, records: Iterator[Any]) -> Iterator[Any]:
        mapping: dict = self.config.get("mapping", {})
        for record in records:
            yield {mapping.get(k, k): v for k, v in record.items()}


class CastTransformer(Transformer):
    _TYPES = {
        "int":   int,
        "float": float,
        "str":   str,
        "bool":  lambda v: v if isinstance(v, bool) else str(v).lower() in ("1", "true", "yes"),
    }

    def transform(self, records: Iterator[Any]) -> Iterator[Any]:
        casts: dict = self.config.get("casts", {})
        for record in records:
            out = dict(record)
            for field, type_name in casts.items():
                if field in out and out[field] is not None:
                    fn = self._TYPES.get(type_name)
                    if fn is None:
                        raise ValueError(f"Unknown cast type: {type_name!r}")
                    try:
                        out[field] = fn(out[field])
                    except (ValueError, TypeError) as e:
                        raise ValueError(
                            f"CastTransformer: cannot cast {field}={out[field]!r} "
                            f"to {type_name}: {e}"
                        ) from e
            yield out


class AddFieldTransformer(Transformer):
    def transform(self, records: Iterator[Any]) -> Iterator[Any]:
        fields: dict = self.config.get("fields", {})
        for record in records:
            yield {**record, **fields}


class DropFieldTransformer(Transformer):
    def transform(self, records: Iterator[Any]) -> Iterator[Any]:
        drop = set(self.config.get("fields", []))
        for record in records:
            yield {k: v for k, v in record.items() if k not in drop}
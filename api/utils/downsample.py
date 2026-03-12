from __future__ import annotations

try:
    import numpy as np
except ImportError:  # pragma: no cover
    np = None


def sample_indices(total: int, max_points: int) -> list[int]:
    if total <= 0:
        return []
    if max_points <= 0 or total <= max_points:
        return list(range(total))

    if np is not None:
        idx = np.linspace(0, total - 1, max_points, dtype=int)
        return idx.tolist()

    stride = (total - 1) / (max_points - 1)
    return [round(i * stride) for i in range(max_points)]

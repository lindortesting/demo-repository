"""Character and word n-gram generators plus a frequency table.

Pure Python, no third-party deps.
"""

from collections import Counter
from collections.abc import Iterable, Sequence

__all__ = ["char_ngrams", "ngram_frequency", "word_ngrams"]


def _check_n(n: int) -> None:
    if n < 1:
        raise ValueError(f"n must be a positive integer, got {n}")


def char_ngrams(text: str, n: int) -> list[str]:
    """Return character n-grams of ``text`` as overlapping substrings of length ``n``."""
    _check_n(n)
    if len(text) < n:
        return []
    return [text[i : i + n] for i in range(len(text) - n + 1)]


def word_ngrams(tokens: Sequence[str], n: int) -> list[tuple[str, ...]]:
    """Return word n-grams from ``tokens`` as overlapping tuples of length ``n``."""
    _check_n(n)
    seq = tuple(tokens)
    if len(seq) < n:
        return []
    return [seq[i : i + n] for i in range(len(seq) - n + 1)]


def ngram_frequency(ngrams: Iterable) -> Counter:
    """Count occurrences of each n-gram in ``ngrams``."""
    return Counter(ngrams)

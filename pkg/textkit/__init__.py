"""textkit — pure-Python text utilities for tokenization, n-grams, and stemming.

The package exposes three independent modules:

- :mod:`textkit.tokenizer` — Unicode-aware word and sentence tokenization.
- :mod:`textkit.ngrams` — character and word n-gram generators and a
  frequency counter built on :class:`collections.Counter`.
- :mod:`textkit.stemmer` — the Porter (1980) suffix-stripping stemmer for
  lowercased English words.

Re-exports the public API at the package level for ``import textkit; textkit.stem(...)``.
"""

from .ngrams import char_ngrams, ngram_frequency, word_ngrams
from .stemmer import stem
from .tokenizer import tokenize_sentences, tokenize_words

__all__ = [
    "char_ngrams",
    "ngram_frequency",
    "stem",
    "tokenize_sentences",
    "tokenize_words",
    "word_ngrams",
]

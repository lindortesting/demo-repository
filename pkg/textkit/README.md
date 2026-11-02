# textkit

Pure-Python text utilities. No third-party dependencies.

`textkit` bundles three small modules that work well together for
shallow text analysis: tokenize a document into words and sentences,
turn the words into character or word n-grams (and count them), and
reduce inflected English words to a common stem.

## Modules

| Module | Purpose |
| --- | --- |
| `textkit.tokenizer` | Unicode-aware word and sentence tokenization. |
| `textkit.ngrams`    | Character and word n-gram generators, plus a frequency counter. |
| `textkit.stemmer`   | Porter (1980) suffix-stripping stemmer for lowercased English. |

All three are re-exported at the package root, so any of the following
work after `import textkit`:

```python
textkit.tokenize_words("Hello, world!")
# ['Hello', 'world']

textkit.word_ngrams(["the", "quick", "brown", "fox"], 2)
# [('the', 'quick'), ('quick', 'brown'), ('brown', 'fox')]

textkit.stem("relational")
# 'relat'
```

## Installation

`textkit` is a regular Python package; build it with `pip` from this
directory:

```sh
pip install .
# or, for an editable install while iterating
pip install -e .
```

Running the test suite requires `pytest` (`pip install -e .[test]`
in this directory or `python3 -m pytest pkg/textkit` from the repo
root).

## Quick tour

```python
import textkit

text = "The quick brown foxes are jumping over the lazy dogs."

words = textkit.tokenize_words(text)
# ['The', 'quick', 'brown', 'foxes', 'are', 'jumping', 'over',
#  'the', 'lazy', 'dogs']

bigrams = textkit.word_ngrams(words, 2)
# [('The', 'quick'), ('quick', 'brown'), ('brown', 'foxes'), ...]

freq = textkit.ngram_frequency(bigrams)
freq.most_common(1)
# [(('the', 'lazy'), 1)]  # or whichever bigram tied for first

stems = [textkit.stem(w.lower()) for w in words]
# ['the', 'quick', 'brown', 'fox', 'are', 'jump', 'over', 'the', 'lazi', 'dog']
```

## Scope

- **Tokenization** is rule-based; it knows about Unicode letter classes,
  curly apostrophes, en/em dashes, common abbreviations, and
  decimal-point vs. sentence-end. It is not a learned English model:
  expect a few joins or splits to be off in edge cases, and tune the
  abbreviation list (`tokenizer._ABBREVIATIONS`) as you encounter new
  ones.
- **n-grams** are simple sliding windows. A faithful Chinese word
  tokenizer would need a dictionary (Jieba, etc.) — `tokenize_words`
  treats Han runs as a single word, on purpose.
- **Stemming** is the original Porter (1980) algorithm. It is
  intentionally lossy and works best on lowercased English morphological
  variants.

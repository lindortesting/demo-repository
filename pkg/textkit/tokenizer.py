"""Unicode-aware word and sentence tokenization.

Public API:
    tokenize_words(text) -> list[str]
    tokenize_sentences(text) -> list[str]
"""

import re

# ---------- Word / number tokens ------------------------------------

# A "word" is a run of Unicode letters that may contain internal
# apostrophes (ASCII ' and the typographic variants ʼ ‘ ’ ‚ ʽ ´ ᾽)
# or hyphens and dashes (- ‑ – — ―).
_WORD = re.compile(
    r"[^\W\d_]+"
    r"(?:[\u0027\u02BC\u02B9\u02BB\u02C8\u02CA\u02CB"
    r"\u2018\u2019\u201A\u201B\u2032\u2035"
    r"\u2010\u2011\u2012\u2013\u2014\u2015\u002D]"
    r"[^\W\d_]+)*",
    re.UNICODE,
)

# Numbers: integers and decimals.
_NUMBER = re.compile(r"\d+(?:[\.,]\d+)*", re.UNICODE)

_TOKEN = re.compile(rf"(?:{_WORD.pattern})|(?:{_NUMBER.pattern})", re.UNICODE)


def tokenize_words(text: str) -> list[str]:
    """Return the word and number tokens of ``text`` in order."""
    return _TOKEN.findall(text)


# ---------- Sentence tokenization ------------------------------------

# Common abbreviations whose trailing "." should not break a sentence.
_ABBREVIATIONS = frozenset({
    "mr", "mrs", "ms", "dr", "prof",
    "sr", "jr", "st",
    "gen", "gov", "sgt", "cpl", "pvt",
    "lt", "capt", "cmdr", "maj", "col", "adm",
    "rev", "hon",
    "vs", "etc", "approx", "incl",
})

_SENTENCE_END = ".!?"

_CLOSE = frozenset('"\'\u2019\u201D\u201C\u2018)]\u300D\u3009\u300B')

# Capital letters across Latin (basic + extended), Greek,
# Cyrillic, Armenian, and fullwidth Latin.
_CAPITAL = re.compile(
    r"[A-Z\u00C0-\u024F\u0370-\u03FF\u0400-\u042F"
    r"\u0531-\u0556\uFF21-\uFF3A]",
    re.UNICODE,
)


def tokenize_sentences(text: str) -> list[str]:
    """Return the sentences of ``text``.

    Breaks at ``.``, ``!``, ``?`` whose preceding alphabetic run
    is not a known abbreviation and not a single all-uppercase
    letter (a name-initial), and whose next non-whitespace,
    non-closing-quote character is a capital letter in Latin,
    Greek, Cyrillic, Armenian, or fullwidth Latin.
    """
    if not isinstance(text, str):
        raise TypeError("text must be str")
    if not text.strip():
        return []

    n = len(text)
    sentences = []
    start = 0
    i = 0
    while i < n:
        if text[i] in _SENTENCE_END:
            preceding = _word_at(text, i - 1, start)
            if preceding.lower() in _ABBREVIATIONS:
                i += 1
                continue
            if len(preceding) == 1 and preceding.isupper():
                i += 1
                continue
            j = _skip_graphical(text, i + 1)
            if j < n and _CAPITAL.match(text[j]):
                sentence = text[start:j].rstrip()
                if sentence:
                    sentences.append(sentence)
                start = j
                i = j
                continue
        i += 1

    tail = text[start:].strip()
    if tail:
        sentences.append(tail)
    return sentences


def _word_at(text: str, i: int, start: int) -> str:
    """Return the alphabetic run ending at index ``i`` (inclusive)."""
    end = i + 1
    while i >= start and text[i].isalpha():
        i -= 1
    return text[i + 1:end]


def _skip_graphical(text: str, j: int) -> int:
    """Advance ``j`` past whitespace and closing-quote characters."""
    n = len(text)
    while j < n and (text[j].isspace() or text[j] in _CLOSE):
        j += 1
    return j

"""Tests for ``textkit.tokenizer``.

Each test encodes one requirement of the tokenizer API.
"""

import pytest

from pkg.textkit.tokenizer import tokenize_words, tokenize_sentences


# ---------- tokenize_words: shape and empty inputs --------------------

def test_words_empty_string_returns_empty_list():
    """Empty input must yield no tokens."""
    assert tokenize_words("") == []


def test_words_whitespace_only_returns_empty_list():
    """Whitespace-only input must yield no tokens."""
    assert tokenize_words("   \n\t  ") == []


def test_words_returns_list_of_strings():
    """Result type must be a list of ``str`` (not generator, not tuple)."""
    result = tokenize_words("hello world")
    assert isinstance(result, list)
    assert all(isinstance(t, str) for t in result)


def test_words_non_string_raises_type_error():
    """Non-str input must fail loudly rather than producing garbage."""
    with pytest.raises(TypeError):
        tokenize_words(12345)


# ---------- tokenize_words: simple ASCII ------------------------------

def test_words_splits_on_whitespace():
    """Whitespace is the default word boundary."""
    assert tokenize_words("hello world") == ["hello", "world"]


def test_words_strips_surrounding_punctuation():
    """Punctuation around a word is delimiter, not part of the token."""
    assert tokenize_words("Hello, world!") == ["Hello", "world"]


def test_words_drops_orphan_punctuation_between_words():
    """Standalone punctuation between words is not emitted."""
    assert tokenize_words("Hello -- world.") == ["Hello", "world"]


def test_words_keeps_internal_apostrophe_in_contraction():
    """ASCII ``'`` inside a contraction like ``don't`` stays attached."""
    assert tokenize_words("I don't know.") == ["I", "don't", "know"]


def test_words_keeps_internal_hyphens_in_compounds():
    """Hyphens inside compounds like ``state-of-the-art`` stay attached."""
    assert tokenize_words("state-of-the-art") == ["state-of-the-art"]


def test_words_treats_curly_apostrophe_as_internal():
    """U+2019 RIGHT SINGLE QUOTATION MARK is a word-internal apostrophe.

    Without this rule, ``don\u2019t`` would split into ``don`` and ``t``,
    which is wrong for real English text copied from the web.
    """
    assert tokenize_words("don\u2019t go") == ["don\u2019t", "go"]


def test_words_treats_en_dash_and_em_dash_as_internal():
    """Unicode dashes (en-dash, em-dash) glue words like hyphens do."""
    assert tokenize_words("pre\u2013war post\u2014modern") == [
        "pre\u2013war", "post\u2014modern"
    ]


# ---------- tokenize_words: Unicode letters ---------------------------

def test_words_handles_accented_latin_letters():
    """Latin-Extended letters are word characters, not separators.

    The naive ``[A-Za-z]+`` regex would split ``café`` into ``caf`` and ``é``,
    which is unacceptable for French, Spanish, Vietnamese, etc.
    """
    assert tokenize_words("caf\u00e9 na\u00efve r\u00e9sum\u00e9") == [
        "caf\u00e9", "na\u00efve", "r\u00e9sum\u00e9"
    ]


def test_words_handles_cyrillic_letters():
    """Cyrillic letters are word characters.

    Russian, Ukrainian, Bulgarian, etc. must tokenize without splitting
    each letter in two.
    """
    assert tokenize_words("\u041f\u0440\u0438\u0432\u0435\u0442 \u043c\u0438\u0440") == [
        "\u041f\u0440\u0438\u0432\u0435\u0442", "\u043c\u0438\u0440"
    ]


def test_words_handles_greek_letters():
    """Greek letters are word characters."""
    assert tokenize_words("\u0393\u03b5\u03b9\u03ac \u03c3\u03bf\u03c5") == [
        "\u0393\u03b5\u03b9\u03ac", "\u03c3\u03bf\u03c5"
    ]


def test_words_handles_cjk_characters():
    """Han characters are tokenized as a single run because the regex
    has no CJK-specific segmentation: a faithful Chinese word tokenizer
    needs a dictionary or learned model (Jieba, etc.), which is out of
    scope here. We guarantee Han at least does not split on whitespace
    or Latin-script confusion.
    """
    assert tokenize_words("\u5317\u4eac\u4eba") == ["\u5317\u4eac\u4eba"]


def test_words_mixed_latin_and_cjk_breaks_at_script_change():
    """Latin and Han live in different Unicode script blocks; even
    without CJK word segmentation, adjacent Latin / Han sequences
    do not fuse into one token because whitespace separates them.
    """
    assert tokenize_words("hello \u4e16\u754c") == ["hello", "\u4e16\u754c"]


# ---------- tokenize_words: numbers ----------------------------------

def test_words_keeps_integer_intact():
    """Integer runs are a single token."""
    assert tokenize_words("I have 42 apples") == ["I", "have", "42", "apples"]


def test_words_keeps_decimal_intact():
    """Decimals stay glued together; the ``.`` is part of the number,
    not a sentence break indicator at the word level."""
    assert tokenize_words("pi is 3.14") == ["pi", "is", "3.14"]


def test_words_handles_thousands_separator_in_european_style():
    """A European-style ``1.000`` (dot thousands sep) is one token."""
    assert tokenize_words("total 1.000 today") == [
        "total", "1.000", "today",
    ]


# ---------- tokenize_sentences: shape and empty inputs ---------------

def test_sentences_empty_string_returns_empty_list():
    """Empty input must yield no sentences."""
    assert tokenize_sentences("") == []


def test_sentences_whitespace_only_returns_empty_list():
    """Whitespace-only input must yield no sentences."""
    assert tokenize_sentences("   \r\n  ") == []


def test_sentences_non_string_raises_type_error():
    """Non-str input must fail loudly."""
    with pytest.raises(TypeError):
        tokenize_sentences(b"hello.")


# ---------- tokenize_sentences: simple splits ------------------------

def test_sentences_single_sentence_unchanged():
    """A lone sentence is returned verbatim."""
    assert tokenize_sentences("Just one.") == ["Just one."]


def test_sentences_two_period_split():
    """Two ASCII sentences split on ``. `` before a capital letter."""
    assert tokenize_sentences("First. Second.") == ["First.", "Second."]


def test_sentences_exclamation_and_question_split():
    """``!`` and ``?`` also start new sentences when followed by a capital."""
    assert tokenize_sentences("Hello! How are you? Fine.") == [
        "Hello!", "How are you?", "Fine.",
    ]


# ---------- tokenize_sentences: abbreviation & decimal & initial -----

def test_sentences_does_not_split_on_abbreviation():
    """A ``.`` after ``Mr``/``Dr``/etc. must not start a new sentence."""
    assert tokenize_sentences("Mr. Smith arrived. He was tired.") == [
        "Mr. Smith arrived.", "He was tired.",
    ]


def test_sentences_handles_multiple_abbreviation_titles():
    """Several consecutive abbreviation titles do not fragment a sentence."""
    assert tokenize_sentences(
        "Dr. and Mrs. Smith went home."
    ) == ["Dr. and Mrs. Smith went home."]


def test_sentences_does_not_split_on_decimal_point():
    """A ``.`` inside a decimal number is not a sentence boundary."""
    assert tokenize_sentences("It costs $3.50 today. Why so much?") == [
        "It costs $3.50 today.", "Why so much?",
    ]


def test_sentences_treats_uniform_initials_as_one_chunk():
    """``J. R. R. Tolkien`` stays as one token-run; ``.`` between
    single uppercase letters is a name initial, not a sentence end."""
    assert tokenize_sentences("J. R. R. Tolkien wrote it.") == [
        "J. R. R. Tolkien wrote it.",
    ]


# ---------- tokenize_sentences: punctuation around the boundary ------

def test_sentences_keeps_closing_quote_attached_to_first_sentence():
    """A closing quote after the terminator belongs to the prior sentence."""
    assert tokenize_sentences('He said "yes." Then he left.') == [
        'He said "yes."', "Then he left.",
    ]


def test_sentences_no_split_when_next_char_is_lowercase():
    """If the next non-whitespace char is lowercase, the ``.`` is part of
    an abbreviation or list, not a sentence boundary."""
    assert tokenize_sentences("e.g. a cat.") == ["e.g. a cat."]


def test_sentences_handles_ellipsis_within_a_sentence():
    """An internal ``...`` does not split when no capital follows."""
    assert tokenize_sentences("Wait... what? Now.") == [
        "Wait... what?", "Now.",
    ]


# ---------- tokenize_sentences: Unicode -------------------------------

def test_sentences_breaks_on_unicode_capital_letter():
    """A capital Cyrillic letter starts a new sentence after ``. ``."""
    assert tokenize_sentences(
        "\u041f\u0440\u0438\u0432\u0435\u0442. \u041a\u0430\u043a \u0434\u0435\u043b\u0430?"
    ) == ["\u041f\u0440\u0438\u0432\u0435\u0442.", "\u041a\u0430\u043a \u0434\u0435\u043b\u0430?"]


# ---------- Regression tests for encoder-bug fixes ---------------------

def test_sentences_opening_quote_after_whitespace_belongs_to_next():
    """Regression: an opening quote that appears *after* inter-sentence
    whitespace must stay with the next sentence, not be swept into the
    prior sentence's slice.
    """
    assert tokenize_sentences('One. "Two."') == ['One.', '"Two."']


def test_sentences_lowercase_accented_letter_is_not_a_boundary():
    """é (U+00E9) is lowercase Latin Extended and must not trigger a
    sentence break; the pre-fix block regex admitted lowercase entries.
    """
    assert tokenize_sentences("Wait... élan is needed.") == [
        "Wait... élan is needed.",
    ]


def test_sentences_lowercase_greek_letter_is_not_a_boundary():
    """The Greek block in the pre-fix regex also admitted lowercase
    letters (e.g. α at U+03B1 should not break a sentence).
    """
    assert tokenize_sentences("Wait... α is greek.") == [
        "Wait... α is greek.",
    ]


def test_sentences_closing_and_opening_quotes_split_correctly():
    """A closing quote immediately after the terminator belongs to the
    prior sentence; an opening quote belonging to the next sentence
    follows whitespace.
    """
    assert tokenize_sentences('"Hello." World.') == [
        '"Hello."', "World.",
    ]


# ---------- Regression tests for reviewer-flagged bugs -----------------

def test_words_nfd_normalizes_to_nfc():
    """Regression: ``'nai\u0308ve'`` (NFD: i + combining diaeresis)
    must tokenize as one word. NFC-normalizing the input collapses the
    combining marks so the existing letter-class regex matches them
    as a single character.
    """
    assert tokenize_words("nai\u0308ve") == ["naïve"]
    # Multiple decomposed words in one input.
    assert tokenize_words("nai\u0308ve cafe\u0301 re\u0301sume\u0301") == [
        "naïve", "café", "résumé",
    ]
    # Precomposed (NFC) input is unchanged by the normalization step.
    assert tokenize_words("naïve café résumé") == [
        "naïve", "café", "résumé",
    ]


def test_sentences_split_after_question_mark_with_single_initial():
    """Regression: a single uppercase letter preceding ``?``/``!`` is
    not a name-initial boundary; those guards only apply to ``.``.
    Without the guard, ``text[i] in {., !, ?}`` would suppress ``?``
    splits and merge ``Who am I?`` with the following sentence.
    """
    assert tokenize_sentences("Who am I? Nobody knows.") == [
        "Who am I?",
        "Nobody knows.",
    ]
    # ``!`` is also a sentence terminator.
    assert tokenize_sentences("Wow! Really? Hmm.") == [
        "Wow!",
        "Really?",
        "Hmm.",
    ]


def test_sentences_initial_guard_still_fires_for_period():
    """Backwards-compat: the abbreviation/initial guards still apply
    to ``.`` so ``Mr.``/``Dr.``/initials like ``J. R. R.`` don't break.
    """
    assert tokenize_sentences("Mr. Smith arrived. He was tired.") == [
        "Mr. Smith arrived.",
        "He was tired.",
    ]
    assert tokenize_sentences("J. R. R. Tolkien wrote it.") == [
        "J. R. R. Tolkien wrote it.",
    ]

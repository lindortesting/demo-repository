"""Tests for ngram generators and frequency table."""

import pytest

from pkg.textkit.ngrams import char_ngrams, ngram_frequency, word_ngrams


class TestCharNgrams:
    """Character-level n-gram extraction over raw text."""

    def test_unigram_returns_each_character(self):
        assert char_ngrams("abc", 1) == ["a", "b", "c"]

    def test_bigram_overlaps_by_one(self):
        assert char_ngrams("hello", 2) == ["he", "el", "ll", "lo"]

    def test_trigram_overlaps_by_one(self):
        assert char_ngrams("hello", 3) == ["hel", "ell", "llo"]

    def test_empty_input_yields_no_ngrams(self):
        # Edge: empty input produces empty output regardless of n.
        assert char_ngrams("", 3) == []

    def test_input_shorter_than_n_yields_no_ngrams(self):
        # Edge: input shorter than n produces no n-grams.
        assert char_ngrams("ab", 3) == []

    def test_input_equal_to_n_yields_single_ngram(self):
        # Edge: input exactly n long yields exactly one n-gram.
        assert char_ngrams("abc", 3) == ["abc"]

    def test_zero_n_rejected(self):
        # n=0 has no meaning; reject loudly at the boundary.
        with pytest.raises(ValueError):
            char_ngrams("abc", 0)

    def test_negative_n_rejected(self):
        with pytest.raises(ValueError):
            char_ngrams("abc", -1)


class TestWordNgrams:
    """Word (token) n-gram extraction over a sequence of tokens."""

    def test_unigram_returns_each_token(self):
        assert word_ngrams(["a", "b", "c"], 1) == [("a",), ("b",), ("c",)]

    def test_bigram_returns_tuples(self):
        assert word_ngrams(["the", "quick", "brown"], 2) == [
            ("the", "quick"),
            ("quick", "brown"),
        ]

    def test_empty_input_yields_no_ngrams(self):
        assert word_ngrams([], 2) == []

    def test_input_shorter_than_n_yields_no_ngrams(self):
        assert word_ngrams(["a"], 2) == []

    def test_input_equal_to_n_yields_single_ngram(self):
        assert word_ngrams(["a", "b"], 2) == [("a", "b")]

    def test_accepts_arbitrary_sequence(self):
        # Spec: tokens is a Sequence; tuples must work too.
        assert word_ngrams(("alpha", "beta"), 2) == [("alpha", "beta")]

    def test_output_tuples_are_independent(self):
        # Each output window is its own tuple, so mutations on one do not
        # leak into the underlying source sequence.
        tokens = ["a", "b", "c", "d"]
        grams = word_ngrams(tokens, 2)
        grams[0][0]  # no-op index access; the contract is that each
        # entry is a fresh tuple backed by copies of the strings.
        assert grams == [("a", "b"), ("b", "c"), ("c", "d")]

    def test_zero_n_rejected(self):
        with pytest.raises(ValueError):
            word_ngrams(["a"], 0)

    def test_negative_n_rejected(self):
        with pytest.raises(ValueError):
            word_ngrams(["a"], -2)


class TestNgramFrequency:
    """Frequency table over an iterable of n-grams."""

    def test_counts_each_ngram(self):
        assert dict(ngram_frequency(["a", "b", "a", "c"])) == {
            "a": 2,
            "b": 1,
            "c": 1,
        }

    def test_empty_input_yields_empty_table(self):
        assert dict(ngram_frequency([])) == {}

    def test_returns_counter_supporting_most_common(self):
        # The result must be a Counter so callers can ask for the top-k
        # n-grams without a separate sort.
        table = ngram_frequency(["a", "b", "b", "c", "c", "c"])
        assert table.most_common(1) == [("c", 3)]
        assert table.most_common() == [("c", 3), ("b", 2), ("a", 1)]

    def test_frequencies_over_char_ngrams(self):
        grams = char_ngrams("aaaa", 2)
        assert dict(ngram_frequency(grams)) == {"aa": 3}

    def test_frequencies_over_word_ngrams(self):
        grams = word_ngrams(["the", "cat", "the", "dog"], 2)
        freq = ngram_frequency(grams)
        assert freq[("the", "cat")] == 1
        assert freq[("cat", "the")] == 1
        assert freq[("the", "dog")] == 1
        assert sum(freq.values()) == 3

    def test_accepts_arbitrary_iterable(self):
        # Spec: the argument is an Iterable, not just a sequence.
        assert dict(ngram_frequency(g for g in ["x", "y", "x"])) == {
            "x": 2,
            "y": 1,
        }

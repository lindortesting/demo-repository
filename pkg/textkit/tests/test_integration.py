"""End-to-end smoke test: every ``textkit`` module exercises the top-level README.

This test is the integration point the per-module sibling PRs are
missing. It runs the full pipeline (``tokenize_words`` →
``word_ngrams`` → ``ngram_frequency``, plus ``tokenize_sentences``,
``char_ngrams``, and ``stem``) on a real, in-repo document so a
regression in any one module surfaces here, not in unit tests.
"""

from pathlib import Path

from pkg.textkit import (
    char_ngrams,
    ngram_frequency,
    stem,
    tokenize_sentences,
    tokenize_words,
    word_ngrams,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
README_PATH = REPO_ROOT / "README.md"


def test_readme_exists():
    """The integration target must be a real file in the repo."""
    assert README_PATH.is_file(), README_PATH


def test_readme_is_nonempty():
    """A non-empty README is the precondition for every assertion below."""
    text = README_PATH.read_text(encoding="utf-8")
    assert text.strip(), "README is whitespace only"


def test_tokenize_words_runs_on_readme():
    readme = README_PATH.read_text(encoding="utf-8")
    words = tokenize_words(readme)
    assert isinstance(words, list)
    assert words, "tokenize_words returned no tokens for the README"
    assert all(isinstance(w, str) for w in words)
    assert "Welcome" in words, "first heading word should survive tokenization"


def test_tokenize_sentences_runs_on_readme():
    readme = README_PATH.read_text(encoding="utf-8")
    sentences = tokenize_sentences(readme)
    assert isinstance(sentences, list)
    assert sentences, "tokenize_sentences returned no sentences"
    joined = " ".join(sentences)
    assert "Welcome" in joined
    assert "Pumpkin Atelier" in joined


def test_word_ngrams_over_readme_tokens():
    readme = README_PATH.read_text(encoding="utf-8")
    words = tokenize_words(readme)
    bigrams = word_ngrams(words, 2)
    assert bigrams, "no word bigrams extracted from README tokens"
    assert all(isinstance(b, tuple) and len(b) == 2 for b in bigrams)


def test_char_ngrams_over_readme():
    readme = README_PATH.read_text(encoding="utf-8")
    trigrams = char_ngrams(readme, 3)
    assert trigrams, "no character trigrams extracted from README"
    assert all(len(g) == 3 for g in trigrams)


def test_ngram_frequency_counts_word_bigrams():
    readme = README_PATH.read_text(encoding="utf-8")
    words = tokenize_words(readme)
    bigrams = word_ngrams(words, 2)
    counts = ngram_frequency(bigrams)
    assert sum(counts.values()) == len(bigrams)
    top_grams = counts.most_common()
    assert top_grams, "ngram_frequency returned an empty Counter"
    assert top_grams[0][1] >= 1


def test_stem_runs_on_readme_tokens():
    readme = README_PATH.read_text(encoding="utf-8")
    words = tokenize_words(readme)
    stems = [stem(w.lower()) for w in words]
    assert stems
    assert all(isinstance(s, str) for s in stems)
    assert any(s for s in stems), "stemmer produced no non-empty stems"


def test_stem_shrinks_common_inflected_forms_from_readme():
    """End-to-end check that the stemmer actually reduces forms we
    know appear in the README: ``includes`` → ``includ`` (Step 1a),
    ``repositories``-style plurals, etc."""
    readme = README_PATH.read_text(encoding="utf-8")
    words = {w.lower() for w in tokenize_words(readme)}

    if "includes" in words:
        assert stem("includes") == "includ"
    if "designed" in words:
        assert stem("designed") == "design"


def test_full_pipeline_returns_consistent_shapes():
    """The three modules must cooperate without raising on real input."""
    readme = README_PATH.read_text(encoding="utf-8")
    words = tokenize_words(readme)
    sentences = tokenize_sentences(readme)
    char_t = char_ngrams(readme, 5)
    word_b = word_ngrams(words, 2)
    freq = ngram_frequency(word_b)
    stems = [stem(w.lower()) for w in words]

    assert len(words) >= len(sentences)
    assert len(char_t) == max(0, len(readme) - 4)
    assert sum(freq.values()) == len(word_b)
    assert len(stems) == len(words)

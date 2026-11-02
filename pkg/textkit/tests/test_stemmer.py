"""Tests for the Porter stemmer implementation in pkg/textkit.stemmer."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from stemmer import stem


def test_step1a_ss():
    assert stem("caresses") == "caress"
    assert stem("passes") == "pass"


def test_step1a_ies():
    assert stem("ponies") == "poni"
    assert stem("ties") == "ti"


def test_step1a_s():
    assert stem("cats") == "cat"


def test_step1a_ss_unchanged():
    assert stem("caress") == "caress"


def test_step1b_eed_to_ee_fires_when_m_gt_0():
    assert stem("agreed") == "agre"
    assert stem("decreed") == "decre"


def test_step1b_ed_uncovered():
    assert stem("plastered") == "plaster"


def test_step1b_ed_at_to_ate():
    assert stem("conflated") == "conflat"


def test_step1b_ed_bl_to_ble():
    assert stem("troubled") == "troubl"


def test_step1b_ed_iz_to_ize():
    assert stem("sized") == "size"


def test_step1b_ing_motor():
    assert stem("motoring") == "motor"


def test_step1b_ing_drops_double():
    assert stem("hopping") == "hop"


def test_step1b_ing_keeps_double_l():
    assert stem("falling") == "fall"


def test_step1b_ing_keeps_double_s():
    assert stem("hissing") == "hiss"


def test_step1b_ing_adds_e():
    assert stem("filing") == "file"


def test_step1b_m_one_no_e():
    assert stem("failing") == "fail"


def test_step1c_y_to_i():
    assert stem("happy") == "happi"


def test_step1c_sk_no_change():
    assert stem("sky") == "sky"


def test_step2_ational():
    assert stem("relational") == "relat"


def test_step2_ization():
    assert stem("vietnamization") == "vietnam"


def test_step2_ator():
    assert stem("operator") == "oper"


def test_step2_alism():
    assert stem("feudalism") == "feudal"


def test_step2_iveness():
    assert stem("decisiveness") == "decis"


def test_step3_icate():
    assert stem("triplicate") == "triplic"


def test_step3_ative():
    assert stem("formative") == "form"


def test_step3_alize():
    assert stem("formalize") == "formal"


def test_step3_ful():
    assert stem("hopeful") == "hope"


def test_step3_ness():
    assert stem("goodness") == "good"


def test_step4_al():
    assert stem("palatable") == "palat"


def test_step4_ance():
    assert stem("allowance") == "allow"


def test_step4_ion_after_s():
    assert stem("decision") == "decis"


def test_step4_ize():
    assert stem("maximize") == "maxim"


def test_step5a_cvc_protects_e():
    # m=1 with cvc-short ending keeps the trailing E.
    assert stem("cave") == "cave"


def test_short_words_passthrough():
    assert stem("") == ""
    assert stem("a") == "a"
    assert stem("by") == "by"


def test_stem_is_stable_for_already_stripped_forms():
    for s in ("poni", "ti", "caress", "mow", "hop", "motor", "size", "run"):
        assert stem(s) == s, s


def test_uppercased_input_normalized():
    assert stem("Cats") == "cat"
    assert stem("PONIES") == "poni"


# ---------- Regression tests for encoder-bug fixes ---------------------

def test_step1b_eed_zero_measure_does_not_leak_to_ed():
    """Regression: when the ``-eed`` rule is checked but fails (measure 0),
    the ``-ed`` rule must still NOT fire on the same word — otherwise
    ``feed`` is collapsed to ``fe``. Porter spec: ``-eed`` is selected
    as a step-1b candidate independently of its measure condition.
    """
    assert stem("feed") == "feed"


def test_measure_vc_transitions_not_v_runs():
    """Regression: Porter measure counts vowel-to-consonant transitions.
    ``tree`` has no such transition, so its measure is 0 and step 5a
    keeps the trailing ``e``.
    """
    assert stem("tree") == "tree"


def test_y_is_vowel_after_consonant_unblocks_ing_strip():
    """Regression: ``y`` after a consonant is a vowel, so ``_has_vowel`` on
    ``cry`` is True and step 1b can peel off the ``-ing`` from ``crying``.
    """
    assert stem("crying") == "cry"


def test_y_is_consonant_at_start_and_after_vowel():
    """Regression: initial ``y`` and ``y`` preceded by a vowel are both
    consonants, encoded non-recursively in ``_cons``.
    """
    from stemmer import _cons
    # Initial y: consonant.
    assert _cons("y", 0) is True
    assert _cons("yacht", 0) is True
    # y after a vowel: consonant.
    assert _cons("boy", 2) is True
    assert _cons("hay", 2) is True
    # y after a consonant: vowel (so the syllable counts).
    assert _cons("cry", 2) is False
    assert _cons("sky", 2) is False
    # Long all-y runs: the lookback must not consume the call stack.
    assert _cons("y" * 5000, 4999) is True


def test_cons_is_iterative_for_long_y_runs():
    """Regression: a long ``y`` run must not consume the call stack.
    Pre-fix, ``_cons`` recursed through every preceding ``y``; after
    Python's recursion limit (≈1000) it would raise ``RecursionError``
    and abort document processing.
    """
    long_word = "y" * 5000 + "e"
    result = stem(long_word)
    assert isinstance(result, str)
    assert result.startswith("y" * 5000)

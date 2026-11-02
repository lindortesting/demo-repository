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

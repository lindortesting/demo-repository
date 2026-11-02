"""Porter stemming algorithm (1980), implemented from the specification.

Reduces a surface English word form to its stem by applying five suffix-stripping
steps keyed on a measure of the stem, plus tests for vowels, double consonants,
and short-syllable (CVC) endings. See Porter, M.F. "An algorithm for suffix
stripping." Program 14(3), July 1980, pages 130-137.
"""

_VOWELS = frozenset("aeiou")


def _cons(word, i):
    """Return True if ``word[i]`` is a consonant for Porter's measure.

    ``y`` is treated as a vowel when preceded by another consonant, as a
    consonant at the start of the word, and as a consonant when preceded
    by a vowel. The function walks past any preceding ``y`` run iteratively
    so input length cannot drive the call stack to its limit.
    """
    if word[i] != "y":
        return word[i] not in _VOWELS
    j = i - 1
    while j >= 0 and word[j] == "y":
        j -= 1
    if j < 0:
        return True
    return word[j] in _VOWELS


def _measure(stem):
    """Return the Porter measure: the number of vowel-to-consonant runs."""
    m = 0
    prev_vowel = False
    for i in range(len(stem)):
        vowel = not _cons(stem, i)
        if not vowel and prev_vowel:
            m += 1
        prev_vowel = vowel
    return m


def _has_vowel(stem):
    return any(not _cons(stem, i) for i in range(len(stem)))


def _double(stem):
    n = len(stem)
    return n >= 2 and stem[-1] == stem[-2] and _cons(stem, n - 1)


def _cvc_short(stem):
    n = len(stem)
    if n < 3 or not _cons(stem, n - 3) or _cons(stem, n - 2) or not _cons(stem, n - 1):
        return False
    return stem[-1] not in "wxy"


def _step1b_cleanup(stem):
    if stem.endswith("at"):
        return stem[:-2] + "ate"
    if stem.endswith("bl"):
        return stem[:-2] + "ble"
    if stem.endswith("iz"):
        return stem[:-2] + "ize"
    if _measure(stem) == 1 and _cvc_short(stem):
        return stem + "e"
    if _double(stem) and stem[-1] not in "lsz":
        return stem[:-1]
    return stem


_STEP_2 = (
    ("ational", "ate"), ("tional", "tion"), ("enci", "ence"), ("anci", "ance"),
    ("izer", "ize"), ("abli", "able"), ("alli", "al"), ("entli", "ent"),
    ("eli", "e"), ("ousli", "ous"), ("ization", "ize"), ("ation", "ate"),
    ("ator", "ate"), ("alism", "al"), ("iveness", "ive"), ("fulness", "ful"),
    ("ousness", "ous"), ("aliti", "al"), ("iviti", "ive"), ("biliti", "ble"),
    ("logi", "log"),
)

_STEP_3 = (
    ("icate", "ic"), ("ative", ""), ("alize", "al"), ("iciti", "ic"),
    ("ical", "ic"), ("ful", ""), ("ness", ""),
)

_STEP_4 = (
    "al", "ance", "ence", "er", "ic", "able", "ible", "ant",
    "ement", "ment", "ent", "ion", "ou", "ism", "ate", "iti",
    "ous", "ive", "ize",
)


def stem(word):
    """Return the Porter stem of a single lowercased word."""
    if len(word) <= 2:
        return word
    w = word.lower()

    if w.endswith("sses"):
        w = w[:-2]
    elif w.endswith("ies"):
        w = w[:-2]
    elif w.endswith("ss"):
        pass
    elif w.endswith("s"):
        w = w[:-1]

    if w.endswith("eed"):
        if _measure(w[:-3]) > 0:
            w = w[:-1]
    elif w.endswith("ed") and _has_vowel(w[:-2]):
        w = _step1b_cleanup(w[:-2])
    elif w.endswith("ing") and _has_vowel(w[:-3]):
        w = _step1b_cleanup(w[:-3])

    if w.endswith("y") and _has_vowel(w[:-1]):
        w = w[:-1] + "i"

    for suf, repl in _STEP_2:
        n = len(suf)
        if w.endswith(suf) and _measure(w[:-n]) > 0:
            w = w[:-n] + repl
            break

    for suf, repl in _STEP_3:
        n = len(suf)
        if w.endswith(suf) and _measure(w[:-n]) > 0:
            w = w[:-n] + repl
            break

    for suf in _STEP_4:
        n = len(suf)
        if w.endswith(suf) and _measure(w[:-n]) > 1:
            if suf == "ion" and not (w[:-n].endswith("s") or w[:-n].endswith("t")):
                continue
            w = w[:-n]
            break

    if w.endswith("e"):
        base = w[:-1]
        if _measure(base) > 1 or (_measure(base) == 1 and not _cvc_short(base)):
            w = base

    if _double(w) and _measure(w[:-1]) > 1 and w.endswith("l"):
        w = w[:-1]

    return w

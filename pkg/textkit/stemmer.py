"""Porter stemming algorithm (1980), implemented from the specification.

Reduces a surface English word form to its stem by applying five suffix-stripping
steps keyed on a measure of the stem, plus tests for vowels, double consonants,
and short-syllable (CVC) endings. See Porter, M.F. "An algorithm for suffix
stripping." Program 14(3), July 1980, pages 130-137.
"""

_VOWELS = frozenset("aeiou")


def _classify(word):
    """Consonant/vowel classification for every position in ``word``.

    Porter's rule for ``y``: consonant at the start of the word,
    consonant after a vowel, vowel after a consonant. The whole
    array is built in a single forward pass so a long ``y`` run
    (e.g. 100_000 contiguous ``y``s) is handled in O(n); repeated
    measure/scvc/double checks then look up each class in O(1).
    """
    n = len(word)
    classes = [False] * n
    for i, ch in enumerate(word):
        if ch != "y":
            classes[i] = ch not in _VOWELS
        elif i == 0:
            classes[i] = True
        elif word[i - 1] == "y":
            classes[i] = classes[i - 1]
        else:
            classes[i] = word[i - 1] in _VOWELS
    return classes


def _measure(cls, end):
    """Porter measure of ``word[:end]`` using precomputed ``cls``."""
    m = 0
    prev_vowel = None
    for i in range(end):
        is_vowel = not cls[i]
        if prev_vowel is not None and prev_vowel and not is_vowel:
            m += 1
        prev_vowel = is_vowel
    return m


def _has_vowel(cls, end):
    for i in range(end):
        if not cls[i]:
            return True
    return False


def _double(word, cls, end):
    return (
        end >= 2
        and word[end - 1] == word[end - 2]
        and cls[end - 1]
    )


def _cvc_short(word, cls, end):
    if end < 3 or not cls[end - 3] or cls[end - 2] or not cls[end - 1]:
        return False
    return word[end - 1] not in "wxy"


def _step1b_cleanup(word, cls, end):
    """Apply the post-strip cleanup rules and return ``(w, new_end)``."""
    if word[end - 2:end] == "at":
        return word[:end - 2] + "ate", end - 2 + 3
    if word[end - 2:end] == "bl":
        return word[:end - 2] + "ble", end - 2 + 3
    if word[end - 2:end] == "iz":
        return word[:end - 2] + "ize", end - 2 + 3
    if _measure(cls, end) == 1 and _cvc_short(word, cls, end):
        return word[:end] + "e", end + 1
    if _double(word, cls, end) and word[end - 1] not in "lsz":
        return word[:end - 1], end - 1
    return word[:end], end


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
    n = len(w)
    cls = _classify(w)

    if w.endswith("sses"):
        n -= 2
        w = w[:n]
        cls = cls[:n]
    elif w.endswith("ies"):
        n -= 2
        w = w[:n]
        cls = cls[:n]
    elif w.endswith("ss"):
        pass
    elif w.endswith("s"):
        n -= 1
        w = w[:n]
        cls = cls[:n]

    if w.endswith("eed"):
        if _measure(cls, n - 3) > 0:
            n -= 1
            w = w[:n]
            cls = cls[:n]
    elif w.endswith("ed") and _has_vowel(cls, n - 2):
        n -= 2
        w = w[:n]
        cls = cls[:n]
        w, n = _step1b_cleanup(w, cls, n)
    elif w.endswith("ing") and _has_vowel(cls, n - 3):
        n -= 3
        w = w[:n]
        cls = cls[:n]
        w, n = _step1b_cleanup(w, cls, n)

    if w.endswith("y") and _has_vowel(cls, n - 1):
        w = w[:n - 1] + "i"
        cls = cls[:n - 1] + [False]
        n -= 0

    for suf, repl in _STEP_2:
        sl = len(suf)
        if w.endswith(suf) and _measure(cls, n - sl) > 0:
            w = w[:n - sl] + repl
            cls = cls[:n - sl] + [ch not in _VOWELS for ch in repl]
            n = len(w)
            break

    for suf, repl in _STEP_3:
        sl = len(suf)
        if w.endswith(suf) and _measure(cls, n - sl) > 0:
            w = w[:n - sl] + repl
            cls = cls[:n - sl] + [ch not in _VOWELS for ch in repl]
            n = len(w)
            break

    for suf in _STEP_4:
        sl = len(suf)
        if w.endswith(suf) and _measure(cls, n - sl) > 1:
            if suf == "ion" and not (w[:n - sl].endswith("s") or w[:n - sl].endswith("t")):
                continue
            w = w[:n - sl]
            cls = cls[:n - sl]
            n -= sl
            break

    if w.endswith("e"):
        if _measure(cls, n - 1) > 1 or (
            _measure(cls, n - 1) == 1 and not _cvc_short(w, cls, n - 1)
        ):
            n -= 1
            w = w[:n]
            cls = cls[:n]

    if _double(w, cls, n) and _measure(cls, n - 1) > 1 and w.endswith("l"):
        n -= 1
        w = w[:n]
        cls = cls[:n]

    return w

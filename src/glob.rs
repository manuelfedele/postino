//! Glob patterns: `*` matches any run of characters, `?` one character,
//! `[abc]`, `[a-z]`, and `[^...]` one of a set, and `\` makes the next
//! character literal. A class that is never closed matches nothing.
//!
//! A pattern is compiled once, then matched in time linear in the text
//! whatever the pattern: a backtracking matcher would let one crafted
//! pattern hold the server for seconds.

use std::fmt;

/// The longest pattern, in bytes, a client may send.
pub const MAX_PATTERN: usize = 256;

#[derive(Clone)]
pub struct Glob {
    source: String,
    kind: Kind,
}

#[derive(Clone)]
enum Kind {
    Any,
    Exact(String),
    Prefix(String),
    Suffix(String),
    Contains(String),
    Never,
    Automaton(Automaton),
}

#[derive(Clone, PartialEq)]
enum Atom {
    Star,
    One,
    Char(char),
    Set {
        negate: bool,
        ranges: Vec<(char, char)>,
    },
}

impl Atom {
    fn accepts(&self, c: char) -> bool {
        match self {
            Atom::Star | Atom::One => true,
            Atom::Char(x) => *x == c,
            Atom::Set { negate, ranges } => {
                ranges.iter().any(|(lo, hi)| *lo <= c && c <= *hi) != *negate
            }
        }
    }
}

impl Glob {
    pub fn new(pattern: &str) -> Glob {
        let kind = match atoms(pattern) {
            None => Kind::Never,
            Some(atoms) => classify(atoms),
        };
        Glob {
            source: pattern.to_string(),
            kind,
        }
    }

    /// A client's pattern, refused when longer than [`MAX_PATTERN`].
    pub fn client(pattern: &str) -> Result<Glob, String> {
        if pattern.len() > MAX_PATTERN {
            return Err(format!("ERR patterns are at most {MAX_PATTERN} bytes long"));
        }
        Ok(Glob::new(pattern))
    }

    pub fn as_str(&self) -> &str {
        &self.source
    }

    pub fn matches(&self, text: &str) -> bool {
        match &self.kind {
            Kind::Any => true,
            Kind::Exact(s) => text == s,
            Kind::Prefix(s) => text.starts_with(s.as_str()),
            Kind::Suffix(s) => text.ends_with(s.as_str()),
            Kind::Contains(s) => text.contains(s.as_str()),
            Kind::Never => false,
            Kind::Automaton(a) => a.matches(text),
        }
    }
}

impl PartialEq for Glob {
    fn eq(&self, other: &Glob) -> bool {
        self.source == other.source
    }
}

impl fmt::Debug for Glob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Glob({:?})", self.source)
    }
}

/// The pattern's atoms, with runs of `*` merged; `None` when a class is
/// never closed.
fn atoms(pattern: &str) -> Option<Vec<Atom>> {
    let p: Vec<char> = pattern.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < p.len() {
        let atom = match p[i] {
            '*' => {
                i += 1;
                if out.last() == Some(&Atom::Star) {
                    continue;
                }
                Atom::Star
            }
            '?' => {
                i += 1;
                Atom::One
            }
            '[' => {
                let (atom, next) = class(&p, i)?;
                i = next;
                atom
            }
            '\\' if i + 1 < p.len() => {
                i += 2;
                Atom::Char(p[i - 1])
            }
            c => {
                i += 1;
                Atom::Char(c)
            }
        };
        out.push(atom);
    }
    Some(out)
}

/// The class opening at `p[start]`, and where the pattern goes on after it.
/// A `]` right after the opening (or after `^`) belongs to the class.
fn class(p: &[char], start: usize) -> Option<(Atom, usize)> {
    let mut i = start + 1;
    let negate = p.get(i) == Some(&'^');
    if negate {
        i += 1;
    }
    let mut ranges = Vec::new();
    let mut first = true;
    while i < p.len() && (p[i] != ']' || first) {
        first = false;
        let mut lo = p[i];
        if lo == '\\' && i + 1 < p.len() {
            i += 1;
            lo = p[i];
        }
        if i + 2 < p.len() && p[i + 1] == '-' && p[i + 2] != ']' {
            let hi = p[i + 2];
            ranges.push(if lo <= hi { (lo, hi) } else { (hi, lo) });
            i += 3;
        } else {
            ranges.push((lo, lo));
            i += 1;
        }
    }
    (i < p.len()).then_some((Atom::Set { negate, ranges }, i + 1))
}

/// The cheapest way to match these atoms.
fn classify(atoms: Vec<Atom>) -> Kind {
    let literal = |atoms: &[Atom]| -> Option<String> {
        atoms
            .iter()
            .map(|a| match a {
                Atom::Char(c) => Some(*c),
                _ => None,
            })
            .collect()
    };
    let star = |a: Option<&Atom>| a == Some(&Atom::Star);
    let n = atoms.len();
    if n == 1 && star(atoms.first()) {
        return Kind::Any;
    }
    if let Some(s) = literal(&atoms) {
        return Kind::Exact(s);
    }
    if n >= 2 {
        let (head, tail) = (star(atoms.first()), star(atoms.last()));
        let inner = &atoms[usize::from(head)..n - usize::from(tail)];
        if let Some(s) = literal(inner) {
            match (head, tail) {
                (false, true) => return Kind::Prefix(s),
                (true, false) => return Kind::Suffix(s),
                (true, true) => return Kind::Contains(s),
                (false, false) => {}
            }
        }
    }
    Kind::Automaton(Automaton::new(atoms))
}

/// The atoms as a nondeterministic automaton run with one bit per state:
/// state `i` means the first `i` atoms have matched, and every live state
/// advances together on each character of the text.
#[derive(Clone)]
struct Automaton {
    atoms: Vec<Atom>,
    words: usize,
    /// Bit `i`: atom `i` is a star, so state `i` stays live on any character.
    stars: Vec<u64>,
    /// For each ASCII character, bit `i`: atom `i` is not a star and accepts it.
    ascii: Vec<u64>,
}

/// Bitset words matched on the stack; longer patterns use the heap.
const STACK_WORDS: usize = 8;

impl Automaton {
    fn new(atoms: Vec<Atom>) -> Automaton {
        let words = atoms.len() / 64 + 1;
        let mut stars = vec![0; words];
        let mut ascii = vec![0; 128 * words];
        for (i, atom) in atoms.iter().enumerate() {
            let bit = 1u64 << (i % 64);
            if *atom == Atom::Star {
                stars[i / 64] |= bit;
                continue;
            }
            for c in 0..128u8 {
                if atom.accepts(c as char) {
                    ascii[c as usize * words + i / 64] |= bit;
                }
            }
        }
        Automaton {
            atoms,
            words,
            stars,
            ascii,
        }
    }

    fn matches(&self, text: &str) -> bool {
        let w = self.words;
        if w <= STACK_WORDS {
            let mut buf = [0u64; 3 * STACK_WORDS];
            let (state, rest) = buf.split_at_mut(STACK_WORDS);
            let (next, mask) = rest.split_at_mut(STACK_WORDS);
            self.run(text, &mut state[..w], &mut next[..w], &mut mask[..w])
        } else {
            let mut buf = vec![0u64; 3 * w];
            let (state, rest) = buf.split_at_mut(w);
            let (next, mask) = rest.split_at_mut(w);
            self.run(text, state, next, mask)
        }
    }

    fn run(&self, text: &str, state: &mut [u64], next: &mut [u64], mask: &mut [u64]) -> bool {
        let w = self.words;
        state[0] = 1;
        self.close(state);
        for c in text.chars() {
            let accepts: &[u64] = if c.is_ascii() {
                &self.ascii[c as usize * w..(c as usize + 1) * w]
            } else {
                mask.fill(0);
                for (i, atom) in self.atoms.iter().enumerate() {
                    if *atom != Atom::Star && atom.accepts(c) {
                        mask[i / 64] |= 1 << (i % 64);
                    }
                }
                mask
            };
            let mut carry = 0;
            let mut live = 0;
            for k in 0..w {
                let advanced = state[k] & accepts[k];
                next[k] = (advanced << 1) | carry | (state[k] & self.stars[k]);
                carry = advanced >> 63;
                live |= next[k];
            }
            if live == 0 {
                return false;
            }
            state.copy_from_slice(next);
            self.close(state);
        }
        let end = self.atoms.len();
        state[end / 64] & (1 << (end % 64)) != 0
    }

    /// Adds the states a star reaches by matching nothing. Runs of stars are
    /// merged, so one step reaches them all.
    fn close(&self, state: &mut [u64]) {
        let mut carry = 0;
        for (word, stars) in state.iter_mut().zip(&self.stars) {
            let starred = *word & stars;
            *word |= (starred << 1) | carry;
            carry = starred >> 63;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The matcher this module replaced, which backtracks to the last star.
    /// Kept to check that compiled patterns mean the same.
    fn reference(pattern: &str, text: &str) -> bool {
        fn class(p: &[char], start: usize, c: char) -> Option<(bool, usize)> {
            let mut i = start + 1;
            let negate = p.get(i) == Some(&'^');
            if negate {
                i += 1;
            }
            let mut hit = false;
            let mut first = true;
            while i < p.len() && (p[i] != ']' || first) {
                first = false;
                let mut lo = p[i];
                if lo == '\\' && i + 1 < p.len() {
                    i += 1;
                    lo = p[i];
                }
                if i + 2 < p.len() && p[i + 1] == '-' && p[i + 2] != ']' {
                    let hi = p[i + 2];
                    let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
                    hit |= lo <= c && c <= hi;
                    i += 3;
                } else {
                    hit |= lo == c;
                    i += 1;
                }
            }
            (i < p.len()).then_some((hit != negate, i + 1))
        }
        let p: Vec<char> = pattern.chars().collect();
        let t: Vec<char> = text.chars().collect();
        let (mut pi, mut ti) = (0, 0);
        let mut star: Option<(usize, usize)> = None;
        while ti < t.len() {
            if pi < p.len() {
                match p[pi] {
                    '*' => {
                        star = Some((pi, ti));
                        pi += 1;
                        continue;
                    }
                    '?' => {
                        pi += 1;
                        ti += 1;
                        continue;
                    }
                    '[' => {
                        if let Some((true, next)) = class(&p, pi, t[ti]) {
                            pi = next;
                            ti += 1;
                            continue;
                        }
                    }
                    '\\' if pi + 1 < p.len() => {
                        if p[pi + 1] == t[ti] {
                            pi += 2;
                            ti += 1;
                            continue;
                        }
                    }
                    c if c == t[ti] => {
                        pi += 1;
                        ti += 1;
                        continue;
                    }
                    _ => {}
                }
            }
            match star {
                Some((sp, st)) => {
                    pi = sp + 1;
                    ti = st + 1;
                    star = Some((sp, st + 1));
                }
                None => return false,
            }
        }
        p[pi..].iter().all(|c| *c == '*')
    }

    #[test]
    fn patterns_match_as_documented() {
        let glob = |p: &str, t: &str| Glob::new(p).matches(t);
        assert!(glob("*", "anything") && glob("*", ""));
        assert!(glob("task:*", "task:42") && !glob("task:*", "job:42"));
        assert!(glob("*:done", "run:7:done") && glob("*:7:*", "run:7:done"));
        assert!(glob("h?llo", "hello") && !glob("h?llo", "hllo"));
        assert!(glob("h[ae]llo", "hallo") && !glob("h[^e]llo", "hello"));
        assert!(glob("h[a-c]llo", "hbllo") && glob("h[c-a]llo", "hbllo"));
        assert!(glob("[]x]", "]") && glob("[^]x]", "y") && !glob("[^]x]", "]"));
        assert!(glob("a\\*b", "a*b") && !glob("a\\*b", "axb"));
        assert!(glob("a\\", "a\\"), "a trailing backslash is literal");
        assert!(
            !glob("a[b", "a[b") && !glob("[", ""),
            "an open class matches nothing"
        );
        assert!(glob("*:*:done", "run:7:done") && glob("é*?", "éé"));
    }

    #[test]
    fn compiled_patterns_agree_with_the_backtracking_matcher() {
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let pattern_chars = ['*', '?', '[', ']', '^', '-', '\\', 'a', 'b', 'é', ':'];
        let text_chars = ['a', 'b', 'é', ':', '*', '[', ']', '-', '\\', '^', '?'];
        for _ in 0..200_000 {
            let pattern: String = (0..next() % 9)
                .map(|_| pattern_chars[next() as usize % pattern_chars.len()])
                .collect();
            let text: String = (0..next() % 9)
                .map(|_| text_chars[next() as usize % text_chars.len()])
                .collect();
            assert_eq!(
                Glob::new(&pattern).matches(&text),
                reference(&pattern, &text),
                "{pattern:?} against {text:?}"
            );
        }
    }

    #[test]
    fn long_patterns_use_several_words() {
        let long = format!("{}b*c", "?".repeat(130));
        let text = format!("{}bxxc", "a".repeat(130));
        assert!(Glob::new(&long).matches(&text));
        assert!(!Glob::new(&long).matches(&text[1..]));
        let huge = format!("*{}*", "a?".repeat(400));
        assert!(Glob::new(&huge).matches(&format!("x{}x", "ab".repeat(400))));
        assert!(reference(&long, &text));
    }

    #[test]
    fn a_crafted_pattern_costs_time_linear_in_the_text() {
        let text = "a".repeat(100_000);
        let started = std::time::Instant::now();
        assert!(!Glob::new(&format!("*{}b", "a".repeat(250))).matches(&text));
        assert!(!Glob::new(&format!("*{}b", "a*".repeat(120))).matches(&text));
        assert!(!Glob::new(&format!("*{}b", "?".repeat(250))).matches(&text));
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn client_patterns_are_bounded() {
        assert!(Glob::client(&"*".repeat(MAX_PATTERN)).is_ok());
        assert!(Glob::client(&"*".repeat(MAX_PATTERN + 1)).is_err());
    }
}

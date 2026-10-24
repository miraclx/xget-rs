use std::sync::Arc;

/// Parsed include/exclude intent for original, validated source-relative member paths.
///
/// This does not select member IDs, validate inventories, or assign destination names. Callers must
/// distinguish empty discovery from a nonempty inventory for which `matches` always returns false.
/// References share this immutable value through Arc. Adding intent shares parsed patterns rather
/// than reparsing or copying spelling/token allocations; bounded pointer-list copies stay explicit.
#[derive(Debug, Default)]
pub(crate) struct Selection {
    includes: Vec<Arc<Pattern>>,
    excludes: Vec<Arc<Pattern>>,
}

impl Selection {
    /// Parses both lists together so exclusions cannot bypass the shared retention budget.
    ///
    /// Limits are 64 total patterns, 1024 UTF-8 bytes per pattern, 64 components per pattern and
    /// 16384 total spelling bytes. Order, duplicates and original spelling are retained for intent
    /// fingerprinting; they have no effect on union matching. An empty include list means all paths.
    #[cfg(test)]
    pub(crate) fn parse(includes: &[&str], excludes: &[&str]) -> Result<Self, PatternError> {
        if includes.len().saturating_add(excludes.len()) > MAX_PATTERNS {
            return Err(PatternError::Count);
        }
        let mut bytes = 0_usize;
        // Check the entire shared budget before allocating retained spellings or parsed tokens.
        for spelling in includes.iter().chain(excludes) {
            if spelling.len() > MAX_PATTERN_BYTES {
                return Err(PatternError::Length);
            }
            bytes = bytes
                .checked_add(spelling.len())
                .ok_or(PatternError::RetainedBytes)?;
            if bytes > MAX_RETAINED_BYTES {
                return Err(PatternError::RetainedBytes);
            }
        }
        Ok(Self {
            includes: includes
                .iter()
                .map(|text| Pattern::parse(text).map(Arc::new))
                .collect::<Result<_, _>>()?,
            excludes: excludes
                .iter()
                .map(|text| Pattern::parse(text).map(Arc::new))
                .collect::<Result<_, _>>()?,
        })
    }

    pub(crate) fn including(&self, spelling: &str) -> Result<Self, PatternError> {
        self.append(spelling, Role::Include)
    }

    pub(crate) fn excluding(&self, spelling: &str) -> Result<Self, PatternError> {
        self.append(spelling, Role::Exclude)
    }

    /// Spelling bytes participate in the common retained-request/discovery text allowance too.
    pub(crate) fn retained_bytes(&self) -> usize {
        self.includes().chain(self.excludes()).map(str::len).sum()
    }

    fn append(&self, spelling: &str, role: Role) -> Result<Self, PatternError> {
        if self.includes.len() + self.excludes.len() >= MAX_PATTERNS {
            return Err(PatternError::Count);
        }
        if spelling.len() > MAX_PATTERN_BYTES {
            return Err(PatternError::Length);
        }
        if self.retained_bytes().saturating_add(spelling.len()) > MAX_RETAINED_BYTES {
            return Err(PatternError::RetainedBytes);
        }
        let pattern = Arc::new(Pattern::parse(spelling)?);
        // Adding intent shares immutable parsed patterns, never reparses or copies their text/tokens.
        // Member/reference duplication shares the entire Selection, without even copying these lists.
        let mut updated = Self {
            includes: self.includes.iter().map(Arc::clone).collect(),
            excludes: self.excludes.iter().map(Arc::clone).collect(),
        };
        match role {
            Role::Include => updated.includes.push(pattern),
            Role::Exclude => updated.excludes.push(pattern),
        }
        Ok(updated)
    }

    /// False means the driver must preserve its pre-pattern request fingerprint unchanged.
    pub(crate) fn is_configured(&self) -> bool {
        !self.includes.is_empty() || !self.excludes.is_empty()
    }

    /// Stable include spellings in caller order, without normalization or deduplication.
    pub(crate) fn includes(&self) -> impl Iterator<Item = &str> {
        self.includes
            .iter()
            .map(|pattern| pattern.spelling.as_str())
    }

    /// Stable exclusion spellings in caller order, kept separate from includes for fingerprinting.
    pub(crate) fn excludes(&self) -> impl Iterator<Item = &str> {
        self.excludes
            .iter()
            .map(|pattern| pattern.spelling.as_str())
    }

    /// Bounds matching work without assigning filesystem meaning to remote path components.
    pub(crate) fn path_is_bounded(path: &str) -> bool {
        path.len() <= MAX_PATH_BYTES && path.split('/').count() <= MAX_DEPTH
    }

    /// Matches a complete ORIGINAL source-relative path, never a namespace or destination path.
    ///
    /// The transport supplies its original path spelling; collections separately validate their
    /// materializable hierarchy. Exact HTTP ancestors need not be portable local filenames.
    /// Byte/depth guards bound matcher work. No Unicode normalization, case folding, URL decoding
    /// or filesystem interpretation occurs. Separators are exclusively '/'.
    pub(crate) fn matches(&self, original_path: &str) -> bool {
        if !Self::path_is_bounded(original_path) {
            return false;
        }
        (self.includes.is_empty()
            || self
                .includes
                .iter()
                .any(|pattern| pattern.matches(original_path)))
            && !self
                .excludes
                .iter()
                .any(|pattern| pattern.matches(original_path))
    }
}

/// Private fixed budgets, not public tuning knobs. Lengths and counts index bounded collections.
const MAX_PATTERNS: usize = 64;
const MAX_PATTERN_BYTES: usize = 1024;
const MAX_RETAINED_BYTES: usize = 16384;
const MAX_DEPTH: usize = 64;
const MAX_PATH_BYTES: usize = 4096;

/// Invalid bounded path-selection intent, without retaining rejected pattern text.
///
/// These fixed limits apply to includes and exclusions together for one input occurrence.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum PatternError {
    /// More than 64 includes and exclusions were supplied in total.
    #[error("too many path patterns; use at most 64 includes and exclusions combined")]
    Count,
    /// One spelling exceeds 1024 UTF-8 bytes.
    #[error("path pattern is too long; use at most 1024 UTF-8 bytes")]
    Length,
    /// The combined spellings exceed 16384 UTF-8 bytes.
    #[error("path patterns exceed the shared byte budget; use at most 16384 UTF-8 bytes combined")]
    RetainedBytes,
    /// One pattern exceeds 64 slash-separated components.
    #[error("path pattern is too deep; use at most 64 components")]
    Depth,
    /// The spelling is not in the documented relative bounded-glob grammar.
    #[error(
        "invalid path pattern; use relative slash-separated literals, *, ? or complete ** components"
    )]
    Syntax,
}

enum Role {
    Include,
    Exclude,
}

#[derive(Debug)]
struct Pattern {
    spelling: String,
    components: Vec<Component>,
}

impl Pattern {
    /// The grammar deliberately has no escape, bracket, brace or extglob forms. Colon is reserved
    /// too, rejecting drive-qualified paths and matching the portable source-path restriction.
    /// '*' consumes zero or more scalars within a component; '?' consumes exactly one scalar.
    /// '**' is permitted only as a whole component and consumes zero or more whole components.
    fn parse(spelling: &str) -> Result<Self, PatternError> {
        if spelling.split('/').count() > MAX_DEPTH {
            return Err(PatternError::Depth);
        }
        let mut components = Vec::new();
        for part in spelling.split('/') {
            if part.is_empty()
                || matches!(part, "." | "..")
                || part.chars().any(|character| {
                    character.is_control()
                        || matches!(character, '\\' | ':' | '[' | ']' | '{' | '}' | '(' | ')')
                })
            {
                return Err(PatternError::Syntax);
            }
            if part == "**" {
                components.push(Component::Recursive);
                continue;
            }
            if part.contains("**") {
                return Err(PatternError::Syntax);
            }
            components.push(Component::Local(part.chars().map(Token::from).collect()));
        }
        Ok(Self {
            spelling: spelling.to_owned(),
            components,
        })
    }

    /// Two iterative dynamic programs, without recursive calls or backtracking. For B parsed
    /// pattern scalars, D pattern components, L path scalars and H path components, work is
    /// O(B*L + D*H); scratch is O(B + D), independent of wildcard ambiguity. The outer program
    /// tracks component boundaries, while the inner program tracks scalar boundaries. Patterns
    /// are visited serially, so scratch is not multiplied by the retained pattern count.
    fn matches(&self, path: &str) -> bool {
        let mut current = vec![false; self.components.len() + 1];
        let mut next = vec![false; current.len()];
        let max_tokens = self
            .components
            .iter()
            .map(|component| match component {
                Component::Recursive => 0,
                Component::Local(tokens) => tokens.len(),
            })
            .max()
            .unwrap_or(0);
        let mut local = vec![false; max_tokens + 1];
        current[0] = true;
        self.skip_recursive(&mut current);
        for part in path.split('/') {
            next.fill(false);
            for (index, component) in self.components.iter().enumerate() {
                if !current[index] {
                    continue;
                }
                match component {
                    Component::Recursive => next[index] = true,
                    Component::Local(tokens) => {
                        if Token::matches(tokens, part, &mut local) {
                            next[index + 1] = true;
                        }
                    }
                }
            }
            self.skip_recursive(&mut next);
            core::mem::swap(&mut current, &mut next);
        }
        current[self.components.len()]
    }

    fn skip_recursive(&self, states: &mut [bool]) {
        for (index, component) in self.components.iter().enumerate() {
            if states[index] && matches!(component, Component::Recursive) {
                states[index + 1] = true;
            }
        }
    }
}

#[derive(Debug)]
enum Component {
    Recursive,
    Local(Vec<Token>),
}

#[derive(Debug)]
enum Token {
    Literal(char),
    One,
    Many,
}

impl From<char> for Token {
    fn from(character: char) -> Self {
        match character {
            '?' => Self::One,
            '*' => Self::Many,
            literal => Self::Literal(literal),
        }
    }
}

impl Token {
    fn matches(tokens: &[Self], part: &str, scratch: &mut [bool]) -> bool {
        let row = &mut scratch[..=tokens.len()];
        row.fill(false);
        row[0] = true;
        for (index, token) in tokens.iter().enumerate() {
            row[index + 1] = row[index] && matches!(token, Self::Many);
        }
        for scalar in part.chars() {
            let mut diagonal = row[0];
            row[0] = false;
            for (index, token) in tokens.iter().enumerate() {
                let previous = row[index + 1];
                row[index + 1] = match token {
                    Self::Literal(literal) => diagonal && *literal == scalar,
                    Self::One => diagonal,
                    Self::Many => previous || row[index],
                };
                diagonal = previous;
            }
        }
        row[tokens.len()]
    }
}

#[cfg(test)]
#[path = "selection_tests.rs"]
mod tests;

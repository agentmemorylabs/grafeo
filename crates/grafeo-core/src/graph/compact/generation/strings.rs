//! One global UTF-8 lexicographic string-code space.

use super::error::GenerationError;
use grafeo_common::utils::hash::FxHashMap;

/// Global dictionary: code `i` is `strings[i]` in UTF-8 byte order.
#[derive(Debug, Clone, Default)]
pub struct GlobalStringDictionary {
    /// Unique strings in lexicographic order.
    pub strings: Vec<String>,
    /// string → code.
    pub code_of: FxHashMap<String, u32>,
}

impl GlobalStringDictionary {
    /// Lookup code for a string.
    #[must_use]
    pub fn code(&self, s: &str) -> Option<u32> {
        self.code_of.get(s).copied()
    }

    /// Number of unique strings.
    #[must_use]
    pub fn len(&self) -> usize {
        self.strings.len()
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }

    /// Slice of strings in lexicographic order.
    #[must_use]
    pub fn as_slice(&self) -> &[String] {
        &self.strings
    }
}

/// Collect unique strings, sort UTF-8 lexicographically, assign dense codes.
///
/// # Errors
///
/// [`GenerationError::WireWidthOverflow`] when unique count exceeds `u32::MAX`.
pub fn collect_and_assign_global_codes(
    occurrences: impl IntoIterator<Item = String>,
) -> Result<GlobalStringDictionary, GenerationError> {
    let mut uniq: Vec<String> = occurrences.into_iter().collect();
    uniq.sort();
    uniq.dedup();
    if uniq.len() > u32::MAX as usize {
        return Err(GenerationError::WireWidthOverflow {
            what: "global_string_dictionary",
            count: uniq.len() as u64,
            max: u64::from(u32::MAX),
        });
    }
    let mut code_of = FxHashMap::default();
    for (i, s) in uniq.iter().enumerate() {
        // Validated above: len fits u32.
        #[allow(clippy::cast_possible_truncation)]
        code_of.insert(s.clone(), i as u32);
    }
    Ok(GlobalStringDictionary {
        strings: uniq,
        code_of,
    })
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn lex_order_is_byte_order_and_dedupes() {
        let d = collect_and_assign_global_codes([
            "zebra".into(),
            "apple".into(),
            "apple".into(),
            "Mango".into(),
            "apple".into(),
        ])
        .unwrap();
        assert_eq!(d.strings, vec!["Mango", "apple", "zebra"]);
        assert_eq!(d.code("Mango"), Some(0));
        assert_eq!(d.code("apple"), Some(1));
        assert_eq!(d.code("zebra"), Some(2));
    }
}

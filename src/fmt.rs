use std::fmt::Display;

pub trait DisplayBytesEx: AsRef<[u8]> {
    fn display<'a>(&'a self) -> DisplayBytes<&'a Self> {
        DisplayBytes(self)
    }
}

impl<T: AsRef<[u8]>> DisplayBytesEx for T {}

pub struct DisplayBytes<T>(T);

impl<T: AsRef<[u8]>> Display for DisplayBytes<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt_quoted_bytes(self.0.as_ref(), f, None)
    }
}

fn fmt_quoted_bytes(
    mut bytes: &[u8],
    f: &mut std::fmt::Formatter,
    quote: Option<char>,
) -> std::fmt::Result {
    loop {
        match std::str::from_utf8(bytes) {
            Ok(text) => {
                fmt_quoted_str(text, f, quote)?;
                break;
            }
            Err(error) => {
                let (valid, after_valid) = bytes.split_at(error.valid_up_to());
                unsafe {
                    fmt_quoted_str(std::str::from_utf8_unchecked(valid), f, quote)?;
                }
                let invalid = if let Some(invalid_sequence_length) = error.error_len() {
                    &after_valid[..invalid_sequence_length]
                } else {
                    after_valid
                };
                for byte in invalid {
                    std::fmt::Display::fmt(&byte.escape_ascii(), f)?;
                }
                if let Some(invalid_sequence_length) = error.error_len() {
                    bytes = &after_valid[invalid_sequence_length..];
                } else {
                    break;
                }
            }
        }
    }
    Ok(())
}

fn fmt_quoted_str(
    mut text: &str,
    f: &mut std::fmt::Formatter,
    quote: Option<char>,
) -> std::fmt::Result {
    while let Some(index) = text.find(|x| Some(x) == quote || x == '\\' || x.is_ascii_control()) {
        if index != 0 {
            f.write_str(&text[..index])?;
        }
        text = &text[index..];
        debug_assert!(!text.is_empty());
        match quote {
            Some(quote @ '\u{80}'..)
                if !text
                    .as_bytes()
                    .first()
                    .is_some_and(|x| x.is_ascii_control() || *x == b'\\') =>
            {
                std::fmt::Display::fmt(&quote.escape_default(), f)?;
                text = &text[quote.len_utf8()..];
            }
            _ => {
                std::fmt::Display::fmt(&text.as_bytes()[0].escape_ascii(), f)?;
                text = &text[1..];
            }
        }
    }
    f.write_str(text)
}

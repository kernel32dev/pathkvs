use std::{
    fmt::Display,
    time::{Duration, SystemTime},
};

use chrono::Local;

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

pub fn parse_general_timestamp(input: &str) -> Option<SystemTime> {
    let input = input.trim();
    if input.starts_with('-') {
        return parse_duration(&input[1..]).and_then(|x| SystemTime::now().checked_sub(x));
    }
    let patterns = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%H:%M:%S",
        "%H:%M",
    ];

    for pattern in patterns {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(input, pattern) {
            return naive.and_local_timezone(Local).earliest().map(|x| x.into());
        }
    }
    None
}

pub fn parse_duration(input: &str) -> Option<Duration> {
    dbg!(input);
    let input = input.trim().to_lowercase();

    let index = input.find(|x: char| !x.is_ascii_digit() && x != '.')?;

    let (value, unit) = input.split_at(index);

    let value = value.parse::<f64>().ok()?;

    match unit {
        "ms" | "millisecond" | "milliseconds" | "millis" | "milissegundo" | "milissegundos" => {
            Duration::try_from_secs_f64(value * 0.001).ok()
        }
        "s" | "second" | "seconds" | "segundo" | "segundos" => {
            Duration::try_from_secs_f64(value).ok()
        }
        "m" | "minute" | "minutes" | "minuto" | "minutos" => {
            Duration::try_from_secs_f64(value * 60.0).ok()
        }
        "h" | "hour" | "hours" | "hora" | "horas" => {
            Duration::try_from_secs_f64(value * 60.0 * 60.0).ok()
        }
        "d" | "day" | "days" | "dia" | "dias" => {
            Duration::try_from_secs_f64(value * 60.0 * 60.0 * 24.0).ok()
        }
        "w" | "week" | "weeks" | "semana" | "semanas" => {
            Duration::try_from_secs_f64(value * 60.0 * 60.0 * 24.0 * 7.0).ok()
        }
        _ => None,
    }
}

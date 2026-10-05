//! Reject duplicates before decoding nested upstream Value fields.
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Value};
use std::fmt;
struct Strict(Value);
impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Strict;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("integer-only JSON without duplicate keys")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Strict, E> {
                Ok(Strict(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                let mut v = Vec::new();
                while let Some(x) = a.next_element::<Strict>()? {
                    v.push(x.0);
                }
                Ok(Strict(v.into()))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                let mut v = Map::new();
                while let Some(k) = a.next_key::<String>()? {
                    if v.contains_key(&k) {
                        return Err(de::Error::custom("Duplicate JSON key"));
                    }
                    v.insert(k, a.next_value::<Strict>()?.0);
                }
                Ok(Strict(v.into()))
            }
        }
        d.deserialize_any(V)
    }
}
pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, serde_json::Error> {
    let v: Strict = serde_json::from_slice(bytes)?;
    serde_json::from_value(v.0)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_ambiguous_json() {
        for s in [
            r#"{"a":{"x":1,"x":2}}"#,
            r#"{"x":1.0}"#,
            r#"{"x":NaN}"#,
            r#"{} {}"#,
        ] {
            assert!(decode::<Value>(s.as_bytes()).is_err());
        }
        assert_eq!(
            decode::<Value>(b"9223372036854775807").unwrap(),
            Value::from(i64::MAX)
        );
    }
}

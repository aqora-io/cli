use bytes::Bytes;
use serde::de::{DeserializeOwned, Error};
use serde_json::de::Deserializer;

use std::fmt;

use crate::process::{ByteProcessResult, ByteProcessor, ProcessReadStream};

use super::JsonFileType;

pub type JsonReadStream<R, T> = ProcessReadStream<R, JsonProcessor<T>>;

#[derive(Debug)]
struct Consumer<'a> {
    consumed: usize,
    bytes: &'a [u8],
}

impl<'a> Consumer<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, consumed: 0 }
    }
    fn consume(&mut self, n: usize) {
        self.consumed += n;
        self.bytes = self.bytes.split_at(n).1
    }
    fn consume_whitespace(&mut self) {
        const CHARS: [u8; 4] = [b' ', b'\t', b'\r', b'\n'];
        let index = self
            .bytes
            .iter()
            .position(|&c| !CHARS.contains(&c))
            .unwrap_or(self.bytes.len());
        self.consume(index);
    }
    fn peek(&self) -> Option<u8> {
        if self.bytes.is_empty() {
            None
        } else {
            Some(self.bytes[0])
        }
    }
    fn consumed(&self) -> usize {
        self.consumed
    }
    fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

#[derive(Debug)]
enum JsonProcessorState {
    Open,
    FirstKey,
    Key,
    Colon,
    FirstItem,
    Item,
    NextItem,
    Closed,
}

pub struct JsonProcessor<T> {
    file_type: JsonFileType,
    state: JsonProcessorState,
    is_key_value: bool,
    last_key: Option<String>,
    ty: std::marker::PhantomData<T>,
}

impl<T> JsonProcessor<T> {
    pub fn new(file_type: JsonFileType) -> Self {
        Self {
            file_type,
            is_key_value: false,
            last_key: None,
            state: match file_type {
                JsonFileType::Json => JsonProcessorState::Open,
                JsonFileType::Jsonl => JsonProcessorState::Item,
            },
            ty: std::marker::PhantomData,
        }
    }
}

impl<T> Default for JsonProcessor<T> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<T> fmt::Debug for JsonProcessor<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonProcessor")
            .field("ty", &self.ty)
            .field("state", &self.state)
            .finish()
    }
}

impl<T> ByteProcessor for JsonProcessor<T>
where
    T: DeserializeOwned,
{
    type Item = (Option<String>, T);
    type Error = serde_json::Error;
    fn process(
        &mut self,
        bytes: Bytes,
        _is_eof: bool,
    ) -> ByteProcessResult<Self::Item, Self::Error> {
        use JsonProcessorState::*;
        let mut consumer = Consumer::new(&bytes);
        loop {
            consumer.consume_whitespace();
            match self.state {
                Open => match consumer.peek() {
                    None => {
                        return ByteProcessResult::Done(consumer.consumed());
                    }
                    Some(b'[') => {
                        consumer.consume(1);
                        self.state = FirstItem;
                    }
                    Some(b'{') => {
                        consumer.consume(1);
                        self.is_key_value = true;
                        self.state = FirstKey;
                    }
                    Some(_) => {
                        return ByteProcessResult::Err(serde_json::Error::custom(
                            "Expected '[' at the start of JSON array",
                        ));
                    }
                },
                FirstItem => match consumer.peek() {
                    None => {
                        return ByteProcessResult::NotReady(consumer.consumed());
                    }
                    Some(b']') => {
                        consumer.consume(1);
                        self.state = Closed;
                    }
                    Some(_) => {
                        if self.is_key_value {
                            self.state = Key;
                        } else {
                            self.state = Item;
                        }
                    }
                },
                FirstKey => match consumer.peek() {
                    None => {
                        return ByteProcessResult::NotReady(consumer.consumed());
                    }
                    Some(b'}') => {
                        consumer.consume(1);
                        self.state = Closed;
                    }
                    Some(_) => {
                        self.state = Key;
                    }
                },
                Key => {
                    let mut iter = Deserializer::from_slice(consumer.bytes()).into_iter::<String>();
                    match iter.next().transpose() {
                        Ok(Some(result)) => {
                            self.state = Colon;
                            self.last_key = Some(result);
                            consumer.consume(iter.byte_offset());
                        }
                        Ok(None) => {
                            return ByteProcessResult::NotReady(consumer.consumed());
                        }
                        Err(err) => {
                            if err.is_eof() {
                                return ByteProcessResult::NotReady(consumer.consumed());
                            } else {
                                return ByteProcessResult::Err(err);
                            }
                        }
                    }
                }
                Colon => match consumer.peek() {
                    None => {
                        return ByteProcessResult::NotReady(consumer.consumed());
                    }
                    Some(b':') => {
                        consumer.consume(1);
                        self.state = Item;
                    }
                    Some(_) => {
                        return ByteProcessResult::Err(serde_json::Error::custom(
                            "Expected ':' after a key",
                        ));
                    }
                },
                Item => {
                    let mut iter = Deserializer::from_slice(consumer.bytes()).into_iter::<T>();
                    match iter.next().transpose() {
                        Ok(Some(result)) => {
                            match self.file_type {
                                JsonFileType::Json => {
                                    self.state = NextItem;
                                }
                                JsonFileType::Jsonl => {
                                    self.state = Item;
                                }
                            }
                            return ByteProcessResult::Ok((
                                consumer.consumed(),
                                consumer.consumed() + iter.byte_offset(),
                                (self.last_key.take(), result),
                            ));
                        }
                        Ok(None) => match self.file_type {
                            JsonFileType::Json => {
                                return ByteProcessResult::NotReady(
                                    consumer.consumed() + iter.byte_offset(),
                                );
                            }
                            JsonFileType::Jsonl => {
                                return ByteProcessResult::Done(
                                    consumer.consumed() + iter.byte_offset(),
                                );
                            }
                        },
                        Err(err) => {
                            if err.is_eof() {
                                return ByteProcessResult::NotReady(consumer.consumed());
                            } else {
                                return ByteProcessResult::Err(err);
                            }
                        }
                    }
                }
                NextItem => match consumer.peek() {
                    None => {
                        return ByteProcessResult::NotReady(consumer.consumed());
                    }
                    Some(b',') => {
                        consumer.consume(1);
                        if self.is_key_value {
                            self.state = Key;
                        } else {
                            self.state = Item;
                        }
                    }
                    Some(b']') => {
                        consumer.consume(1);
                        if self.is_key_value {
                            return ByteProcessResult::Err(serde_json::Error::custom(
                                "Expected ',' or '}' after an array item",
                            ));
                        } else {
                            self.state = Closed;
                        }
                    }
                    Some(b'}') => {
                        consumer.consume(1);
                        if self.is_key_value {
                            self.state = Closed;
                        } else {
                            return ByteProcessResult::Err(serde_json::Error::custom(
                                "Expected ',' or ']' after an array item",
                            ));
                        }
                    }
                    Some(_) => {
                        if self.is_key_value {
                            return ByteProcessResult::Err(serde_json::Error::custom(
                                "Expected ',' or '}' after an array item",
                            ));
                        } else {
                            return ByteProcessResult::Err(serde_json::Error::custom(
                                "Expected ',' or ']' after an array item",
                            ));
                        }
                    }
                },
                Closed => match consumer.peek() {
                    None => {
                        return ByteProcessResult::Done(consumer.consumed());
                    }
                    Some(_) => {
                        return ByteProcessResult::Err(serde_json::Error::custom(
                            "Unexpected character after close",
                        ));
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::stream::TryStreamExt;

    const EXAMPLE_JSON1: &str = r#"[
        ["Name", "Session", "Score", "Completed"],
        ["Gilbert", "2013", 24, true],
        ["Alexa", "2013", 29, true],
        ["May", "2012B", 14, false],
        ["Deloise", "2012A", 19, true]
    ]"#;
    const EXAMPLE_JSON2: &str = r#"[
        {"name": "Gilbert", "session": "2013", "score": 24, "completed": true},
        {"name": "Alexa", "session": "2013", "score": 29, "completed": true},
        {"name": "May", "session": "2012B", "score": 14, "completed": false},
        {"name": "Deloise", "session": "2012A", "score": 19, "completed": true}
    ]"#;
    const EXAMPLE_JSON3: &str = r#"[
        {"name": "Gilbert", "wins": [["straight", "7♣"], ["one pair", "10♥"]]},
        {"name": "Alexa", "wins": [["two pair", "4♠"], ["two pair", "9♠"]]},
        {"name": "May", "wins": []},
        {"name": "Deloise", "wins": [["three of a kind", "5♣"]]}
    ]"#;

    const BAD_JSON1: &str = r#"[
        ["Name", "Session", "Score", "Completed"],
        ["Gilbert", "2013", 24, true],
        ["Alexa", "2013", 29, true],
        ["May", "2012B", 14, false],
        ["Deloise", "2012A", 19, BAD_TOKEN]
    ]"#;
    const BAD_JSON2: &str = r#"[
        {"name": "Gilbert", "session": "2013", "score": 24, "completed": true},
        {"name": "Alexa", "session": "2013", "score": 29, "completed": true},
        {"name": "May", "session": "2012B", "score": 14, "completed": false},
        {"name": "Deloise", "session": "2012A", "score": 19, "completed": BAD_TOKEN}
    ]"#;

    const INCOMPLETE_JSON1: &str = r#"[
        ["Name", "Session", "Score", "Completed"],
        ["Gilbert", "2013", 24, true],
        ["Alexa", "2013", 29, true],
        ["May", "2012B", 14, false],
        ["Deloise", "2012A", 19"#;
    const INCOMPLETE_JSON2: &str = r#"[
        {"name": "Gilbert", "session": "2013", "score": 24, "completed": true},
        {"name": "Alexa", "session": "2013", "score": 29, "completed": true},
        {"name": "May", "session": "2012B", "score": 14, "completed": false},
        {"name": "Deloise", "session": "2012A", "score": 19"#;

    const EXAMPLE_JSONL1: &str = r#"["Name", "Session", "Score", "Completed"]
["Gilbert", "2013", 24, true]
["Alexa", "2013", 29, true]
["May", "2012B", 14, false]
["Deloise", "2012A", 19, true]"#;
    const EXAMPLE_JSONL2: &str = r#"{"name": "Gilbert", "session": "2013", "score": 24, "completed": true}
{"name": "Alexa", "session": "2013", "score": 29, "completed": true}
{"name": "May", "session": "2012B", "score": 14, "completed": false}
{"name": "Deloise", "session": "2012A", "score": 19, "completed": true}"#;
    const EXAMPLE_JSONL3: &str = r#"{"name": "Gilbert", "wins": [["straight", "7♣"], ["one pair", "10♥"]]}
{"name": "Alexa", "wins": [["two pair", "4♠"], ["two pair", "9♠"]]}
{"name": "May", "wins": []}
{"name": "Deloise", "wins": [["three of a kind", "5♣"]]}"#;

    const BAD_JSONL1: &str = r#"["Name", "Session", "Score", "Completed"]
["Gilbert", "2013", 24, true]
["Alexa", "2013", 29, true]
["May", "2012B", 14, false]
["Deloise", "2012A", 19, BAD_TOKEN]"#;
    const BAD_JSONL2: &str = r#"{"name": "Gilbert", "session": "2013", "score": 24, "completed": true}
{"name": "Alexa", "session": "2013", "score": 29, "completed": true}
{"name": "May", "session": "2012B", "score": 14, "completed": false}
{"name": "Deloise", "session": "2012A", "score": 19, "completed": BAD_TOKEN}"#;

    const INCOMPLETE_JSONL1: &str = r#"["Name", "Session", "Score", "Completed"]
["Gilbert", "2013", 24, true]
["Alexa", "2013", 29, true]
["May", "2012B", 14, false]
["Deloise", "2012A", "#;
    const INCOMPLETE_JSONL2: &str = r#"{"name": "Gilbert", "session": "2013", "score": 24, "completed": true}
{"name": "Alexa", "session": "2013", "score": 29, "completed": true}
{"name": "May", "session": "2012B", "score": 14, "completed": false}
{"name": "Deloise", "session": "2012A", "#;

    #[tokio::test]
    async fn test_jsonl() {
        for (json_str, jsonl_str) in [
            (EXAMPLE_JSON1, EXAMPLE_JSONL1),
            (EXAMPLE_JSON2, EXAMPLE_JSONL2),
            (EXAMPLE_JSON3, EXAMPLE_JSONL3),
        ] {
            let example_values = serde_json::from_str::<serde_json::Value>(json_str)
                .unwrap()
                .as_array()
                .cloned()
                .unwrap();
            let stream_values = JsonReadStream::new(
                jsonl_str.as_bytes(),
                JsonProcessor::new(JsonFileType::Jsonl),
            )
            .map_ok(|item| item.item)
            .map_ok(|(_, value)| value)
            .try_collect::<Vec<serde_json::Value>>()
            .await
            .unwrap();
            assert_eq!(example_values, stream_values);
        }
    }

    #[tokio::test]
    async fn test_bad_jsonl() {
        for (json_str, jsonl_str) in [(EXAMPLE_JSON1, BAD_JSONL1), (EXAMPLE_JSON2, BAD_JSONL2)] {
            let example_values = serde_json::from_str::<serde_json::Value>(json_str)
                .unwrap()
                .as_array()
                .cloned()
                .unwrap();
            let mut stream = JsonReadStream::<_, serde_json::Value>::new(
                jsonl_str.as_bytes(),
                JsonProcessor::new(JsonFileType::Jsonl),
            )
            .map_ok(|item| item.item)
            .map_ok(|(_, value)| value);
            let good_values = example_values.len() - 1;
            let mut example_iter = example_values.into_iter();
            for _ in 0..good_values {
                assert_eq!(stream.try_next().await.unwrap(), example_iter.next());
            }
            assert!(stream.try_next().await.is_err());
        }
    }

    #[tokio::test]
    async fn test_incomplete_jsonl() {
        for (json_str, jsonl_str) in [
            (EXAMPLE_JSON1, INCOMPLETE_JSONL1),
            (EXAMPLE_JSON2, INCOMPLETE_JSONL2),
        ] {
            let example_values = serde_json::from_str::<serde_json::Value>(json_str)
                .unwrap()
                .as_array()
                .cloned()
                .unwrap();
            let mut stream = JsonReadStream::<_, serde_json::Value>::new(
                jsonl_str.as_bytes(),
                JsonProcessor::new(JsonFileType::Jsonl),
            )
            .map_ok(|item| item.item)
            .map_ok(|(_, value)| value);
            let good_values = example_values.len() - 1;
            let mut example_iter = example_values.into_iter();
            for _ in 0..good_values {
                assert_eq!(stream.try_next().await.unwrap(), example_iter.next());
            }
            assert!(stream.try_next().await.is_err());
        }
    }

    #[tokio::test]
    async fn test_json() {
        for example_str in [EXAMPLE_JSON1, EXAMPLE_JSON2, EXAMPLE_JSON3] {
            let example_values = serde_json::from_str::<serde_json::Value>(example_str)
                .unwrap()
                .as_array()
                .cloned()
                .unwrap();
            let stream_values = JsonReadStream::new_default(example_str.as_bytes())
                .map_ok(|item| item.item)
                .map_ok(|(_, value)| value)
                .try_collect::<Vec<serde_json::Value>>()
                .await
                .unwrap();
            assert_eq!(example_values, stream_values);
        }
    }

    #[tokio::test]
    async fn test_bad_json() {
        for (json_str, jsonl_str) in [(EXAMPLE_JSON1, BAD_JSON1), (EXAMPLE_JSON2, BAD_JSON2)] {
            let example_values = serde_json::from_str::<serde_json::Value>(json_str)
                .unwrap()
                .as_array()
                .cloned()
                .unwrap();
            let mut stream =
                JsonReadStream::<_, serde_json::Value>::new_default(jsonl_str.as_bytes())
                    .map_ok(|item| item.item)
                    .map_ok(|(_, value)| value);
            let good_values = example_values.len() - 1;
            let mut example_iter = example_values.into_iter();
            for _ in 0..good_values {
                assert_eq!(stream.try_next().await.unwrap(), example_iter.next());
            }
            assert!(stream.try_next().await.is_err());
        }
    }

    #[tokio::test]
    async fn test_incomplete_json() {
        for (json_str, jsonl_str) in [
            (EXAMPLE_JSON1, INCOMPLETE_JSON1),
            (EXAMPLE_JSON2, INCOMPLETE_JSON2),
        ] {
            let example_values = serde_json::from_str::<serde_json::Value>(json_str)
                .unwrap()
                .as_array()
                .cloned()
                .unwrap();
            let mut stream =
                JsonReadStream::<_, serde_json::Value>::new_default(jsonl_str.as_bytes())
                    .map_ok(|item| item.item)
                    .map_ok(|(_, value)| value);
            let good_values = example_values.len() - 1;
            let mut example_iter = example_values.into_iter();
            for _ in 0..good_values {
                assert_eq!(stream.try_next().await.unwrap(), example_iter.next());
            }
            assert!(stream.try_next().await.is_err());
        }
    }
}

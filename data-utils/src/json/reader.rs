use serde::de::{DeserializeOwned, Error};
use serde_json::de::Deserializer;

use std::fmt;

use crate::utils::{ByteProcessResult, ByteProcessor, ProcessReadStream};

pub type JsonlReadStream<R, T> = ProcessReadStream<R, JsonlProcessor<T>>;
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

pub struct JsonlProcessor<T> {
    ty: std::marker::PhantomData<T>,
}

impl<T> Default for JsonlProcessor<T> {
    fn default() -> Self {
        Self {
            ty: std::marker::PhantomData,
        }
    }
}

impl<T> fmt::Debug for JsonlProcessor<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonlProcessor")
            .field("ty", &self.ty)
            .finish()
    }
}

impl<T> ByteProcessor for JsonlProcessor<T>
where
    T: DeserializeOwned,
{
    type Item = T;
    type Error = serde_json::Error;
    fn process(&mut self, bytes: &[u8]) -> ByteProcessResult<Self::Item, Self::Error> {
        let mut consumer = Consumer::new(bytes);
        consumer.consume_whitespace();
        let mut iter = Deserializer::from_slice(consumer.bytes()).into_iter::<T>();
        match iter.next().transpose() {
            Ok(Some(result)) => {
                ByteProcessResult::Ok((iter.byte_offset() + consumer.consumed(), result))
            }
            Ok(None) => ByteProcessResult::Done(iter.byte_offset() + consumer.consumed()),
            Err(err) => {
                if err.is_eof() {
                    ByteProcessResult::NotReady(consumer.consumed())
                } else {
                    ByteProcessResult::Err(err)
                }
            }
        }
    }
}

#[derive(Debug)]
enum JsonProcessorState {
    Open,
    FirstItem,
    Item,
    NextItem,
    Closed,
}

pub struct JsonProcessor<T> {
    ty: std::marker::PhantomData<T>,
    state: JsonProcessorState,
}

impl<T> Default for JsonProcessor<T> {
    fn default() -> Self {
        Self {
            ty: std::marker::PhantomData,
            state: JsonProcessorState::Open,
        }
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
    type Item = T;
    type Error = serde_json::Error;
    fn process(&mut self, bytes: &[u8]) -> ByteProcessResult<Self::Item, Self::Error> {
        use JsonProcessorState::*;
        let mut consumer = Consumer::new(bytes);
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
                        self.state = Item;
                    }
                },
                Item => {
                    let mut iter = Deserializer::from_slice(consumer.bytes()).into_iter::<T>();
                    match iter.next().transpose() {
                        Ok(Some(result)) => {
                            self.state = NextItem;
                            return ByteProcessResult::Ok((
                                iter.byte_offset() + consumer.consumed(),
                                result,
                            ));
                        }
                        Ok(None) => {
                            self.state = NextItem;
                            return ByteProcessResult::Done(
                                iter.byte_offset() + consumer.consumed(),
                            );
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
                NextItem => match consumer.peek() {
                    None => {
                        return ByteProcessResult::NotReady(consumer.consumed());
                    }
                    Some(b',') => {
                        consumer.consume(1);
                        self.state = Item;
                    }
                    Some(b']') => {
                        consumer.consume(1);
                        self.state = Closed;
                    }
                    Some(_) => {
                        return ByteProcessResult::Err(serde_json::Error::custom(
                            "Expected ',' or ']' after an array item",
                        ));
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
            let stream_values = JsonlReadStream::new_default(jsonl_str.as_bytes())
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
            let mut stream =
                JsonlReadStream::<_, serde_json::Value>::new_default(jsonl_str.as_bytes());
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
            let mut stream =
                JsonlReadStream::<_, serde_json::Value>::new_default(jsonl_str.as_bytes());
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
                JsonReadStream::<_, serde_json::Value>::new_default(jsonl_str.as_bytes());
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
                JsonReadStream::<_, serde_json::Value>::new_default(jsonl_str.as_bytes());
            let good_values = example_values.len() - 1;
            let mut example_iter = example_values.into_iter();
            for _ in 0..good_values {
                assert_eq!(stream.try_next().await.unwrap(), example_iter.next());
            }
            assert!(stream.try_next().await.is_err());
        }
    }
}

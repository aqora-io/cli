use base64::prelude::*;
use std::{fmt, str::FromStr};
use uuid::Uuid;

const BASE32_ALPHABET: base32::Alphabet = base32::Alphabet::Rfc4648 { padding: false };

#[derive(Debug, Clone, Copy)]
pub enum NodeType {
    User,
    Competition,
    Organization,
    ProjectVersionFile,
}

impl FromStr for NodeType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "User" => Ok(NodeType::User),
            "Competition" => Ok(NodeType::Competition),
            "Organization" => Ok(NodeType::Organization),
            "ProjectVersionFile" => Ok(NodeType::ProjectVersionFile),
            _ => Err(format!("Unknown node kind: {}", s)),
        }
    }
}

impl fmt::Display for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeType::User => write!(f, "User"),
            NodeType::Competition => write!(f, "Competition"),
            NodeType::Organization => write!(f, "Organization"),
            NodeType::ProjectVersionFile => write!(f, "ProjectVersionFile"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Id {
    pub id: Uuid,
    pub ty: NodeType,
}

impl Id {
    pub fn parse_node_id(id: impl AsRef<str>) -> Result<Id, Box<dyn std::error::Error>> {
        let bytes = BASE64_URL_SAFE_NO_PAD.decode(id.as_ref())?;
        let len = bytes.len();
        if bytes.len() < 1 + 16 {
            return Err("Invalid id length".into());
        }
        if bytes[0] != 0 {
            return Err("Invalid id version".into());
        }

        let ty = std::str::from_utf8(&bytes[1..len - 16])?.parse()?;
        let id = Uuid::from_slice(&bytes[len - 16..])?;
        Ok(Id { id, ty })
    }

    pub fn to_node_id(self) -> String {
        let mut bytes = vec![0]; // version
        bytes.extend_from_slice(self.ty.to_string().as_bytes());
        bytes.extend_from_slice(self.id.as_bytes());
        BASE64_URL_SAFE_NO_PAD.encode(&bytes)
    }

    pub fn to_package_id(self) -> String {
        base32::encode(BASE32_ALPHABET, &self.id.into_bytes()).to_lowercase()
    }
}

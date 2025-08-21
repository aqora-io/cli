#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expr {
    Tag(String),
    Named(String),
    Either(Vec<Group>),
    Star,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Group {
    pub exprs: Vec<Expr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Segment {
    Match(Group),
    DoubleStar,
}

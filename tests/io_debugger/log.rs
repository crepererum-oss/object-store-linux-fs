use indexmap::{IndexMap, IndexSet};
use rustix::io::Errno;

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Flag {
    Know(&'static str),
    Unknown(u64),
}

impl std::fmt::Display for Flag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Know(name) => write!(f, "{name}"),
            Self::Unknown(x) => write!(f, "{x:#x}"),
        }
    }
}

#[derive(Debug)]
pub enum Value {
    Ellipsis,
    Flags(IndexSet<Flag>),
    ID(&'static str, u64),
    String(String),
    Unit,
}

impl Value {
    pub fn flags<F, B>(f: F) -> Self
    where
        F: bitflags::Flags<Bits = B>,
        B: Into<u64>,
    {
        let unknown: u64 = f.unknown_bits().into();
        Self::Flags(
            f.iter_names()
                .map(|(name, _)| Flag::Know(name))
                .chain((unknown > 0).then_some(Flag::Unknown(unknown)))
                .collect(),
        )
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ellipsis => write!(f, "..."),
            Self::Flags(flags) => {
                if flags.is_empty() {
                    write!(f, "<empty>")
                } else {
                    for (idx, flag) in flags.iter().enumerate() {
                        if idx > 0 {
                            write!(f, "|")?;
                        }
                        write!(f, "{flag}")?;
                    }
                    Ok(())
                }
            }
            Self::ID(what, id) => write!(f, "{what}#{id}"),
            Self::String(s) => write!(f, "\"{s}\""),
            Self::Unit => write!(f, "()"),
        }
    }
}

#[derive(Debug)]
pub struct LogEntry {
    pub task_id: u64,
    pub block_id: u64,
    pub fun: &'static str,
    pub args: IndexMap<&'static str, Value>,
    pub res: Result<Value, Errno>,
}

impl std::fmt::Display for LogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            task_id,
            block_id,
            fun,
            args,
            res,
        } = self;

        write!(f, "T{task_id} B{block_id} {fun}(")?;

        for (idx, (name, val)) in args.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{name}: {val}")?;
        }

        write!(f, ") -> ")?;

        match res {
            Ok(val) => write!(f, "Ok({val})"),
            Err(e) => write!(f, "Err({e})"),
        }
    }
}

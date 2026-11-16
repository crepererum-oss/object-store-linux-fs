use indexmap::IndexMap;
use rustix::io::Errno;

#[derive(Debug)]
pub enum Value {
    Ellipsis,
    String(String),
    Unit,
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ellipsis => write!(f, "..."),
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

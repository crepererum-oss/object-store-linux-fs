use std::io::ErrorKind;

use crate::constants::STORE_NAME;

pub(crate) trait IOErrorExt {
    type Out;

    fn into_get_err<L>(self, location: L) -> Self::Out
    where
        L: ToString;
    fn into_list_err(self) -> Option<Self::Out>;
    fn into_put_err<L>(self, location: L) -> Self::Out
    where
        L: ToString;
    fn into_update_err<L>(self, location: L) -> Self::Out
    where
        L: ToString;
    fn into_other_err(self) -> Self::Out;
}

impl IOErrorExt for rustix::io::Errno {
    type Out = object_store::Error;

    fn into_get_err<L>(self, location: L) -> Self::Out
    where
        L: ToString,
    {
        std::io::Error::from(self).into_get_err(location)
    }

    fn into_list_err(self) -> Option<Self::Out> {
        std::io::Error::from(self).into_list_err()
    }

    fn into_put_err<L>(self, location: L) -> Self::Out
    where
        L: ToString,
    {
        std::io::Error::from(self).into_put_err(location)
    }

    fn into_update_err<L>(self, location: L) -> Self::Out
    where
        L: ToString,
    {
        std::io::Error::from(self).into_update_err(location)
    }

    fn into_other_err(self) -> Self::Out {
        std::io::Error::from(self).into_other_err()
    }
}

impl IOErrorExt for std::io::Error {
    type Out = object_store::Error;

    fn into_get_err<L>(self, location: L) -> Self::Out
    where
        L: ToString,
    {
        match self.kind() {
            ErrorKind::NotFound => object_store::Error::NotFound {
                path: location.to_string(),
                source: Box::new(self),
            },
            _ => object_store::Error::Generic {
                store: STORE_NAME,
                source: Box::new(self),
            },
        }
    }

    fn into_list_err(self) -> Option<Self::Out> {
        match self.kind() {
            ErrorKind::NotFound | ErrorKind::NotADirectory => {
                // state changed under us, ignore it
                None
            }
            _ => Some(object_store::Error::Generic {
                store: STORE_NAME,
                source: Box::new(self),
            }),
        }
    }

    fn into_put_err<L>(self, location: L) -> Self::Out
    where
        L: ToString,
    {
        match self.kind() {
            ErrorKind::AlreadyExists => object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: Box::new(self),
            },
            _ => object_store::Error::Generic {
                store: STORE_NAME,
                source: Box::new(self),
            },
        }
    }

    fn into_update_err<L>(self, location: L) -> Self::Out
    where
        L: ToString,
    {
        match self.kind() {
            ErrorKind::NotFound => object_store::Error::Precondition {
                path: location.to_string(),
                source: Box::new(self),
            },
            _ => object_store::Error::Generic {
                store: STORE_NAME,
                source: Box::new(self),
            },
        }
    }

    fn into_other_err(self) -> Self::Out {
        object_store::Error::Generic {
            store: STORE_NAME,
            source: Box::new(self),
        }
    }
}

impl<T, E> IOErrorExt for Result<T, E>
where
    E: IOErrorExt,
{
    type Out = Result<T, E::Out>;

    fn into_get_err<L>(self, location: L) -> Self::Out
    where
        L: ToString,
    {
        self.map_err(|e| e.into_get_err(location))
    }

    fn into_list_err(self) -> Option<Self::Out> {
        match self {
            Ok(x) => Some(Ok(x)),
            Err(e) => e.into_list_err().map(Err),
        }
    }

    fn into_put_err<L>(self, location: L) -> Self::Out
    where
        L: ToString,
    {
        self.map_err(|e| e.into_put_err(location))
    }

    fn into_update_err<L>(self, location: L) -> Self::Out
    where
        L: ToString,
    {
        self.map_err(|e| e.into_update_err(location))
    }

    fn into_other_err(self) -> Self::Out {
        self.map_err(|e| e.into_other_err())
    }
}

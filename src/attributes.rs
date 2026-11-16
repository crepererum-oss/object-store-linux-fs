use std::{mem::MaybeUninit, os::fd::OwnedFd};

use object_store::{Attribute, Attributes, Error, Result};
use rustix::io::Errno;

use crate::{constants::STORE_NAME, io::IOBlock};

pub(crate) fn attr_to_xattr(attr: &Attribute) -> Result<String> {
    let name = match attr {
        Attribute::ContentDisposition => "content-disposition",
        Attribute::ContentEncoding => "content-encoding",
        Attribute::ContentLanguage => "content-language",
        Attribute::ContentType => "content-type",
        Attribute::CacheControl => "cache-control",
        Attribute::StorageClass => {
            return Err(Error::NotImplemented {
                operation: "StorageClass attribute".to_owned(),
                implementer: STORE_NAME.to_owned(),
            });
        }
        Attribute::Metadata(s) => s,
        other => {
            return Err(Error::NotImplemented {
                operation: format!("`{other:?}` attribute"),
                implementer: STORE_NAME.to_owned(),
            });
        }
    };

    Ok(format!("user.{name}"))
}

fn xattr_to_attr(attr: &str) -> Option<Attribute> {
    let attr = attr.strip_prefix("user.")?;

    match attr {
        "content-disposition" => Some(Attribute::ContentDisposition),
        "content-encoding" => Some(Attribute::ContentEncoding),
        "content-language" => Some(Attribute::ContentLanguage),
        "content-type" => Some(Attribute::ContentType),
        "cache-control" => Some(Attribute::CacheControl),
        "storage-class" => {
            // ignore
            None
        }
        other => Some(Attribute::Metadata(other.to_owned().into())),
    }
}

pub(crate) fn read_attrs_from_fd(io: &mut dyn IOBlock, fd: &OwnedFd) -> Result<Attributes, Errno> {
    let names_len = io.flistxattr_query(fd)?;
    let mut buffer_names = vec![MaybeUninit::uninit(); names_len];
    let (init, _uinit) = io.flistxattr(fd, &mut buffer_names)?;
    let names = std::str::from_utf8(init).map_err(|_| Errno::INVAL)?;

    let mut attributes = Attributes::with_capacity(names.chars().filter(|c| *c == '\0').count());

    let mut buffer_values = vec![MaybeUninit::uninit(); 16];
    for name in names.split('\0') {
        let Some(attr) = xattr_to_attr(name) else {
            continue;
        };

        let attr_len = io.fgetxattr_query(fd, name)?;
        if buffer_values.len() < attr_len {
            buffer_values.resize(attr_len, MaybeUninit::uninit());
        }
        let (init, _uninit) = io.fgetxattr(fd, name, &mut buffer_values)?;
        let value = String::from_utf8(init.to_vec()).map_err(|_| Errno::INVAL)?;
        attributes.insert(attr, value.into());
    }

    Ok(attributes)
}

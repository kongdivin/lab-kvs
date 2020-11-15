use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::fs::{create_dir_all, read_dir, remove_file, File};
use std::io::{prelude::*, BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};

const LOG_FILE_EXT: &str = "log";
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

#[derive(Debug)]
pub enum KvsError {
    Unexpected,
    KeyNotFound,
    InvalidPath,
    IoError(std::io::Error),
    SerdeJsonError(serde_json::Error),
}

impl Display for KvsError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "This is error")
    }
}

impl From<std::io::Error> for KvsError {
    fn from(io_err: std::io::Error) -> Self {
        KvsError::IoError(io_err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(serde_err: serde_json::Error) -> Self {
        KvsError::SerdeJsonError(serde_err)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;

type KvsKey = String;

type KvsValue = String;

type Gen = u64;

#[derive(Debug)]
struct IndexMeta {
    gen: Gen,
    pos: u64,
}

type KvsReaders = HashMap<Gen, BufReader<File>>;

type KvsIndex = HashMap<KvsKey, IndexMeta>;

#[derive(Serialize, Deserialize, Debug)]
pub enum KvsCommand {
    Set(KvsKey, KvsValue),
    Remove(KvsKey),
}

#[derive(Debug)]
pub struct KvStore {
    index: KvsIndex,
    readers: KvsReaders,
    writer: BufWriter<File>,
    gen: Gen,
    cursor: u64,
    path: PathBuf,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();

        create_dir_all(&path)?;

        if !path.is_dir() {
            return Err(KvsError::InvalidPath);
        }

        let mut gen_list = gen_list(&path)?;
        let mut readers = readers(&path, &gen_list)?;
        gen_list.sort();
        let gen = match gen_list.last() {
            Some(last) => last + 1,
            None => 0,
        };
        let writer = prepare_new_gen(&path, gen, &mut readers)?;

        Ok(KvStore {
            index: index(&path, &gen_list)?,
            readers,
            writer,
            cursor: 0,
            gen,
            path,
        })
    }

    pub fn get(&mut self, key: KvsKey) -> Result<Option<KvsValue>> {
        match self.index.get(&key) {
            Some(IndexMeta { gen, pos }) => match self.readers.get_mut(&gen) {
                Some(reader) => {
                    reader.seek(SeekFrom::Start(*pos))?;
                    let mut cmd = String::new();
                    reader.read_line(&mut cmd)?;
                    match serde_json::from_str(&cmd)? {
                        KvsCommand::Set(_, val) => Ok(Some(val)),
                        _ => panic!(),
                    }
                }
                None => panic!(),
            },
            None => Ok(None),
        }
    }

    pub fn set(&mut self, key: KvsKey, value: KvsValue) -> Result<()> {
        let set_cmd = serde_json::to_string(&KvsCommand::Set(key.to_owned(), value))?;
        let idx_val = IndexMeta {
            gen: self.gen,
            pos: self.cursor,
        };

        self.log_cmd(set_cmd)?;
        self.index.insert(key, idx_val);

        if self.cursor > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let remove_cmd = serde_json::to_string(&KvsCommand::Remove(key.to_owned()))?;
            self.log_cmd(remove_cmd)?;
            self.index.remove(&key);

            if self.cursor > COMPACTION_THRESHOLD {
                self.compact()?;
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    fn log_cmd(&mut self, cmd: impl AsRef<str>) -> Result<()> {
        let cmd = format!("{}\n", cmd.as_ref());
        let bytes = cmd.as_bytes();
        let len = bytes.len() as u64;
        self.writer.write_all(bytes)?;
        self.writer.flush()?;
        self.cursor += len;
        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        self.gen += 2;
        self.cursor = 0;
        self.writer = prepare_new_gen(&self.path, self.gen, &mut self.readers)?;

        let compact_gen = self.gen - 1;
        let f = new_log(&self.path, compact_gen)?;
        let mut buf = BufWriter::new(f);
        let mut cursor: u64 = 0;

        self.readers
            .insert(compact_gen, reader(&self.path, compact_gen)?);

        for (_, IndexMeta { gen, pos }) in self.index.iter_mut() {
            match self.readers.get_mut(&gen) {
                Some(reader) => {
                    reader.seek(SeekFrom::Start(*pos))?;
                    let mut cmd = String::new();
                    reader.read_line(&mut cmd)?;
                    let cmd = cmd.as_bytes();
                    buf.write_all(cmd)?;
                    *gen = compact_gen;
                    *pos = cursor;
                    cursor += cmd.len() as u64;
                }
                None => panic!(),
            }
        }

        buf.flush()?;

        for gen in gen_list(&self.path)? {
            if gen < compact_gen {
                self.readers.remove(&gen);
                remove_file(log_path(&self.path, gen))?;
            }
        }

        Ok(())
    }
}

fn prepare_new_gen(path: &Path, new_gen: Gen, readers: &mut KvsReaders) -> Result<BufWriter<File>> {
    let log = new_log(path, new_gen)?;
    readers.insert(new_gen, reader(path, new_gen)?);

    Ok(BufWriter::new(log))
}

fn new_log(path: &Path, gen: Gen) -> Result<File> {
    let f = File::create(log_path(path, gen))?;
    Ok(f)
}

fn readers(path: &Path, gen_list: &[Gen]) -> Result<KvsReaders> {
    let mut readers: KvsReaders = HashMap::new();

    for gen in gen_list {
        readers.insert(*gen, reader(&path, *gen)?);
    }

    Ok(readers)
}

fn reader(path: &Path, gen: Gen) -> Result<BufReader<File>> {
    let f = File::open(log_path(path, gen))?;
    Ok(BufReader::new(f))
}

fn index(path: &Path, gen_list: &[Gen]) -> Result<HashMap<KvsKey, IndexMeta>> {
    let mut index: HashMap<KvsKey, IndexMeta> = HashMap::new();
    for gen in gen_list {
        let f = File::open(log_path(path, *gen))?;
        let mut buf = BufReader::new(f);
        let mut pos = 0;

        loop {
            let mut cmd = String::new();
            let n = buf.read_line(&mut cmd)?;

            if n == 0 {
                break;
            }

            match serde_json::from_str(&cmd)? {
                KvsCommand::Set(key, _) => {
                    index.insert(key, IndexMeta { gen: *gen, pos });
                }
                KvsCommand::Remove(key) => {
                    index.remove(&key);
                }
            }

            pos += n as u64;
        }
    }

    Ok(index)
}

fn log_path(path: &Path, gen: Gen) -> PathBuf {
    path.join(format!("{}.log", gen))
}

fn gen_list(path: &Path) -> Result<Vec<Gen>> {
    let gen_list: Vec<Gen> = read_dir(path)?
        .flat_map(|res| -> Result<PathBuf> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some(LOG_FILE_EXT.as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|file_name| file_name.trim_end_matches(format!(".{}", LOG_FILE_EXT).as_str()))
                .map(str::parse::<Gen>)
        })
        .flatten()
        .collect();

    Ok(gen_list)
}
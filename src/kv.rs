use std::collections::HashMap;
use failure::Fail;
use crate::{KvsError, Result};
use serde::{Deserialize, Serialize};
use std::fs::{OpenOptions, File};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write, BufWriter, self}; 
use std::path::{Path, PathBuf};

pub struct KvStore {
    readers: HashMap<String, BufReaderWithPos<File>>,
    log_path: PathBuf, 
    writer: BufWriterWithPos<File>,
}


struct CommandPos {
    pos: u64,
    len: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set {key: String, value: String},
    Remove {key: String},
}

impl KvStore {
    /// Create a new KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();  
        let log_path = path.join("kvs.log"); 
        let mut store = HashMap::new();
        let file = OpenOptions::new().create(true).read(true).write(true).open(&log_path)?;

        let reader = BufReader::new(&file);
        let mut pos = 0;
        for line in reader.lines() {
            let line = line?;
            let cmd: Command = serde_json::from_str(&line)?;
            let len = line.len() as u64 + 1;
            match cmd {
                Command::Set {key, value} => {
                    store.insert(key, CommandPos{pos, len});
                },
                Command::Remove { key } => {
                    store.remove(&key);
                }
            }
            pos += line.len() as u64 + 1;
        }
        return Ok(KvStore{store, log_path, writer: file})
    }

    /// Insert or update the given key-value pair.
    pub fn set(&mut self, key: String, value: String) -> Result<()>{

        let cmd = Command::Set {key: key.clone(), value};
        let serialized = serde_json::to_string(&cmd)?;

        let pos = self.writer.seek(SeekFrom::End(0))?;
        
        let mut file = OpenOptions::new()
            .create(true)       
            .append(true)   
            .open(&self.log_path)?;
        
        self.store.insert(key.clone(), CommandPos{pos, len: serialized.len() as u64});

        writeln!(file, "{}", serialized)?;

        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>>{

        let cmd_pos = match self.store.get(&key) {
            Some(pos) => pos,
            None => return Ok(None)
        };

        let mut reader = BufReader::new(File::open(&self.log_path)?);

        reader.seek(SeekFrom::Start(cmd_pos.pos));
        let mut buf = vec![0u8; cmd_pos.len as usize];

        reader.read_exact(&mut buf);
        
        let cmd: Command = serde_json::from_slice(&buf)?;

        match cmd {
            Command::Set {key, value} => return Ok(Some(value)),
            _ => return Ok(None),
        }
    }
    // get

    pub fn remove(&mut self, key: String) -> Result<()>{
        if !self.store.contains_key(&key){
            return Err(KvsError::KeyNotFound)
        }
        let cmd = Command::Remove {key: key.clone()};
        let serialized = serde_json::to_string(&cmd)?;

        let pos = self.writer.seek(SeekFrom::End(0))?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        
        self.store.insert(key.clone(), CommandPos{pos, len: serialized.len() as u64});

        writeln!(file, "{}", serialized)?;
        Ok(())
    }
    // remove
}


pub struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    pub fn new(mut inner: R) -> io::Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes_read = self.reader.read(buf)?;
        self.pos += bytes_read as u64;
        Ok(bytes_read)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

pub struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64
}


impl<W: Write + Seek> BufWriterWithPos<W> {
    pub fn new(mut inner: W) -> io::Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}


impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let bytes_written = self.writer.write(buf)?;
        self.pos += bytes_written as u64;
        Ok(bytes_written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}


impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

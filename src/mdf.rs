use crate::StreamWithData;
use derivative::Derivative;
use mdf::{PageHeader, PagePointer, PageProvider, RawPage, PAGE_SIZE};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::path::Path;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct MTFPageProvider<'a> {
    #[derivative(Debug = "ignore")]
    data: &'a [u8],
    #[derivative(Debug = "ignore")]
    index: MTFBackupIndex,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct IndexEntry {
    start: u32, // inclusive
    stop: u32,  // inclusive
    base: u32,
}

#[derive(Serialize, Deserialize)]
struct MTFBackupIndex {
    // First layer is one per file_id
    // that contains outer_level_entries entries per file_id
    // the index into that array is calculated by page_id / divisor
    // the innermost layer now contains a list of IndexEntries, that contain a
    // map of (start page id, stop page id) -> base actual index
    idx: Vec<Vec<Vec<IndexEntry>>>,
    outer_level_entries: usize,
    divisor: usize,
    max_page_ids: HashMap<u16, u32>,
}

impl MTFBackupIndex {
    // Shoot for approximately 1024 entries in the inner level
    // Assuming a average run length of O(100), this should work out nicely
    const DIVISOR: usize = 1024;

    fn cache_name(data: &[u8]) -> String {
        let mut hasher = DefaultHasher::new();
        // lets get some of the first pages, these should be some of the system pages, so hopefully unique
        hasher.write(&data[..10 * PAGE_SIZE]);
        hasher.write_usize(data.len());
        let hash = hasher.finish();
        format!(".mtf_backup_index_{:<016x}", hash)
    }

    fn try_load_cache(data: &[u8]) -> Option<Self> {
        let path = Self::cache_name(data);
        let path = Path::new(&path);
        if path.exists() {
            Some(bincode::deserialize_from(std::fs::File::open(path).unwrap()).unwrap())
        } else {
            None
        }
    }

    fn write_cache(&self, data: &[u8]) {
        let path = Self::cache_name(data);
        let file = std::fs::File::create(path).unwrap();
        bincode::serialize_into(file, self).unwrap()
    }

    pub fn build(data: &[u8]) -> Self {
        match Self::try_load_cache(data) {
            Some(idx) => idx,
            None => {
                let num_pages = data.len() / PAGE_SIZE;
                let outer_level_entries = num_pages / Self::DIVISOR;
                let divisor = Self::DIVISOR;
                let mut idx = Vec::new();

                // We need to save this index, because we cannot reconstruct it if the end coincides with zero pages
                let mut start_idx = 0;
                // First one should be valid
                let mut start = PageHeader::parse_ptr(data).unwrap();
                let mut old = start;
                let mut max_page_ids = HashMap::new();

                let mut write_entry = |start: PagePointer, end: PagePointer, start_idx: u32| {
                    while idx.len() < end.file_id as usize {
                        idx.push(vec![vec![]; outer_level_entries]);
                    }

                    idx[(end.file_id - 1) as usize][start.page_id as usize / divisor].push(
                        IndexEntry {
                            start: start.page_id,
                            stop: end.page_id,
                            base: start_idx,
                        },
                    );

                    max_page_ids
                        .entry(end.file_id)
                        .and_modify(|e| *e = end.page_id.max(*e))
                        .or_insert(end.page_id);
                };

                for i in 1..num_pages {
                    let new = PageHeader::parse_ptr(&data[i * PAGE_SIZE..]);

                    if let Some(new) = new {
                        if (start.file_id != new.file_id) || (old.page_id + 1) != (new.page_id) {
                            write_entry(start, old, start_idx);

                            start = new;
                            start_idx = i as u32;
                        }

                        old = new;
                    }
                }

                write_entry(start, old, start_idx);

                let idx = Self {
                    divisor,
                    outer_level_entries,
                    idx,
                    max_page_ids,
                };

                idx.write_cache(data);

                idx
            }
        }
    }

    pub fn lookup(&self, ptr: PagePointer) -> Option<u32> {
        let outer_entries = &self.idx[(ptr.file_id - 1) as usize];
        let mut outer_idx = ptr.page_id as usize / self.divisor;

        loop {
            while outer_entries[outer_idx].is_empty() {
                outer_idx -= 1;
            }

            let entries = &outer_entries[outer_idx];

            for entry in entries {
                if entry.start <= ptr.page_id && entry.stop >= ptr.page_id {
                    return Some(entry.base + ptr.page_id - entry.start);
                }
            }

            if outer_idx == 0 {
                break;
            } else {
                outer_idx -= 1;
            }
        }

        // Gracefully break, to make it easier to read broken tables
        // panic!("page not found in idx: {:#?}", ptr);
        // error!("could not find page {:?}, aborting early", ptr);
        None
    }
}

impl<'a> MTFPageProvider<'a> {
    pub fn from_stream(stream: StreamWithData<'a>) -> Self {
        assert_eq!(stream.stream.header.id, "MQDA");

        // For some reason there are two bytes at the start of this that don't actually belong
        Self {
            data: &stream.data[2..],
            index: MTFBackupIndex::build(&stream.data[2..]),
        }
    }
}

impl<'a> PageProvider for MTFPageProvider<'a> {
    fn file_ids(&self) -> Vec<u16> {
        self.index.max_page_ids.keys().cloned().collect()
    }

    fn num_pages(&self, file_id: u16) -> u32 {
        self.index.max_page_ids[&file_id] + 1
    }

    fn get(&self, ptr: PagePointer) -> Option<RawPage<Self>> {
        let idx = self.index.lookup(ptr);
        idx.and_then(|idx| {
            if (idx + 1) as usize * PAGE_SIZE <= self.data.len() {
                let page = RawPage::parse(
                    &self.data[idx as usize * PAGE_SIZE..(idx + 1) as usize * PAGE_SIZE],
                    self,
                );
                // Do some double checking here, maybe remove, when we are sure the index is working as expected
                assert_eq!(page.header.ptr, ptr);
                Some(page)
            } else {
                None
            }
        })
    }
}

use mdf::{PAGE_SIZE, PageHeader, PagePointer, PageProvider, RawPage};
use crate::StreamWithData;

pub struct MTFPageProvider<'a> {
    data: &'a [u8],
    index: MTFBackupIndex
}

#[derive(Clone, Copy)]
struct IndexEntry {
    start: u32, // inclusive
    stop: u32, // inclusive
    base: u32,
}

struct MTFBackupIndex {
    // First layer is one per file_id
    // that contains outer_level_entries entries per file_id
    // the index into that array is calculated by page_id / divisor
    // the innermost layer now contains a list of IndexEntries, that contain a
    // map of (start page id, stop page id) -> base actual index
    idx: Vec<Vec<Vec<IndexEntry>>>,
    outer_level_entries: usize,
    divisor: usize
}

impl MTFBackupIndex {
    // Shoot for approximately 1024 entries in the inner level
    // Assuming a average run length of O(100), this should work out nicely
    const DIVISOR: usize = 1024;

    pub fn build(data: &[u8]) -> Self {
        let num_pages = data.len() / PAGE_SIZE;
        let outer_level_entries = num_pages / Self::DIVISOR;
        let divisor = Self::DIVISOR;
        let mut idx = Vec::new();

        let mut start = PageHeader::parse_ptr(data);
        let mut old = start;
        for i in 1..num_pages {
            let new = PageHeader::parse_ptr(&data[i * PAGE_SIZE..]);
            // file_id == 0 is invalid and occurs mostly due to uninitialized pages
            if new.file_id == 0 {
                continue
            }

            if (start.file_id != new.file_id) || (old.page_id + 1) != (new.page_id) {
                while idx.len() < old.file_id as usize {
                    idx.push(vec![vec![]; outer_level_entries]);
                }

                // println!("found range: i = {}, start = {:?}, stop = {:?}", i, start, old);
                idx[(old.file_id - 1) as usize][start.page_id as usize / divisor].push(IndexEntry {
                    start: start.page_id,
                    stop: old.page_id,
                    base: (i - 1) as u32 + start.page_id - old.page_id
                });

                start = new;
            }

            old = new;
        }

        Self {
            divisor,
            outer_level_entries,
            idx,
        }
    }

    pub fn lookup(&self, ptr: PagePointer) -> u32 {
        let outer_entries = &self.idx[(ptr.file_id - 1) as usize];
        let mut outer_idx = ptr.page_id as usize / self.divisor;

        while outer_entries[outer_idx].len() == 0 {
            outer_idx -= 1;
        }

        let entries = &outer_entries[outer_idx];
        for entry in entries {
            if entry.start <= ptr.page_id && entry.stop >= ptr.page_id {
                return entry.base + ptr.page_id - entry.start;
            }
        }

        panic!("page not found in idx: {:#?}", ptr);
    }
}

impl<'a> MTFPageProvider<'a> {
    pub fn from_stream(stream: StreamWithData<'a>) -> Self {
        assert_eq!(stream.stream.header.id, "MQDA");

        // For some reason there are two bytes at the start of this that don't actually belong
        Self {
            data: &stream.data[2..],
            index: MTFBackupIndex::build(&stream.data[2..])
        }
    }
}

impl<'a> PageProvider for MTFPageProvider<'a> {
    fn get(&self, ptr: PagePointer) -> RawPage<Self> {
        let idx = self.index.lookup(ptr);
        let page = RawPage::parse(&self.data[idx as usize * PAGE_SIZE..(idx + 1) as usize * PAGE_SIZE], self);
        // Do some double checking here, maybe remove, when we are sure the index is working as expected
        assert_eq!(page.header.ptr, ptr);
        page
    }
}

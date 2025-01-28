pub struct DataCollection {
    count_committed_entries: usize,
}

impl DataCollection {
    pub fn new() -> Self {
        DataCollection {
            count_committed_entries: 0,
        }
    }

    pub fn commit_entry(&mut self) {
        self.count_committed_entries += 1
    }

    pub fn count_committed_entries(&self) -> usize {
        self.count_committed_entries
    }
}

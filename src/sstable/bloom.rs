use crate::Key;

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

#[derive(Clone, Debug)]
pub(crate) struct BloomFilter {
    pub(crate) m: u64,
    pub(crate) k: u32,
    pub(crate) seed: u64,
    pub(crate) bits: Vec<u8>,
}

impl BloomFilter {
    pub(crate) fn new(n: usize, p: f64, seed: u64) -> Self {
        let p = p.clamp(f64::EPSILON, 0.99);
        let m = ((-(n as f64) * p.ln()) / (std::f64::consts::LN_2 * std::f64::consts::LN_2)).ceil()
            as u64;
        let m = m.max(1024);
        let k = ((m as f64 / n.max(1) as f64) * std::f64::consts::LN_2).ceil() as u32;
        let k = k.clamp(1, 64);
        Self {
            m,
            k,
            seed,
            bits: vec![0u8; ((m + 7) / 8) as usize],
        }
    }

    fn hash(&self, key: &Key, salt: u64) -> u64 {
        let mut h = FNV_OFFSET ^ self.seed.wrapping_mul(salt);
        for b in key.0.iter().chain(key.1.to_le_bytes().iter()) {
            h ^= *b as u64;
            h = h.wrapping_mul(FNV_PRIME);
        }
        h
    }

    fn bit_indexes(&self, key: &Key) -> impl Iterator<Item = usize> + '_ {
        let h1 = self.hash(key, 1);
        let h2 = self.hash(key, 2) | 1;
        (0..self.k).map(move |i| (h1.wrapping_add((i as u64).wrapping_mul(h2)) % self.m) as usize)
    }

    pub(crate) fn insert(&mut self, key: &Key) {
        let m = self.m;
        let k = self.k;
        let h1 = self.hash(key, 1);
        let h2 = self.hash(key, 2) | 1;
        for i in 0..k {
            let idx = (h1.wrapping_add((i as u64).wrapping_mul(h2)) % m) as usize;
            self.bits[idx / 8] |= 1 << (idx % 8);
        }
    }

    pub(crate) fn contains(&self, key: &Key) -> bool {
        self.bit_indexes(key)
            .all(|i| self.bits[i / 8] & (1 << (i % 8)) != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Key;

    fn key(i: usize) -> Key {
        (format!("tag{i}").into_bytes(), i as i64)
    }

    #[test]
    fn insert_and_contains() {
        let mut bf = BloomFilter::new(1000, 0.01, 42);
        for i in 0..1000 {
            bf.insert(&key(i));
        }
        for i in 0..1000 {
            assert!(bf.contains(&key(i)), "key {i} should be contained");
        }
    }

    #[test]
    fn empty_filter_contains_nothing() {
        let bf = BloomFilter::new(0, 0.01, 42);
        for i in 0..100 {
            assert!(!bf.contains(&key(i)));
        }
    }

    #[test]
    fn m_has_minimum_floor() {
        let bf = BloomFilter::new(1, 0.01, 42);
        assert!(bf.m >= 1024);
    }

    #[test]
    fn false_positive_rate_within_bounds() {
        let mut bf = BloomFilter::new(10_000, 0.01, 7);
        for i in 0..10_000 {
            bf.insert(&key(i));
        }
        let mut fps = 0;
        for i in 10_000..20_000 {
            if bf.contains(&key(i)) {
                fps += 1;
            }
        }
        let rate = fps as f64 / 10_000.0;
        assert!(
            (0.001..0.05).contains(&rate),
            "false positive rate {rate} out of expected range"
        );
    }
}

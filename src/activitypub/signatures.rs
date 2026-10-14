//! Slice A3: RSA-2048 math, PEM/DER codecs, PKCS#1 v1.5 `rsa-sha256`,
//! `Digest` header helpers, and legacy HTTP Signatures (draft-cavage).
//!
//! Pure-Rust implementation using only crates already vendored in `Cargo.lock`
//! (`sha2` for hashing, `rand` for key generation, `axum::http` for headers,
//! `chrono` for `Date` skew checks). No external crypto crates.

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use rand::Rng;
use sha2::{Digest as _, Sha256};
use std::str::FromStr;

// ---------------------------------------------------------------------------
// BigUint: arbitrary-precision unsigned integer over u32 little-endian limbs
// ---------------------------------------------------------------------------

/// Arbitrary-precision unsigned integer, little-endian `u32` limbs, no
/// trailing zero limbs (zero is the empty limb vector).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BigUint {
    limbs: Vec<u32>,
}

impl PartialOrd for BigUint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BigUint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.limbs.len() != other.limbs.len() {
            return self.limbs.len().cmp(&other.limbs.len());
        }
        for i in (0..self.limbs.len()).rev() {
            if self.limbs[i] != other.limbs[i] {
                return self.limbs[i].cmp(&other.limbs[i]);
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl BigUint {
    pub fn zero() -> Self {
        Self { limbs: Vec::new() }
    }

    pub fn one() -> Self {
        Self { limbs: vec![1] }
    }

    pub fn is_zero(&self) -> bool {
        self.limbs.is_empty()
    }

    pub fn is_one(&self) -> bool {
        self.limbs.len() == 1 && self.limbs[0] == 1
    }

    pub fn is_odd(&self) -> bool {
        !self.limbs.is_empty() && self.limbs[0] & 1 == 1
    }

    fn trim(&mut self) {
        while self.limbs.last() == Some(&0) {
            self.limbs.pop();
        }
    }

    fn limb(&self, i: usize) -> u32 {
        if i < self.limbs.len() {
            self.limbs[i]
        } else {
            0
        }
    }

    pub fn from_u32(v: u32) -> Self {
        if v == 0 {
            Self::zero()
        } else {
            Self { limbs: vec![v] }
        }
    }

    pub fn from_u64(v: u64) -> Self {
        if v == 0 {
            return Self::zero();
        }
        let mut limbs = vec![v as u32];
        if v >> 32 != 0 {
            limbs.push((v >> 32) as u32);
        }
        Self { limbs }
    }

    pub fn from_bytes_be(bytes: &[u8]) -> Self {
        let mut limbs = vec![0u32; bytes.len().div_ceil(4)];
        for (i, &b) in bytes.iter().rev().enumerate() {
            limbs[i / 4] |= (b as u32) << ((i % 4) * 8);
        }
        let mut v = Self { limbs };
        v.trim();
        v
    }

    /// Minimal big-endian encoding (zero encodes as empty).
    pub fn to_bytes_be(&self) -> Vec<u8> {
        if self.is_zero() {
            return Vec::new();
        }
        let mut out = vec![0u8; self.limbs.len() * 4];
        for (i, &l) in self.limbs.iter().enumerate() {
            let o = out.len() - (i + 1) * 4;
            out[o..o + 4].copy_from_slice(&l.to_be_bytes());
        }
        let skip = out.iter().position(|&b| b != 0).unwrap_or(out.len());
        out[skip..].to_vec()
    }

    /// Fixed-length big-endian encoding, left-padded with zeros.
    pub fn to_bytes_be_padded(&self, len: usize) -> Result<Vec<u8>, String> {
        let raw = self.to_bytes_be();
        if raw.len() > len {
            return Err("integer too large for target length".to_string());
        }
        let mut out = vec![0u8; len - raw.len()];
        out.extend_from_slice(&raw);
        Ok(out)
    }

    pub fn bit_len(&self) -> usize {
        match self.limbs.last() {
            None => 0,
            Some(&top) => (self.limbs.len() - 1) * 32 + (32 - top.leading_zeros() as usize),
        }
    }

    pub fn get_bit(&self, i: usize) -> bool {
        let (w, b) = (i / 32, i % 32);
        w < self.limbs.len() && (self.limbs[w] >> b) & 1 == 1
    }

    pub fn shl(&self, bits: u32) -> Self {
        if self.is_zero() {
            return Self::zero();
        }
        let words = (bits / 32) as usize;
        let rem = bits % 32;
        let mut limbs = vec![0u32; words + self.limbs.len() + usize::from(rem > 0)];
        if rem == 0 {
            limbs[words..words + self.limbs.len()].copy_from_slice(&self.limbs);
        } else {
            let mut carry = 0u32;
            for (i, &l) in self.limbs.iter().enumerate() {
                let cur = ((l as u64) << rem) | carry as u64;
                limbs[words + i] = cur as u32;
                carry = (cur >> 32) as u32;
            }
            limbs[words + self.limbs.len()] = carry;
        }
        let mut v = Self { limbs };
        v.trim();
        v
    }

    pub fn shr(&self, bits: u32) -> Self {
        let words = (bits / 32) as usize;
        if words >= self.limbs.len() {
            return Self::zero();
        }
        let rem = bits % 32;
        let mut limbs = vec![0u32; self.limbs.len() - words];
        if rem == 0 {
            limbs.copy_from_slice(&self.limbs[words..]);
        } else {
            let mut carry = 0u32;
            for i in (0..limbs.len()).rev() {
                let l = self.limbs[i + words];
                limbs[i] = (l >> rem) | carry;
                carry = l << (32 - rem);
            }
        }
        let mut v = Self { limbs };
        v.trim();
        v
    }

    pub fn add(&self, other: &Self) -> Self {
        let n = self.limbs.len().max(other.limbs.len());
        let mut limbs = Vec::with_capacity(n + 1);
        let mut carry: u64 = 0;
        for i in 0..n {
            let s = self.limb(i) as u64 + other.limb(i) as u64 + carry;
            limbs.push(s as u32);
            carry = s >> 32;
        }
        if carry != 0 {
            limbs.push(carry as u32);
        }
        let mut v = Self { limbs };
        v.trim();
        v
    }

    pub fn add_small(&self, v: u32) -> Self {
        self.add(&Self::from_u32(v))
    }

    /// Subtraction; errors on underflow (`self < other`).
    pub fn sub(&self, other: &Self) -> Result<Self, String> {
        if self < other {
            return Err("BigUint subtraction underflow".to_string());
        }
        let mut limbs = Vec::with_capacity(self.limbs.len());
        let mut borrow: i64 = 0;
        for i in 0..self.limbs.len() {
            let d = self.limbs[i] as i64 - other.limb(i) as i64 - borrow;
            limbs.push(d as u32);
            borrow = if d < 0 { 1 } else { 0 };
        }
        let mut v = Self { limbs };
        v.trim();
        Ok(v)
    }

    /// Square `self` using symmetry (`a[i] * a[j]` for `i < j` doubled).
    pub fn square(&self) -> Self {
        if self.is_zero() {
            return Self::zero();
        }
        let n = self.limbs.len();
        let mut limbs = vec![0u32; 2 * n];
        for i in 0..n {
            let ai = self.limbs[i] as u64;
            let mut carry: u64 = 0;
            for j in (i + 1)..n {
                let prod = ai * (self.limbs[j] as u64);
                let idx = i + j;
                let t = limbs[idx] as u64 + prod + carry;
                limbs[idx] = t as u32;
                carry = t >> 32;
            }
            let mut k = i + n;
            while carry != 0 {
                let t = limbs[k] as u64 + carry;
                limbs[k] = t as u32;
                carry = t >> 32;
                k += 1;
            }
        }
        // Double cross terms.
        let mut shift_carry = 0u32;
        for limb in limbs.iter_mut() {
            let next_carry = *limb >> 31;
            *limb = (*limb << 1) | shift_carry;
            shift_carry = next_carry;
        }
        // Add diagonal terms `a[i]^2`.
        let mut diag_carry: u64 = 0;
        for i in 0..n {
            let ai = self.limbs[i] as u64;
            let sq = ai * ai;
            let lo = (sq & 0xffff_ffff) + limbs[2 * i] as u64 + diag_carry;
            limbs[2 * i] = lo as u32;
            let hi = (sq >> 32) + limbs[2 * i + 1] as u64 + (lo >> 32);
            limbs[2 * i + 1] = hi as u32;
            diag_carry = hi >> 32;
        }
        let mut v = Self { limbs };
        v.trim();
        v
    }

    pub fn mul(&self, other: &Self) -> Self {
        if self.is_zero() || other.is_zero() {
            return Self::zero();
        }
        let mut limbs = vec![0u32; self.limbs.len() + other.limbs.len()];
        for (i, &a) in self.limbs.iter().enumerate() {
            let mut carry: u64 = 0;
            for (j, &b) in other.limbs.iter().enumerate() {
                let t = limbs[i + j] as u64 + a as u64 * b as u64 + carry;
                limbs[i + j] = t as u32;
                carry = t >> 32;
            }
            let mut k = i + other.limbs.len();
            while carry != 0 {
                let t = limbs[k] as u64 + carry;
                limbs[k] = t as u32;
                carry = t >> 32;
                k += 1;
            }
        }
        let mut v = Self { limbs };
        v.trim();
        v
    }

    /// Knuth TAOCP 4.3.1 Algorithm D division. Returns `(quotient, remainder)`.
    pub fn div_rem(a: &Self, b: &Self) -> Result<(Self, Self), String> {
        if b.is_zero() {
            return Err("division by zero".to_string());
        }
        if a < b {
            return Ok((Self::zero(), a.clone()));
        }
        let n = a.limbs.len();
        let m = b.limbs.len();
        if m == 1 {
            let mut q = vec![0u32; n];
            let mut rem: u64 = 0;
            let d = b.limbs[0] as u64;
            for i in (0..n).rev() {
                let cur = (rem << 32) | a.limbs[i] as u64;
                q[i] = (cur / d) as u32;
                rem = cur % d;
            }
            let mut qq = Self { limbs: q };
            qq.trim();
            return Ok((qq, Self::from_u64(rem)));
        }
        const B: u64 = 1 << 32;
        let shift = b.limbs[m - 1].leading_zeros();
        let vn = b.shl(shift);
        let mut un = a.shl(shift);
        un.limbs.resize(n + 1, 0);
        let mut q = vec![0u32; n - m + 1];
        for j in (0..=(n - m)).rev() {
            let ujm = un.limbs[j + m] as u64;
            let ujm1 = un.limbs[j + m - 1] as u64;
            let ujm2 = un.limbs[j + m - 2] as u64;
            let mut qhat = ((ujm * B + ujm1) / vn.limbs[m - 1] as u64).min(B - 1);
            let mut rhat = (ujm * B + ujm1) % vn.limbs[m - 1] as u64;
            while qhat * vn.limbs[m - 2] as u64 > B * rhat + ujm2 {
                qhat -= 1;
                rhat += vn.limbs[m - 1] as u64;
                if rhat >= B {
                    break;
                }
            }
            // un[j..=j+m] -= qhat * vn[0..m]
            let mut k: i64 = 0;
            for i in 0..m {
                let p = qhat * vn.limbs[i] as u64;
                let t = un.limbs[j + i] as i64 - k - (p & 0xffff_ffff) as i64;
                un.limbs[j + i] = t as u32;
                k = (p >> 32) as i64 - (t >> 32);
            }
            let t = un.limbs[j + m] as i64 - k;
            un.limbs[j + m] = t as u32;
            q[j] = qhat as u32;
            if t < 0 {
                // Add back: qhat was one too big.
                q[j] = q[j].wrapping_sub(1);
                let mut carry: u64 = 0;
                for i in 0..m {
                    let t = un.limbs[j + i] as u64 + vn.limbs[i] as u64 + carry;
                    un.limbs[j + i] = t as u32;
                    carry = t >> 32;
                }
                un.limbs[j + m] = un.limbs[j + m].wrapping_add(carry as u32);
            }
        }
        let mut qq = Self { limbs: q };
        qq.trim();
        let mut rr = Self {
            limbs: un.limbs[..m].to_vec(),
        };
        rr = rr.shr(shift);
        rr.trim();
        Ok((qq, rr))
    }

    pub fn rem_u32(&self, d: u32) -> u32 {
        let mut rem: u64 = 0;
        let divisor = d as u64;
        for &limb in self.limbs.iter().rev() {
            rem = ((rem << 32) | limb as u64) % divisor;
        }
        rem as u32
    }

    pub fn rem(&self, m: &Self) -> Result<Self, String> {
        Ok(Self::div_rem(self, m)?.1)
    }

    /// Binary square-and-multiply modular exponentiation.
    pub fn mod_pow(base: &Self, exp: &Self, modulus: &Self) -> Result<Self, String> {
        if modulus.is_zero() {
            return Err("mod_pow with zero modulus".to_string());
        }
        if modulus.is_one() {
            return Ok(Self::zero());
        }
        let b = Self::div_rem(base, modulus)?.1;
        let mut result = Self::one();
        for i in (0..exp.bit_len()).rev() {
            result = Self::div_rem(&result.square(), modulus)?.1;
            if exp.get_bit(i) {
                result = Self::div_rem(&result.mul(&b), modulus)?.1;
            }
        }
        Ok(result)
    }

    /// Extended-GCD modular inverse: `a^-1 mod m`.
    pub fn mod_inverse(a: &Self, m: &Self) -> Result<Self, String> {
        if m.is_zero() || m.is_one() {
            return Err("mod_inverse needs modulus > 1".to_string());
        }
        let aa = Self::div_rem(a, m)?.1;
        let (mut old_r, mut r) = (aa, m.clone());
        let (mut old_s, mut s) = (SignedBig::one(), SignedBig::zero());
        while !r.is_zero() {
            let (q, rem) = Self::div_rem(&old_r, &r)?;
            let qs = s.mul_mag(&q);
            let new_s = old_s.sub(&qs);
            old_r = r;
            r = rem;
            old_s = s;
            s = new_s;
        }
        if !old_r.is_one() {
            return Err("mod_inverse: not coprime".to_string());
        }
        Ok(old_s.to_mod(m))
    }
}

/// Signed big integer used only by the extended Euclidean algorithm.
#[derive(Clone, Debug)]
struct SignedBig {
    neg: bool,
    mag: BigUint,
}

impl SignedBig {
    fn zero() -> Self {
        Self {
            neg: false,
            mag: BigUint::zero(),
        }
    }

    fn one() -> Self {
        Self {
            neg: false,
            mag: BigUint::one(),
        }
    }

    fn normalize(mut self) -> Self {
        if self.mag.is_zero() {
            self.neg = false;
        }
        self
    }

    fn sub(&self, other: &Self) -> Self {
        if self.neg == other.neg {
            if self.mag >= other.mag {
                Self {
                    neg: self.neg,
                    mag: self.mag.sub(&other.mag).unwrap_or_else(|_| BigUint::zero()),
                }
                .normalize()
            } else {
                Self {
                    neg: !self.neg,
                    mag: other.mag.sub(&self.mag).unwrap_or_else(|_| BigUint::zero()),
                }
                .normalize()
            }
        } else {
            Self {
                neg: self.neg,
                mag: self.mag.add(&other.mag),
            }
            .normalize()
        }
    }

    fn mul_mag(&self, q: &BigUint) -> Self {
        Self {
            neg: self.neg && !q.is_zero() && !self.mag.is_zero(),
            mag: self.mag.mul(q),
        }
        .normalize()
    }

    fn to_mod(&self, m: &BigUint) -> BigUint {
        let r = self.mag.rem(m).unwrap_or_else(|_| BigUint::zero());
        if self.neg && !r.is_zero() {
            m.sub(&r).unwrap_or_else(|_| BigUint::zero())
        } else {
            r
        }
    }
}

// ---------------------------------------------------------------------------
// Primality testing and RSA key generation
// ---------------------------------------------------------------------------

const SMALL_PRIMES: &[u32] = &[
    2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97,
    101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193,
    197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283, 293, 307,
    311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421,
    431, 433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541, 547,
    557, 563, 569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659,
    661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797,
    809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929,
    937, 941, 947, 953, 967, 971, 977, 983, 991, 997,
];

/// Miller-Rabin probable-prime test with trial division + 8 random-base rounds.
pub fn is_probable_prime(
    n: &BigUint,
    fill: &mut dyn FnMut(&mut [u8]),
) -> Result<bool, String> {
    if n.bit_len() <= 1 {
        return Ok(false);
    }
    for &p in SMALL_PRIMES {
        if n.limbs.len() == 1 && n.limbs[0] == p {
            return Ok(true);
        }
        if n.rem_u32(p) == 0 {
            return Ok(false);
        }
    }
    // n - 1 = 2^s * d with d odd.
    let one = BigUint::one();
    let nm1 = n.sub(&one)?;
    let mut d = nm1.clone();
    let mut s: u32 = 0;
    while !d.is_odd() {
        d = d.shr(1);
        s += 1;
    }
    let two = BigUint::from_u32(2);
    let nm2 = n.sub(&two)?;
    let range = nm2.sub(&one)?; // n - 3; bases live in [2, n-2]
    let byte_len = (n.bit_len() + 7) / 8;
    let mut buf = vec![0u8; byte_len];
    'rounds: for _ in 0..5 {
        fill(&mut buf);
        let a = BigUint::from_bytes_be(&buf).rem(&range)?.add_small(2);
        let mut x = BigUint::mod_pow(&a, &d, n)?;
        if x == one || x == nm1 {
            continue 'rounds;
        }
        for _ in 1..s {
            x = BigUint::div_rem(&x.square(), n)?.1;
            if x == nm1 {
                continue 'rounds;
            }
            if x.is_one() {
                return Ok(false);
            }
        }
        return Ok(false);
    }
    Ok(true)
}

fn add_two(v: &BigUint) -> Option<BigUint> {
    let two = BigUint::from_u32(2);
    let r = v.add(&two);
    if r.bit_len() > v.bit_len() {
        None
    } else {
        Some(r)
    }
}

/// Random `bits`-wide prime (top two bits set so the product of two
/// `bits/2` primes is guaranteed to be `bits` bits wide).
fn gen_prime(bits: usize, fill: &mut dyn FnMut(&mut [u8])) -> Result<BigUint, String> {
    if bits < 16 {
        return Err("gen_prime needs at least 16 bits".to_string());
    }
    let byte_len = (bits + 7) / 8;
    loop {
        let mut buf = vec![0u8; byte_len];
        fill(&mut buf);
        // Mask excess high bits when bits is not a multiple of 8, then set the
        // top two bits (so p * q has exact bit length 2 * bits) and the low bit.
        let excess = byte_len * 8 - bits;
        if excess > 0 {
            buf[0] &= 0xffu8 >> excess;
        }
        let top_bit = 7 - excess;
        buf[0] |= 1u8 << top_bit;
        if top_bit >= 1 {
            buf[0] |= 1u8 << (top_bit - 1);
        } else if buf.len() > 1 {
            buf[1] |= 0x80;
        }
        let last = buf.len() - 1;
        buf[last] |= 1u8;

        let mut cand = BigUint::from_bytes_be(&buf);
        let mut rem: Vec<u32> = SMALL_PRIMES.iter().map(|&p| cand.rem_u32(p)).collect();
        'step: loop {
            let mut composite = false;
            for (i, &p) in SMALL_PRIMES.iter().enumerate() {
                if rem[i] == 0 {
                    composite = true;
                    break;
                }
                let _ = p;
            }
            if !composite && is_probable_prime(&cand, fill)? {
                return Ok(cand);
            }
            match add_two(&cand) {
                Some(next) => {
                    cand = next;
                    for (i, &p) in SMALL_PRIMES.iter().enumerate() {
                        let r = rem[i] + 2;
                        rem[i] = if r >= p { r - p } else { r };
                    }
                }
                None => break 'step, // overflowed bit length; draw fresh bytes
            }
        }
    }
}

/// RSA public key (`n`, `e`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RsaPublicKey {
    pub n: BigUint,
    pub e: BigUint,
}

/// RSA private key with CRT parameters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RsaPrivateKey {
    pub n: BigUint,
    pub e: BigUint,
    pub d: BigUint,
    pub p: BigUint,
    pub q: BigUint,
    /// `d mod (p-1)`
    pub dp: BigUint,
    /// `d mod (q-1)`
    pub dq: BigUint,
    /// `q^-1 mod p`
    pub qinv: BigUint,
}

fn generate_rsa_keypair_with_fill(
    bits: usize,
    fill: &mut dyn FnMut(&mut [u8]),
) -> Result<RsaPrivateKey, String> {
    if bits < 512 || bits % 2 != 0 {
        return Err("generate_rsa_keypair needs an even bit size >= 512".to_string());
    }
    let e = BigUint::from_u32(65537);
    loop {
        let p = gen_prime(bits / 2, fill)?;
        let q = gen_prime(bits / 2, fill)?;
        if p == q {
            continue;
        }
        let n = p.mul(&q);
        if n.bit_len() != bits {
            continue;
        }
        let pm1 = p.sub(&BigUint::one())?;
        let qm1 = q.sub(&BigUint::one())?;
        let phi = pm1.mul(&qm1);
        let d = match BigUint::mod_inverse(&e, &phi) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let dp = d.rem(&pm1)?;
        let dq = d.rem(&qm1)?;
        let qinv = match BigUint::mod_inverse(&q, &p) {
            Ok(v) => v,
            Err(_) => continue,
        };
        return Ok(RsaPrivateKey {
            n,
            e,
            d,
            p,
            q,
            dp,
            dq,
            qinv,
        });
    }
}

/// Generate a random RSA keypair of `bits` bits (default 2048 via
/// [`generate_rsa_keypair_2048`]). Returns `(private_key, public_key)`.
pub fn generate_rsa_keypair(bits: usize) -> Result<(RsaPrivateKey, RsaPublicKey), String> {
    let mut rng = rand::rng();
    let mut fill = |buf: &mut [u8]| rng.fill_bytes(buf);
    let priv_key = generate_rsa_keypair_with_fill(bits, &mut fill)?;
    let pub_key = RsaPublicKey {
        n: priv_key.n.clone(),
        e: priv_key.e.clone(),
    };
    Ok((priv_key, pub_key))
}

/// Generate a random 2048-bit RSA keypair.
pub fn generate_rsa_keypair_2048() -> Result<(RsaPrivateKey, RsaPublicKey), String> {
    generate_rsa_keypair(2048)
}

// ---------------------------------------------------------------------------
// Base64 (RFC 4648 standard alphabet)
// ---------------------------------------------------------------------------

const B64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64_ALPHABET[((triple >> 18) & 63) as usize] as char);
        out.push(B64_ALPHABET[((triple >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64_ALPHABET[((triple >> 6) & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(B64_ALPHABET[(triple & 63) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn b64_value(c: u8) -> Result<u8, String> {
    match c {
        b'A'..=b'Z' => Ok(c - b'A'),
        b'a'..=b'z' => Ok(c - b'a' + 26),
        b'0'..=b'9' => Ok(c - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err(format!("invalid base64 character: 0x{c:02x}")),
    }
}

pub fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    let clean: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if clean.is_empty() {
        return Ok(Vec::new());
    }
    if clean.len() % 4 != 0 {
        return Err("invalid base64 length".to_string());
    }
    let mut out = Vec::with_capacity(clean.len() / 4 * 3);
    let quanta = clean.chunks(4).len();
    for (qi, chunk) in clean.chunks(4).enumerate() {
        let pad = chunk.iter().rev().take_while(|&&b| b == b'=').count();
        if pad > 2 {
            return Err("invalid base64 padding".to_string());
        }
        if pad > 0 && qi + 1 != quanta {
            return Err("base64 padding in non-final quantum".to_string());
        }
        let mut vals = [0u8; 4];
        for (i, &c) in chunk.iter().enumerate() {
            if c == b'=' {
                if i < 4 - pad {
                    return Err("misplaced base64 padding".to_string());
                }
                vals[i] = 0;
            } else {
                if pad > 0 && i >= 4 - pad {
                    return Err("misplaced base64 padding".to_string());
                }
                vals[i] = b64_value(c)?;
            }
        }
        let triple =
            (vals[0] as u32) << 18 | (vals[1] as u32) << 12 | (vals[2] as u32) << 6 | vals[3] as u32;
        out.push((triple >> 16) as u8);
        if pad < 2 {
            out.push((triple >> 8) as u8);
        }
        if pad < 1 {
            out.push(triple as u8);
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// ASN.1 DER writer / reader (minimal subset for RSA keys)
// ---------------------------------------------------------------------------

fn der_encode_len(len: usize, out: &mut Vec<u8>) {
    if len < 128 {
        out.push(len as u8);
    } else {
        let mut tmp = len;
        let mut bytes = Vec::new();
        while tmp > 0 {
            bytes.push(tmp as u8);
            tmp >>= 8;
        }
        bytes.reverse();
        out.push(0x80 | bytes.len() as u8);
        out.extend_from_slice(&bytes);
    }
}

fn der_encode_integer_bytes(content: &[u8], out: &mut Vec<u8>) {
    // Strip leading zeros, then force the sign bit clear.
    let mut start = 0;
    while start < content.len() && content[start] == 0 {
        start += 1;
    }
    let mut body = &content[start..];
    if body.is_empty() {
        body = &[0];
    }
    out.push(0x02);
    let extra = usize::from(body[0] & 0x80 != 0);
    der_encode_len(body.len() + extra, out);
    if extra == 1 {
        out.push(0x00);
    }
    out.extend_from_slice(body);
}

fn der_encode_integer(v: &BigUint, out: &mut Vec<u8>) {
    der_encode_integer_bytes(&v.to_bytes_be(), out);
}

fn der_encode_sequence(content: &[u8], out: &mut Vec<u8>) {
    out.push(0x30);
    der_encode_len(content.len(), out);
    out.extend_from_slice(content);
}

fn der_encode_null(out: &mut Vec<u8>) {
    out.push(0x05);
    out.push(0x00);
}

/// DER of the rsaEncryption algorithm identifier (OID 1.2.840.113549.1.1.1 + NULL).
fn der_rsa_alg_id() -> Vec<u8> {
    let mut inner = Vec::new();
    inner.push(0x06);
    der_encode_len(9, &mut inner);
    inner.extend_from_slice(&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01]);
    der_encode_null(&mut inner);
    let mut out = Vec::new();
    der_encode_sequence(&inner, &mut out);
    out
}

fn der_encode_bit_string(content: &[u8], out: &mut Vec<u8>) {
    out.push(0x03);
    der_encode_len(content.len() + 1, out);
    out.push(0x00); // zero unused bits
    out.extend_from_slice(content);
}

fn der_encode_octet_string(content: &[u8], out: &mut Vec<u8>) {
    out.push(0x04);
    der_encode_len(content.len(), out);
    out.extend_from_slice(content);
}

/// PKCS#1 `RSAPublicKey ::= SEQUENCE { modulus, publicExponent }`.
fn encode_rsa_public_der(key: &RsaPublicKey) -> Vec<u8> {
    let mut inner = Vec::new();
    der_encode_integer(&key.n, &mut inner);
    der_encode_integer(&key.e, &mut inner);
    let mut out = Vec::new();
    der_encode_sequence(&inner, &mut out);
    out
}

/// SubjectPublicKeyInfo wrapping PKCS#1.
fn encode_spki_der(key: &RsaPublicKey) -> Vec<u8> {
    let rsa_der = encode_rsa_public_der(key);
    let mut inner = Vec::new();
    inner.extend_from_slice(&der_rsa_alg_id());
    der_encode_bit_string(&rsa_der, &mut inner);
    let mut out = Vec::new();
    der_encode_sequence(&inner, &mut out);
    out
}

/// PKCS#1 `RSAPrivateKey` (version 0 + n, e, d, p, q, dP, dQ, qInv).
fn encode_pkcs1_private_der(key: &RsaPrivateKey) -> Vec<u8> {
    let mut inner = Vec::new();
    der_encode_integer_bytes(&[0], &mut inner);
    der_encode_integer(&key.n, &mut inner);
    der_encode_integer(&key.e, &mut inner);
    der_encode_integer(&key.d, &mut inner);
    der_encode_integer(&key.p, &mut inner);
    der_encode_integer(&key.q, &mut inner);
    der_encode_integer(&key.dp, &mut inner);
    der_encode_integer(&key.dq, &mut inner);
    der_encode_integer(&key.qinv, &mut inner);
    let mut out = Vec::new();
    der_encode_sequence(&inner, &mut out);
    out
}

/// PKCS#8 `PrivateKeyInfo` wrapping PKCS#1.
fn encode_pkcs8_private_der(key: &RsaPrivateKey) -> Vec<u8> {
    let pkcs1 = encode_pkcs1_private_der(key);
    let mut inner = Vec::new();
    der_encode_integer_bytes(&[0], &mut inner);
    inner.extend_from_slice(&der_rsa_alg_id());
    der_encode_octet_string(&pkcs1, &mut inner);
    let mut out = Vec::new();
    der_encode_sequence(&inner, &mut out);
    out
}

fn der_read_tlv(data: &[u8], pos: usize) -> Result<(u8, &[u8], usize), String> {
    if pos + 2 > data.len() {
        return Err("DER truncated".to_string());
    }
    let tag = data[pos];
    let mut p = pos + 1;
    let first = data[p];
    p += 1;
    let len = if first & 0x80 == 0 {
        first as usize
    } else {
        let count = (first & 0x7f) as usize;
        if count == 0 || count > 4 {
            return Err("unsupported DER length form".to_string());
        }
        if p + count > data.len() {
            return Err("DER truncated".to_string());
        }
        let mut len = 0usize;
        for i in 0..count {
            len = (len << 8) | data[p + i] as usize;
        }
        if len < 128 {
            return Err("non-minimal DER length".to_string());
        }
        p += count;
        len
    };
    if len > 16 * 1024 * 1024 {
        return Err("DER object too large".to_string());
    }
    if p + len > data.len() {
        return Err("DER truncated".to_string());
    }
    Ok((tag, &data[p..p + len], p + len))
}

/// Split concatenated TLVs, returning `(tag, body)` pairs.
fn der_split_tlvs(data: &[u8]) -> Result<Vec<(u8, Vec<u8>)>, String> {
    let mut out = Vec::new();
    let mut p = 0;
    while p < data.len() {
        let (tag, body, next) = der_read_tlv(data, p)?;
        out.push((tag, body.to_vec()));
        p = next;
    }
    Ok(out)
}

fn der_parse_integer(body: &[u8]) -> Result<BigUint, String> {
    if body.is_empty() {
        return Err("empty DER INTEGER".to_string());
    }
    if body[0] & 0x80 != 0 {
        return Err("negative DER INTEGER".to_string());
    }
    let mut bytes = body;
    while bytes.len() > 1 && bytes[0] == 0 {
        bytes = &bytes[1..];
    }
    Ok(BigUint::from_bytes_be(bytes))
}

fn parse_rsa_public_der(der: &[u8]) -> Result<RsaPublicKey, String> {
    let (tag, body, end) = der_read_tlv(der, 0)?;
    if tag != 0x30 || end != der.len() {
        return Err("expected DER SEQUENCE for RSAPublicKey".to_string());
    }
    let parts = der_split_tlvs(body)?;
    if parts.len() != 2 || parts[0].0 != 0x02 || parts[1].0 != 0x02 {
        return Err("malformed RSAPublicKey".to_string());
    }
    Ok(RsaPublicKey {
        n: der_parse_integer(&parts[0].1)?,
        e: der_parse_integer(&parts[1].1)?,
    })
}

fn parse_spki_der(der: &[u8]) -> Result<RsaPublicKey, String> {
    let (tag, body, end) = der_read_tlv(der, 0)?;
    if tag != 0x30 || end != der.len() {
        return Err("expected DER SEQUENCE for SubjectPublicKeyInfo".to_string());
    }
    let parts = der_split_tlvs(body)?;
    if parts.len() != 2 || parts[0].0 != 0x30 || parts[1].0 != 0x03 {
        return Err("malformed SubjectPublicKeyInfo".to_string());
    }
    let bit_string = &parts[1].1;
    if bit_string.is_empty() || bit_string[0] != 0x00 {
        return Err("unsupported BIT STRING padding".to_string());
    }
    parse_rsa_public_der(&bit_string[1..])
}

fn parse_pkcs1_private_der(der: &[u8]) -> Result<RsaPrivateKey, String> {
    let (tag, body, end) = der_read_tlv(der, 0)?;
    if tag != 0x30 || end != der.len() {
        return Err("expected DER SEQUENCE for RSAPrivateKey".to_string());
    }
    let parts = der_split_tlvs(body)?;
    if parts.len() != 9 || parts[0].0 != 0x02 {
        return Err("malformed RSAPrivateKey".to_string());
    }
    for part in parts.iter().skip(1) {
        if part.0 != 0x02 {
            return Err("malformed RSAPrivateKey".to_string());
        }
    }
    let version = der_parse_integer(&parts[0].1)?;
    if !version.is_zero() {
        return Err("unsupported RSAPrivateKey version".to_string());
    }
    Ok(RsaPrivateKey {
        n: der_parse_integer(&parts[1].1)?,
        e: der_parse_integer(&parts[2].1)?,
        d: der_parse_integer(&parts[3].1)?,
        p: der_parse_integer(&parts[4].1)?,
        q: der_parse_integer(&parts[5].1)?,
        dp: der_parse_integer(&parts[6].1)?,
        dq: der_parse_integer(&parts[7].1)?,
        qinv: der_parse_integer(&parts[8].1)?,
    })
}

fn parse_pkcs8_private_der(der: &[u8]) -> Result<RsaPrivateKey, String> {
    let (tag, body, end) = der_read_tlv(der, 0)?;
    if tag != 0x30 || end != der.len() {
        return Err("expected DER SEQUENCE for PrivateKeyInfo".to_string());
    }
    let parts = der_split_tlvs(body)?;
    if parts.len() != 3 || parts[0].0 != 0x02 || parts[1].0 != 0x30 || parts[2].0 != 0x04 {
        return Err("malformed PrivateKeyInfo".to_string());
    }
    let version = der_parse_integer(&parts[0].1)?;
    if !version.is_zero() {
        return Err("unsupported PrivateKeyInfo version".to_string());
    }
    parse_pkcs1_private_der(&parts[2].1)
}

// ---------------------------------------------------------------------------
// PEM encoding / decoding
// ---------------------------------------------------------------------------

fn pem_encode(label: &str, der: &[u8]) -> String {
    let b64 = base64_encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).unwrap_or(""));
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

fn pem_decode(pem: &str) -> Result<(String, Vec<u8>), String> {
    let begin = pem.find("-----BEGIN ").ok_or("missing PEM BEGIN line")?;
    let label_start = begin + "-----BEGIN ".len();
    let label_end = pem[label_start..]
        .find("-----")
        .ok_or("malformed PEM BEGIN line")?;
    let label = pem[label_start..label_start + label_end].trim().to_string();
    let end_marker = format!("-----END {label}-----");
    let body_start = label_start + label_end + "-----".len();
    let end = pem[body_start..]
        .find(&end_marker)
        .ok_or("missing PEM END line")?;
    let body = &pem[body_start..body_start + end];
    Ok((label, base64_decode(body)?))
}

/// Encode a public key as `-----BEGIN PUBLIC KEY-----` (SPKI) PEM, the form
/// Misskey (`publicKeyPem`) and Node `crypto` expect, with 64-char lines.
pub fn public_key_to_spki_pem(key: &RsaPublicKey) -> String {
    pem_encode("PUBLIC KEY", &encode_spki_der(key))
}

/// Encode a private key as `-----BEGIN PRIVATE KEY-----` (PKCS#8) PEM.
pub fn private_key_to_pkcs8_pem(key: &RsaPrivateKey) -> String {
    pem_encode("PRIVATE KEY", &encode_pkcs8_private_der(key))
}

/// Encode a private key as `-----BEGIN RSA PRIVATE KEY-----` (PKCS#1) PEM.
pub fn private_key_to_pkcs1_pem(key: &RsaPrivateKey) -> String {
    pem_encode("RSA PRIVATE KEY", &encode_pkcs1_private_der(key))
}

/// Parse a `PUBLIC KEY` (SPKI) or `RSA PUBLIC KEY` (raw PKCS#1) PEM.
pub fn parse_public_key_pem(pem: &str) -> Result<RsaPublicKey, String> {
    let (label, der) = pem_decode(pem)?;
    match label.as_str() {
        "PUBLIC KEY" => parse_spki_der(&der),
        "RSA PUBLIC KEY" => parse_rsa_public_der(&der),
        _ => Err(format!("unsupported PEM label: {label}")),
    }
}

/// Parse a `PRIVATE KEY` (PKCS#8) or `RSA PRIVATE KEY` (PKCS#1) PEM.
pub fn parse_private_key_pem(pem: &str) -> Result<RsaPrivateKey, String> {
    let (label, der) = pem_decode(pem)?;
    match label.as_str() {
        "PRIVATE KEY" => parse_pkcs8_private_der(&der),
        "RSA PRIVATE KEY" => parse_pkcs1_private_der(&der),
        _ => Err(format!("unsupported PEM label: {label}")),
    }
}

// ---------------------------------------------------------------------------
// PKCS#1 v1.5 rsa-sha256 sign & verify (RFC 8017 section 9.2)
// ---------------------------------------------------------------------------

/// DigestInfo prefix for SHA-256 (19 bytes), followed by the 32-byte hash.
const SHA256_DIGEST_INFO_PREFIX: [u8; 19] = [
    0x30, 0x31, 0x30, 0x0d, 0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01, 0x05,
    0x00, 0x04, 0x20,
];

/// Sign `message` with PKCS#1 v1.5 + SHA-256. Returns the raw signature bytes.
pub fn rsa_sha256_sign(private_key_pem: &str, message: &[u8]) -> Result<Vec<u8>, String> {
    let key = parse_private_key_pem(private_key_pem)?;
    let hash = Sha256::digest(message);
    let k = (key.n.bit_len() + 7) / 8;
    const T_LEN: usize = 19 + 32;
    if k < T_LEN + 3 + 8 {
        return Err("key too small for PKCS#1 v1.5 SHA-256".to_string());
    }
    let mut em = Vec::with_capacity(k);
    em.push(0x00);
    em.push(0x01);
    em.extend(std::iter::repeat(0xff).take(k - T_LEN - 3));
    em.push(0x00);
    em.extend_from_slice(&SHA256_DIGEST_INFO_PREFIX);
    em.extend_from_slice(hash.as_slice());
    debug_assert_eq!(em.len(), k);
    let m = BigUint::from_bytes_be(&em);
    // CRT decryption path: s = m^d mod n via p and q.
    let m1 = BigUint::mod_pow(&m, &key.dp, &key.p)?;
    let m2 = BigUint::mod_pow(&m, &key.dq, &key.q)?;
    // h = qInv * (m1 - m2) mod p, computed without underflow. `m2 < q` may
    // exceed `p` (q > p is common), so reduce it mod p first.
    let m2p = m2.rem(&key.p)?;
    let diff = m1.add(&key.p).sub(&m2p)?.rem(&key.p)?;
    let h = key.qinv.mul(&diff).rem(&key.p)?;
    let s = m2.add(&key.q.mul(&h));
    s.to_bytes_be_padded(k)
}

/// Sign `message` and return the base64-encoded signature.
pub fn rsa_sha256_sign_base64(private_key_pem: &str, message: &[u8]) -> Result<String, String> {
    Ok(base64_encode(&rsa_sha256_sign(
        private_key_pem,
        message,
    )?))
}

/// Verify a PKCS#1 v1.5 + SHA-256 signature. Returns `Ok(false)` for a
/// well-formed but non-matching signature; `Err` for undecodable keys.
pub fn rsa_sha256_verify(
    public_key_pem: &str,
    message: &[u8],
    signature: &[u8],
) -> Result<bool, String> {
    let key = parse_public_key_pem(public_key_pem)?;
    let k = (key.n.bit_len() + 7) / 8;
    if signature.len() != k {
        return Ok(false);
    }
    let s = BigUint::from_bytes_be(signature);
    let v = BigUint::mod_pow(&s, &key.e, &key.n)?;
    let em = v.to_bytes_be_padded(k)?;
    // Expect 0x00 || 0x01 || PS (>= 8 bytes of 0xff) || 0x00 || DigestInfo || H.
    if em.len() != k || em[0] != 0x00 || em[1] != 0x01 {
        return Ok(false);
    }
    let mut i = 2;
    while i < em.len() && em[i] == 0xff {
        i += 1;
    }
    if i - 2 < 8 || i >= em.len() || em[i] != 0x00 {
        return Ok(false);
    }
    let t = &em[i + 1..];
    if t.len() != SHA256_DIGEST_INFO_PREFIX.len() + 32 {
        return Ok(false);
    }
    if t[..SHA256_DIGEST_INFO_PREFIX.len()] != SHA256_DIGEST_INFO_PREFIX {
        return Ok(false);
    }
    let hash = Sha256::digest(message);
    Ok(t[SHA256_DIGEST_INFO_PREFIX.len()..] == hash[..])
}

// ---------------------------------------------------------------------------
// Digest header (RFC 9530 style "SHA-256=<base64>" used by Misskey/Sharkey)
// ---------------------------------------------------------------------------

/// Compute the `Digest` header value `"SHA-256=<base64(sha256(body))>"`.
pub fn compute_digest_header(body: &[u8]) -> String {
    format!("SHA-256={}", base64_encode(Sha256::digest(body).as_slice()))
}

/// Verify a `Digest` header value against `body`. The algorithm name match is
/// case-insensitive and other comma-separated digests are ignored.
pub fn verify_digest_header(header_value: &str, body: &[u8]) -> bool {
    let expected = Sha256::digest(body);
    for part in header_value.split(',') {
        let part = part.trim();
        let Some((algo, value)) = part.split_once('=') else {
            continue;
        };
        if !algo.trim().eq_ignore_ascii_case("SHA-256") {
            continue;
        }
        let value = value.trim().trim_matches('"');
        let Ok(raw) = base64_decode(value) else {
            continue;
        };
        if raw.as_slice() == expected.as_slice() {
            return true;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Legacy HTTP Signatures (draft-cavage-http-signatures)
// ---------------------------------------------------------------------------

/// Headers produced by [`sign_request_headers`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedRequestHeaders {
    pub date: String,
    pub digest: Option<String>,
    /// Full value for the `Signature` header.
    pub signature: String,
}

/// Parsed `Signature` header (or `Authorization: Signature ...`) value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedSignature {
    pub key_id: String,
    pub algorithm: String,
    pub headers: Vec<String>,
    pub signature: Vec<u8>,
}

/// Sign an outgoing request the way Misskey/Sharkey `ApRequestService` does:
/// POST (or any call with `body`) signs
/// `(request-target) date host digest`; GET (no body) signs
/// `(request-target) date host`.
pub fn sign_request_headers(
    method: &str,
    path_and_query: &str,
    host: &str,
    date: &str,
    body: Option<&[u8]>,
    key_id: &str,
    private_key_pem: &str,
) -> Result<SignedRequestHeaders, String> {
    let mut lines = vec![
        format!("(request-target): {} {}", method.to_lowercase(), path_and_query),
        format!("date: {date}"),
        format!("host: {host}"),
    ];
    let mut header_names = "(request-target) date host".to_string();
    let digest = if let Some(b) = body {
        let d = compute_digest_header(b);
        lines.push(format!("digest: {d}"));
        header_names.push_str(" digest");
        Some(d)
    } else {
        None
    };
    let signing_string = lines.join("\n");
    let sig_b64 = rsa_sha256_sign_base64(private_key_pem, signing_string.as_bytes())?;
    let signature = format!(
        "keyId=\"{key_id}\",algorithm=\"rsa-sha256\",headers=\"{header_names}\",signature=\"{sig_b64}\""
    );
    Ok(SignedRequestHeaders {
        date: date.to_string(),
        digest,
        signature,
    })
}

/// Parse a `Signature` header value (also accepts an
/// `Authorization: Signature ...` value).
pub fn parse_signature_header(header_value: &str) -> Result<ParsedSignature, String> {
    let mut v = header_value.trim();
    // Tolerate an `Authorization: Signature ...` value.
    if v.len() >= 9 && v[..9].eq_ignore_ascii_case("signature")
        && (v.len() == 9 || v.as_bytes()[9].is_ascii_whitespace())
    {
        v = v[9..].trim_start();
    }
    let mut key_id: Option<String> = None;
    let mut algorithm = "rsa-sha256".to_string();
    let mut headers: Option<Vec<String>> = None;
    let mut signature: Option<Vec<u8>> = None;
    for part in split_quoted_commas(v) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (k, val) = part
            .split_once('=')
            .ok_or_else(|| format!("malformed signature param: {part}"))?;
        let val = val.trim().trim_matches('"').to_string();
        match k.trim().to_lowercase().as_str() {
            "keyid" => key_id = Some(val),
            "algorithm" => algorithm = val.to_lowercase(),
            "headers" => {
                headers = Some(val.split_whitespace().map(|s| s.to_string()).collect());
            }
            "signature" => signature = Some(base64_decode(&val)?),
            _ => {}
        }
    }
    if algorithm != "rsa-sha256" && algorithm != "hs2019" {
        return Err(format!("unsupported signature algorithm: {algorithm}"));
    }
    Ok(ParsedSignature {
        key_id: key_id.ok_or("signature missing keyId")?,
        algorithm,
        headers: headers.unwrap_or_else(|| vec!["date".to_string()]),
        signature: signature.ok_or("signature missing signature")?,
    })
}

fn split_quoted_commas(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    for c in s.chars() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                cur.push(c);
            }
            ',' if !in_quotes => {
                parts.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
    }
    parts.push(cur);
    parts
}

/// Map `{actor}#main-key` and `{actor}/publickey` back to `{actor}`.
pub fn actor_uri_from_key_id(key_id: &str) -> String {
    if let Some((actor, _)) = key_id.split_once('#') {
        return actor.to_string();
    }
    if let Some(actor) = key_id.strip_suffix("/publickey") {
        return actor.to_string();
    }
    key_id.to_string()
}

fn header_value(headers: &HeaderMap, name: &str) -> Result<String, String> {
    let hn = HeaderName::from_str(&name.to_lowercase())
        .map_err(|_| format!("invalid header name: {name}"))?;
    let v = headers
        .get(&hn)
        .ok_or_else(|| format!("missing header: {name}"))?;
    v.to_str()
        .map(|s| s.to_string())
        .map_err(|_| format!("non-visible header value: {name}"))
}

/// Build the signing string: lowercase `name: value` lines joined by `\n`,
/// with `(request-target): {method} {path_and_query}`.
pub fn build_signing_string(
    method: &str,
    path_and_query: &str,
    headers: &HeaderMap,
    signed_header_names: &[String],
) -> Result<String, String> {
    let mut lines = Vec::with_capacity(signed_header_names.len());
    for name in signed_header_names {
        if name.eq_ignore_ascii_case("(request-target)") {
            lines.push(format!(
                "(request-target): {} {}",
                method.to_lowercase(),
                path_and_query
            ));
        } else {
            lines.push(format!("{}: {}", name.to_lowercase(), header_value(headers, name)?));
        }
    }
    Ok(lines.join("\n"))
}

/// Parse an HTTP date: RFC 2822 first, then RFC 7231 IMF-fixdate.
/// Returns milliseconds since the Unix epoch.
pub fn parse_http_date(s: &str) -> Result<i64, String> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(s) {
        return Ok(dt.timestamp_millis());
    }
    if let Ok(naive) =
        chrono::NaiveDateTime::parse_from_str(s, "%a, %d %b %Y %H:%M:%S GMT")
    {
        return Ok(naive.and_utc().timestamp_millis());
    }
    Err(format!("unparseable Date header: {s}"))
}

/// Verify an incoming signed request.
///
/// Requires `(request-target)`, `host` and `date` in the signed header list
/// (plus `digest` when `body` is present), validates the `Digest` header
/// against `body`, checks `Date` skew when `now_ms` is `Some`, and verifies
/// the RSA-SHA256 signature over the rebuilt signing string.
pub fn verify_http_signature(
    method: &str,
    path_and_query: &str,
    headers: &HeaderMap,
    body: Option<&[u8]>,
    public_key_pem: &str,
    now_ms: Option<i64>,
    max_clock_skew_secs: i64,
) -> Result<ParsedSignature, String> {
    let raw = headers
        .get("signature")
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
        })
        .ok_or("missing Signature header")?;
    let parsed = parse_signature_header(raw)?;
    let names: Vec<String> = parsed.headers.iter().map(|h| h.to_lowercase()).collect();
    for required in ["(request-target)", "host", "date"] {
        if !names.iter().any(|h| h == required) {
            return Err(format!("signed headers missing required {required}"));
        }
    }
    if body.is_some() && !names.iter().any(|h| h == "digest") {
        return Err("signed headers missing required digest".to_string());
    }
    if let Some(b) = body {
        let digest = header_value(headers, "digest")?;
        if !verify_digest_header(&digest, b) {
            return Err("Digest header does not match body".to_string());
        }
    }
    let date_str = header_value(headers, "date")?;
    let date_ms = parse_http_date(&date_str)?;
    if let Some(now) = now_ms {
        if (now - date_ms).abs() > max_clock_skew_secs * 1000 {
            return Err("Date header outside allowed clock skew".to_string());
        }
    }
    let signing_string = build_signing_string(method, path_and_query, headers, &parsed.headers)?;
    if !rsa_sha256_verify(public_key_pem, signing_string.as_bytes(), &parsed.signature)? {
        return Err("HTTP signature verification failed".to_string());
    }
    Ok(parsed)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn u(n: u128) -> BigUint {
        BigUint::from_bytes_be(&n.to_be_bytes())
    }

    fn from_u128_native(n: u128) -> BigUint {
        u(n)
    }

    fn to_u128_native(v: &BigUint) -> u128 {
        let b = v.to_bytes_be();
        assert!(b.len() <= 16);
        let mut padded = [0u8; 16];
        padded[16 - b.len()..].copy_from_slice(&b);
        u128::from_be_bytes(padded)
    }

    // Tiny deterministic PRNG (xorshift64*) for reproducible tests.
    fn xorshift_fill(seed: u64) -> impl FnMut(&mut [u8]) {
        let mut x = if seed == 0 {
            0x9e3779b97f4a7c15
        } else {
            seed
        };
        move |dest: &mut [u8]| {
            let mut i = 0;
            while i < dest.len() {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                if x == 0 {
                    x = 0x2545f4914f6cdd1d;
                }
                let r = x.to_le_bytes();
                let n = (dest.len() - i).min(8);
                dest[i..i + n].copy_from_slice(&r[..n]);
                i += n;
            }
        }
    }

    fn lcg_next(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *state >> 11
    }

    #[test]
    fn biguint_bytes_roundtrip() {
        for v in [0u128, 1, 255, 256, 65535, u128::MAX, 123456789012345678901234567890] {
            assert_eq!(to_u128_native(&from_u128_native(v)), v);
        }
        assert!(BigUint::zero().to_bytes_be().is_empty());
        assert_eq!(BigUint::from_bytes_be(&[]), BigUint::zero());
        assert_eq!(BigUint::from_bytes_be(&[0, 0, 1]), BigUint::one());
    }

    #[test]
    fn biguint_add_sub_mul_known_answers() {
        // 0xffffffff + 1 = 0x100000000
        let a = BigUint::from_u32(u32::MAX);
        assert_eq!(a.add_small(1), BigUint::from_u64(0x1_0000_0000));
        // Classic: 123456789 * 987654321 = 121932631112635269
        let p = u(123456789).mul(&u(987654321));
        assert_eq!(to_u128_native(&p), 121932631112635269);
        // Subtraction roundtrip + underflow error.
        assert_eq!(
            to_u128_native(&p.sub(&u(123456789)).unwrap()),
            121932631112635269 - 123456789
        );
        assert_eq!(
            to_u128_native(&p.sub(&u(987654321)).unwrap()),
            121932631112635269 - 987654321
        );
        assert!(u(5).sub(&u(6)).is_err());
        // Shifts.
        assert_eq!(u(1).shl(64), u(1u128 << 64));
        assert_eq!(u(1u128 << 64).shr(64), u(1));
        assert_eq!(u(0xffff).shr(200), BigUint::zero());
        assert_eq!(u(1).bit_len(), 1);
        assert_eq!(u(0).bit_len(), 0);
        assert_eq!(u(255).bit_len(), 8);
        assert_eq!(u(256).bit_len(), 9);
    }

    #[test]
    fn biguint_div_rem_known_answers() {
        // Exact division.
        let (q, r) = BigUint::div_rem(&u(121932631112635269), &u(987654321)).unwrap();
        assert_eq!(to_u128_native(&q), 123456789);
        assert!(r.is_zero());
        // Division with remainder: 10^20 = 810000007 * 123456789 + 37854249?
        // Checked via invariant instead of hand arithmetic for the big case.
        let (q, r) = BigUint::div_rem(&u(100000000000000000000), &u(123456789)).unwrap();
        assert_eq!(q.mul(&u(123456789)).add(&r), u(100000000000000000000));
        assert!(r < u(123456789));
        assert_eq!(to_u128_native(&q), 100000000000000000000u128 / 123456789);
        assert_eq!(to_u128_native(&r), 100000000000000000000u128 % 123456789);
        // Divisor larger than dividend.
        let (q, r) = BigUint::div_rem(&u(5), &u(100)).unwrap();
        assert!(q.is_zero());
        assert_eq!(to_u128_native(&r), 5);
        assert!(BigUint::div_rem(&u(1), &BigUint::zero()).is_err());
    }

    #[test]
    fn biguint_div_rem_multi_limb_against_u128() {
        // Cross-check Knuth division against native u128 on pseudo-random values.
        let mut state = 0x12345678abcdef01u64;
        for _ in 0..300 {
            let hi = lcg_next(&mut state) as u128;
            let lo = lcg_next(&mut state) as u128;
            let a = from_u128_native((hi << 64) | lo | 1);
            let b = from_u128_native(lcg_next(&mut state) as u128 | 1);
            let (q, r) = BigUint::div_rem(&a, &b).unwrap();
            let an = to_u128_native(&a);
            let bn = to_u128_native(&b);
            assert_eq!(to_u128_native(&q), an / bn);
            assert_eq!(to_u128_native(&r), an % bn);
            // mul/add cross-check too.
            assert_eq!(to_u128_native(&a.add(&b)), an.wrapping_add(bn));
            let (x, y) = if an >= bn { (an, bn) } else { (bn, an) };
            assert_eq!(
                to_u128_native(
                    &from_u128_native(x).sub(&from_u128_native(y)).unwrap()
                ),
                x - y
            );
        }
    }

    #[test]
    fn biguint_mod_pow_and_inverse_known_answers() {
        // 4^13 mod 497 = 445 (textbook).
        let r = BigUint::mod_pow(&u(4), &u(13), &u(497)).unwrap();
        assert_eq!(to_u128_native(&r), 445);
        assert_eq!(to_u128_native(&BigUint::mod_pow(&u(2), &u(10), &u(1000)).unwrap()), 24);
        // 17^-1 mod 3120 = 2753 (textbook RSA).
        let inv = BigUint::mod_inverse(&u(17), &u(3120)).unwrap();
        assert_eq!(to_u128_native(&inv), 2753);
        // Fermat spot-check: 7^100 mod 101 = 1.
        assert!(BigUint::mod_pow(&u(7), &u(100), &u(101)).unwrap().is_one());
        assert!(BigUint::mod_inverse(&u(6), &u(9)).is_err());
    }

    fn textbook_key() -> RsaPrivateKey {
        // p=61, q=53, e=17, d=2753 with CRT params derived by BigUint itself.
        let p = u(61);
        let q = u(53);
        let n = p.mul(&q);
        assert_eq!(to_u128_native(&n), 3233);
        let e = u(17);
        let d = BigUint::mod_inverse(&e, &u(60 * 52)).unwrap();
        assert_eq!(to_u128_native(&d), 2753);
        let dp = d.rem(&u(60)).unwrap();
        let dq = d.rem(&u(52)).unwrap();
        let qinv = BigUint::mod_inverse(&q, &p).unwrap();
        assert_eq!(to_u128_native(&dp), 53);
        assert_eq!(to_u128_native(&dq), 49);
        assert_eq!(to_u128_native(&qinv), 38);
        RsaPrivateKey {
            n,
            e,
            d,
            p,
            q,
            dp,
            dq,
            qinv,
        }
    }

    #[test]
    fn textbook_key_der_roundtrip() {
        // Hardcoded known-answer fixture for the ASN.1 codec (no external tools).
        let key = textbook_key();
        let pkcs1 = encode_pkcs1_private_der(&key);
        let back = parse_pkcs1_private_der(&pkcs1).unwrap();
        assert_eq!(back, key);
        let pkcs8 = encode_pkcs8_private_der(&key);
        assert_eq!(parse_pkcs8_private_der(&pkcs8).unwrap(), key);
        let pub_key = RsaPublicKey {
            n: key.n.clone(),
            e: key.e.clone(),
        };
        assert_eq!(parse_rsa_public_der(&encode_rsa_public_der(&pub_key)).unwrap(), pub_key);
        assert_eq!(parse_spki_der(&encode_spki_der(&pub_key)).unwrap(), pub_key);
        // PEM wrappers roundtrip for both labels.
        let spki_pem = public_key_to_spki_pem(&pub_key);
        assert!(spki_pem.starts_with("-----BEGIN PUBLIC KEY-----\n"));
        assert_eq!(parse_public_key_pem(&spki_pem).unwrap(), pub_key);
        let raw_pub_pem = pem_encode("RSA PUBLIC KEY", &encode_rsa_public_der(&pub_key));
        assert_eq!(parse_public_key_pem(&raw_pub_pem).unwrap(), pub_key);
        let pkcs8_pem = private_key_to_pkcs8_pem(&key);
        assert_eq!(parse_private_key_pem(&pkcs8_pem).unwrap(), key);
        let pkcs1_pem = private_key_to_pkcs1_pem(&key);
        assert_eq!(parse_private_key_pem(&pkcs1_pem).unwrap(), key);
        assert!(parse_public_key_pem(&pkcs8_pem).is_err());
        assert!(parse_private_key_pem(&spki_pem).is_err());
    }

    /// Regression: CRT signing underflowed (`m1 + p - m2`) for keys with
    /// `q > p` whenever `m2 > m1 + p`, failing a few percent of signatures.
    #[test]
    fn sign_verify_with_q_larger_than_p() {
        let mut seed = 1u64;
        let key = loop {
            let mut fill = xorshift_fill(seed);
            let k = generate_rsa_keypair_with_fill(512, &mut fill).unwrap();
            if k.q > k.p.add(&k.p.shr(3)) {
                break k;
            }
            seed += 1;
        };
        let priv_pem = private_key_to_pkcs8_pem(&key);
        let pub_pem = public_key_to_spki_pem(&RsaPublicKey {
            n: key.n.clone(),
            e: key.e.clone(),
        });
        for i in 0..200 {
            let msg = format!("message {i}");
            let sig = rsa_sha256_sign(&priv_pem, msg.as_bytes()).expect("sign must not underflow");
            assert!(rsa_sha256_verify(&pub_pem, msg.as_bytes(), &sig).unwrap());
        }
    }

    #[test]
    fn deterministic_keygen_is_stable() {
        let mut f1 = xorshift_fill(42);
        let k1 = generate_rsa_keypair_with_fill(512, &mut f1).unwrap();
        let mut f2 = xorshift_fill(42);
        let k2 = generate_rsa_keypair_with_fill(512, &mut f2).unwrap();
        assert_eq!(k1, k2);
        assert_eq!(k1.n.bit_len(), 512);
        // d*e = 1 mod lambda-ish check via phi.
        let phi = k1.p.sub(&u(1)).unwrap().mul(&k1.q.sub(&u(1)).unwrap());
        let check = k1.d.mul(&k1.e).rem(&phi).unwrap();
        assert!(check.is_one());
    }

    #[test]
    fn keygen_1024_pem_roundtrip_and_sign_verify() {
        let (priv_key, pub_key) = generate_rsa_keypair(1024).unwrap();
        assert_eq!(priv_key.n.bit_len(), 1024);
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let priv_back = parse_private_key_pem(&priv_pem).unwrap();
        let pub_back = parse_public_key_pem(&pub_pem).unwrap();
        assert_eq!(priv_back, priv_key);
        assert_eq!(pub_back, pub_key);
        let msg = b"hello federation";
        let sig = rsa_sha256_sign(&priv_pem, msg).unwrap();
        assert_eq!(sig.len(), 128);
        assert!(rsa_sha256_verify(&pub_pem, msg, &sig).unwrap());
        // Tampered message / signature / wrong key all fail.
        assert!(!rsa_sha256_verify(&pub_pem, b"hello federatioN", &sig).unwrap());
        let mut bad_sig = sig.clone();
        bad_sig[10] ^= 0x01;
        assert!(!rsa_sha256_verify(&pub_pem, msg, &bad_sig).unwrap());
        assert!(!rsa_sha256_verify(&pub_pem, msg, &sig[..64]).unwrap());
        let (_, other_pub) = generate_rsa_keypair(1024).unwrap();
        let other_pem = public_key_to_spki_pem(&other_pub);
        assert!(!rsa_sha256_verify(&other_pem, msg, &sig).unwrap());
        // Base64 helper agrees.
        assert_eq!(rsa_sha256_sign_base64(&priv_pem, msg).unwrap(), base64_encode(&sig));
    }

    #[test]
    fn base64_known_answers() {
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_decode("aGVsbG8=").unwrap(), b"hello");
        assert_eq!(base64_decode("aGVs\nbG8=").unwrap(), b"hello");
        assert_eq!(base64_decode("").unwrap(), b"");
        assert!(base64_decode("aGVsbG8").is_err());
        assert!(base64_decode("aGVsbG8!").is_err());
        // Roundtrip incl. all byte values.
        let all: Vec<u8> = (0..=255).collect();
        assert_eq!(base64_decode(&base64_encode(&all)).unwrap(), all);
    }

    #[test]
    fn digest_header_roundtrip() {
        let body = br#"{"type":"Create"}"#;
        let header = compute_digest_header(body);
        assert!(header.starts_with("SHA-256="));
        assert!(verify_digest_header(&header, body));
        assert!(!verify_digest_header(&header, b"tampered"));
        // Case-insensitive algorithm + extra digests tolerated.
        assert!(verify_digest_header(
            &format!("md5=AAAA, {} ", header.replace("SHA-256", "sha-256")),
            body
        ));
        assert!(!verify_digest_header("SHA-256=AAAA", body));
        assert!(!verify_digest_header("garbage", body));
    }

    fn test_headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.insert(
                HeaderName::from_str(k).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        map
    }

    fn date_fixture() -> (&'static str, i64) {
        // 2026-10-14 is a Wednesday; toUTCString form (IMF-fixdate).
        let s = "Wed, 14 Oct 2026 12:00:00 GMT";
        (s, parse_http_date(s).unwrap())
    }

    #[test]
    fn http_get_sign_and_verify() {
        let (priv_key, pub_key) = generate_rsa_keypair(1024).unwrap();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let (date, now) = date_fixture();
        let key_id = "https://example.local/users/123#main-key";
        let signed = sign_request_headers(
            "GET",
            "/users/123",
            "example.local",
            date,
            None,
            key_id,
            &priv_pem,
        )
        .unwrap();
        assert!(signed.digest.is_none());
        assert!(signed.signature.contains("headers=\"(request-target) date host\""));
        let headers = test_headers(&[
            ("date", date),
            ("host", "example.local"),
            ("signature", &signed.signature),
        ]);
        let parsed = verify_http_signature(
            "get",
            "/users/123",
            &headers,
            None,
            &pub_pem,
            Some(now + 60_000),
            300,
        )
        .unwrap();
        assert_eq!(parsed.key_id, key_id);
        assert_eq!(actor_uri_from_key_id(&parsed.key_id), "https://example.local/users/123");
        // Tampered path fails.
        assert!(verify_http_signature(
            "get",
            "/users/124",
            &headers,
            None,
            &pub_pem,
            Some(now),
            300
        )
        .is_err());
        // Tampered host header value fails.
        let bad = test_headers(&[
            ("date", date),
            ("host", "evil.example"),
            ("signature", &signed.signature),
        ]);
        assert!(verify_http_signature("get", "/users/123", &bad, None, &pub_pem, Some(now), 300).is_err());
        // Wrong public key fails.
        let (_, other) = generate_rsa_keypair(1024).unwrap();
        let other_pem = public_key_to_spki_pem(&other);
        assert!(verify_http_signature(
            "get",
            "/users/123",
            &headers,
            None,
            &other_pem,
            Some(now),
            300
        )
        .is_err());
        // Expired Date fails; missing skew check passes.
        assert!(verify_http_signature(
            "get",
            "/users/123",
            &headers,
            None,
            &pub_pem,
            Some(now + 3600_000),
            300
        )
        .is_err());
        assert!(verify_http_signature("get", "/users/123", &headers, None, &pub_pem, None, 300).is_ok());
    }

    #[test]
    fn http_post_sign_and_verify() {
        let (priv_key, pub_key) = generate_rsa_keypair(1024).unwrap();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let (date, now) = date_fixture();
        let body = br#"{"type":"Follow"}"#;
        let key_id = "https://example.local/users/123/publickey";
        let signed = sign_request_headers(
            "POST",
            "/users/456/inbox",
            "example.local",
            date,
            Some(body),
            key_id,
            &priv_pem,
        )
        .unwrap();
        let digest = signed.digest.clone().unwrap();
        assert!(signed.signature.contains("headers=\"(request-target) date host digest\""));
        assert!(verify_digest_header(&digest, body));
        let headers = test_headers(&[
            ("date", date),
            ("host", "example.local"),
            ("digest", &digest),
            ("signature", &signed.signature),
        ]);
        let parsed = verify_http_signature(
            "POST",
            "/users/456/inbox",
            &headers,
            Some(body),
            &pub_pem,
            Some(now),
            300,
        )
        .unwrap();
        assert_eq!(actor_uri_from_key_id(&parsed.key_id), "https://example.local/users/123");
        // Tampered body: Digest mismatch fails.
        let tampered = br#"{"type":"Follow","tampered":true}"#;
        assert!(verify_http_signature(
            "POST",
            "/users/456/inbox",
            &headers,
            Some(tampered),
            &pub_pem,
            Some(now),
            300
        )
        .is_err());
        // GET-style signature (no digest covered) rejected for POST with body.
        let get_signed = sign_request_headers(
            "POST",
            "/users/456/inbox",
            "example.local",
            date,
            None,
            key_id,
            &priv_pem,
        )
        .unwrap();
        let get_headers = test_headers(&[
            ("date", date),
            ("host", "example.local"),
            ("signature", &get_signed.signature),
        ]);
        assert!(verify_http_signature(
            "POST",
            "/users/456/inbox",
            &get_headers,
            Some(body),
            &pub_pem,
            Some(now),
            300
        )
        .is_err());
    }

    #[test]
    fn signature_header_parsing_variants() {
        let (priv_key, _) = generate_rsa_keypair(1024).unwrap();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let signed = sign_request_headers(
            "GET",
            "/x",
            "example.local",
            "Wed, 14 Oct 2026 12:00:00 GMT",
            None,
            "https://example.local/users/1#main-key",
            &priv_pem,
        )
        .unwrap();
        // Raw Signature value.
        let p = parse_signature_header(&signed.signature).unwrap();
        assert_eq!(p.algorithm, "rsa-sha256");
        assert_eq!(p.headers, vec!["(request-target)", "date", "host"]);
        // Authorization: Signature ... prefix.
        let p2 = parse_signature_header(&format!("Signature {}", signed.signature)).unwrap();
        assert_eq!(p2, p);
        // hs2019 accepted, unknown rejected.
        assert!(parse_signature_header("keyId=\"a\",algorithm=\"hs2019\",signature=\"AAA=\"").is_ok());
        assert!(parse_signature_header("keyId=\"a\",algorithm=\"hmac-sha256\",signature=\"AAA=\"").is_err());
        // headers omitted defaults to date.
        let p3 = parse_signature_header("keyId=\"a\",signature=\"AAA=\"").unwrap();
        assert_eq!(p3.headers, vec!["date"]);
        assert!(parse_signature_header("algorithm=\"rsa-sha256\",signature=\"AAA=\"").is_err());
        // actor URIs.
        assert_eq!(
            actor_uri_from_key_id("https://example.local/users/123#main-key"),
            "https://example.local/users/123"
        );
        assert_eq!(
            actor_uri_from_key_id("https://example.local/users/123/publickey"),
            "https://example.local/users/123"
        );
    }

    #[test]
    fn verify_rejects_missing_required_signed_headers() {
        let (priv_key, pub_key) = generate_rsa_keypair(1024).unwrap();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let (date, now) = date_fixture();
        // Craft a signature covering only (request-target) + date.
        let signing_string = format!("(request-target): get /users/123\ndate: {date}");
        let sig = rsa_sha256_sign_base64(&priv_pem, signing_string.as_bytes()).unwrap();
        let value = format!(
            "keyId=\"https://example.local/users/1#main-key\",algorithm=\"rsa-sha256\",headers=\"(request-target) date\",signature=\"{sig}\""
        );
        let headers = test_headers(&[
            ("date", date),
            ("host", "example.local"),
            ("signature", &value),
        ]);
        assert!(verify_http_signature("get", "/users/123", &headers, None, &pub_pem, Some(now), 300).is_err());
        // Unparseable Date fails (all required headers signed, so the
        // failure comes from date parsing, not the header gate).
        let full = sign_request_headers(
            "GET",
            "/users/123",
            "example.local",
            date,
            None,
            "https://example.local/users/1#main-key",
            &priv_pem,
        )
        .unwrap();
        let bad_date = test_headers(&[
            ("date", "not a date"),
            ("host", "example.local"),
            ("signature", &full.signature),
        ]);
        assert!(verify_http_signature("get", "/users/123", &bad_date, None, &pub_pem, Some(now), 300).is_err());
    }

    #[test]
    fn node_crypto_rsa2048_fixture_interop() {
        let node_pub_pem = "-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtB8BOMPCFjO+KBuZFVM+\n\
UlZ98+5yiaTRFJYTMzNHtP+J1jphiTe6VRLLTY92mVM+p8ziIz7hklh7ta/Y+7Hr\n\
EfOksEZG+7dvSopGJf9YBXmCeYNGOjAEjYLhx+I88CQsw3Xvu/nnzXJDOQe07hWm\n\
vF7E1AyimnOVi3bjQNrqyCQ35U9hDJZFUAfc8oHeYjqdu7N/jy0VYt+U+vCQod8a\n\
z+Fufip7bv/hSnVBLOUb61zd44ksDf1rpiA3Q617oYh5/18U5jycaTbxeWU/iNfM\n\
HVN0N9bZ8OpLCRlVWl4RN5uYhxbjrovKswWWlHX8kC/7nkp3NXHo6VY5FEVtocVH\n\
eQIDAQAB\n\
-----END PUBLIC KEY-----\n";
        let node_priv_pem = "-----BEGIN PRIVATE KEY-----\n\
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC0HwE4w8IWM74o\n\
G5kVUz5SVn3z7nKJpNEUlhMzM0e0/4nWOmGJN7pVEstNj3aZUz6nzOIjPuGSWHu1\n\
r9j7sesR86SwRkb7t29KikYl/1gFeYJ5g0Y6MASNguHH4jzwJCzDde+7+efNckM5\n\
B7TuFaa8XsTUDKKac5WLduNA2urIJDflT2EMlkVQB9zygd5iOp27s3+PLRVi35T6\n\
8JCh3xrP4W5+Kntu/+FKdUEs5RvrXN3jiSwN/WumIDdDrXuhiHn/XxTmPJxpNvF5\n\
ZT+I18wdU3Q31tnw6ksJGVVaXhE3m5iHFuOui8qzBZaUdfyQL/ueSnc1cejpVjkU\n\
RW2hxUd5AgMBAAECggEASXhMG0deJ4g7jEiVjKlDxnoZLiKY/aQI1IjSlBPar+17\n\
37I4jqv4iTL9UkmuaqQV7oQXyaCgif0lmNp9e0z7QXtMNcq0sYAFGTaFnoIk+Taz\n\
DbWKM7naYY59YfbaPGnFCIVWoCozbw2odd664lJajINrbcMpoC59VUHNOGsTUUcl\n\
Zu6XJb41vJR984iBWVl3H4IBQ0fq8omwSdDfnVesTUFAI5+UWbVsFSkR2GxeafeW\n\
wCcxb/HZGmHdZA0r+wcE5DHlvKWEuNyTLeaL+5Ls4i4tF1u5M4VObR6p8pOXCuDr\n\
bhlrMC29ppcytPA6gp3rfYjQB3KGSCez85QOZEoiMQKBgQDfFkkAYRDJXOFmiKMI\n\
5Es9iEKNDRtnJGZHsY7EUaY0KjwARnf0B107GOcpDP/rAvDcE1AgrH8WWp2U/F7c\n\
Tn0ivFoC71mcgqDW8CpxWm6gFfJxIDcQHab4pCePZTSokCGvQHkQ8SCF9CiYPF8e\n\
N36OYIfCcqzw10S61wdoneWpywKBgQDOsfNbnEM4MqP3slJ4xfZEMJiCmn9A0XLN\n\
FKlg64WwZYuPf0tVGLUsFk7v2bxKjPdIB/CN0k/bmBgbTsCVNaxN2JQgCWVN1Lkn\n\
RK74TDk1U54AoEEg5wFDboKxwila4O+QzEv9Kvc4wKfP6m3UKwcsjCHOJ8IeKNdr\n\
g6/sV7h7SwKBgQDMcmLyuwIkWD+ltSEc/APMX30GQXXUn7IwkBPT7TQRaIdWhT5X\n\
YQxbJaZro6NCII7WUcjEOPG7bc4vBN52rmPWGqGvQOVyHvECKlXrTkJF6fcnH6v2\n\
1AX6po4ux0RsGrMUJlUNZr40pR8y7VkKmzcbinHAjVHa4p31US1T/ijE4wKBgBsK\n\
kOoFUY4Cogz/2WZPj95v9SPxDoDuZfSKpXY2mfDv+Ag4ASUK7HjuGjmq5MVkF93m\n\
nAQR5by280BvVyauNPKiSQ6/6+74y5hllfaKEln1rJAsZxjk3/dwd8zFuB9Zmj35\n\
XF+sxVc5bTr4UpIVZwTe+HU+IllLwpMgUV28RDhtAoGBAKGCggYfQ1Q54vmljH1c\n\
me9QQFofDzsoq8Gj1xuhDFFNmHqW1z0DQkkf1gdKgx9E/uE4ceXTLE9z313cof8v\n\
mzqBLgLPPqB2yu9VRXgr43i6mJ3jiW6NK+tPIj7U7GNdGRd8LAmjP0W/8S6CFoYk\n\
/Xx9/OtP1rd1WHkDZbdd3gi9\n\
-----END PRIVATE KEY-----\n";
        let msg = b"(request-target): post /inbox\ndate: Wed, 14 Oct 2026 12:00:00 GMT\nhost: example.local\ndigest: SHA-256=47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";
        let expected_sig = "HsFrxxUwBWUhCpsXx9sn2QIFZin4n1iGmxhpTX/VBfjg9wZaykhLUf4W2Y1oAGGiGPiri4AqpZ+EDqg5AobkK5ygwrbuqViCr/+7tK6RC02FAJKV+5OfQpq4OGAK9WoFm6HJ6pylZZuTHXi84fVv/u1mvQIXLGWSJ1wZL9cEE+LXlqCB4uk5ZKDZQpUfcy3djn26gTXzQRaeYSzxhyNzOmv7v6eXAu5X+Tte3Vbtj3le1UUTDhbT2MpnCuefFfRXra5tjVOMi0UtBsa97HaxgDgAmPCx6vjVtVJPgXjaQVV2bTsfXKwFVKyKLyusPTkxvTTuMaTX0lixrg8xhb09XA==";

        let our_sig = rsa_sha256_sign_base64(node_priv_pem, msg).unwrap();
        assert_eq!(our_sig, expected_sig);
        let sig_bytes = base64_decode(expected_sig).unwrap();
        assert!(rsa_sha256_verify(node_pub_pem, msg, &sig_bytes).unwrap());
    }
}

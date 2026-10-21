//! Pure-Rust media processing: dimensions for stills/video containers and
//! the `small`-preview decision point, ported from
//! `birchtree/src/media/processing.rs` and adapted to pinetree.
//!
//! Adaptation notes vs birchtree:
//!
//! - pinetree's manifest has no `image`/`blurhash` crates (and the WP1 agent
//!   must keep `cargo test --offline` green without adding dependencies), so
//!   there is no pixel decode here. Dimensions come from container headers
//!   (PNG IHDR, JPEG SOF, GIF logical screen descriptor, WebP VP8/VP8L/VP8X
//!   headers, BMP DIB header, MP4 `tkhd`); `blurhash` is therefore always
//!   `None` (honest unknown — the row and packer both admit `null`).
//! - The `small` preview bytes are rendered by the ffmpeg scale pass in
//!   [`crate::media::transcode::transcode_still_with_preview`] (WebP, bound
//!   [`PREVIEW_BOUND`]); this module only decides the bound. Rows that finish
//!   on the legacy path (WebP passthrough, audio) carry no preview.
//! - HEIC/HEIF/TIFF/AVIF have no header parser here on purpose: those rows
//!   take the ffmpeg `ImageToWebp` job when transcoding is enabled, and
//!   answering `Unidentifiable` on the legacy path keeps a toolchain-less
//!   server honest (the row reports failure, surfaced as 422) instead of
//!   guessing.
//!
//! Failures are classified so the HTTP layer can reproduce Mastodon's two
//! pinned bodies: [`ProcessError::Unidentifiable`] becomes
//! `422 File type of uploaded media could not be verified` and
//! [`ProcessError::Other`] becomes
//! `500 Error processing thumbnail for uploaded media`.

/// Bounding box of the generated `small` preview (Mastodon: "resize to fit
/// 460x460").
pub const PREVIEW_BOUND: u32 = 460;

/// A processing failure, classified the way Mastodon's upload rescues are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessError {
    /// The bytes could not be identified or decoded (Paperclip's
    /// `NotIdentifiedByImageMagickError` equivalent → 422).
    Unidentifiable(String),
    /// A downstream generation step failed (Paperclip::Error equivalent →
    /// 500).
    Other(String),
}

impl std::fmt::Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessError::Unidentifiable(detail) => {
                write!(f, "unidentifiable media: {detail}")
            }
            ProcessError::Other(detail) => write!(f, "media processing failed: {detail}"),
        }
    }
}

impl std::error::Error for ProcessError {}

/// Outcome of processing an upload's original (or derived) bytes.
///
/// `preview_*` is `Some` only when a preview was actually generated (the
/// ffmpeg still path); the pure-Rust path never fabricates preview bytes.
#[derive(Debug, Default, Clone)]
pub struct Processed {
    pub width: Option<u32>,
    pub height: Option<u32>,
    /// Always `None` on the pure-Rust path (no pixel decode, no encoder).
    pub blurhash: Option<String>,
    /// Container duration in milliseconds (mp4 `tkhd` walk yields none;
    /// video duration comes from the ffmpeg probe in the transcode path).
    pub duration_ms: Option<u64>,
}

/// Process the bytes of an attachment classified as `mime_type` (the
/// *sniffed* content type, never the declared one).
///
/// HEIC/HEIF/TIFF/AVIF have no pure-Rust parser here on purpose: those rows
/// take the ffmpeg `ImageToWebp` job when transcoding is enabled, and
/// answering `Unidentifiable` on the legacy path keeps a toolchain-less
/// server honest (the row reports failure, surfaced as 422) instead of
/// guessing.
pub fn process(mime_type: &str, bytes: &[u8]) -> Result<Processed, ProcessError> {
    if mime_type.starts_with("image/") {
        return process_image(mime_type, bytes);
    }
    match mime_type {
        "video/mp4" | "audio/mp4" => {
            let (width, height) = mp4_dimensions(bytes);
            Ok(Processed {
                width,
                height,
                ..Processed::default()
            })
        }
        // Accepted containers with no metadata parser: dimensions stay
        // `None` (honest "unknown", never fabricated).
        "video/webm" | "audio/ogg" | "audio/mpeg" | "audio/wav" | "audio/flac" => {
            Ok(Processed::default())
        }
        other => Err(ProcessError::Unidentifiable(format!(
            "unsupported media type {other}"
        ))),
    }
}

/// Header-only dimensions for a still image. GIF animacy is irrelevant: the
/// logical screen descriptor covers every GIF, and GIFs never take this
/// path when transcoding is enabled anyway (`GifToWebm`).
fn process_image(mime_type: &str, bytes: &[u8]) -> Result<Processed, ProcessError> {
    let dims = match mime_type {
        "image/png" => png_dims(bytes),
        "image/jpeg" => jpeg_dims(bytes),
        "image/gif" => gif_dims(bytes),
        "image/webp" => webp_dims(bytes),
        "image/bmp" => bmp_dims(bytes),
        _ => None,
    };
    match dims {
        Some((width, height)) => Ok(Processed {
            width: Some(width),
            height: Some(height),
            ..Processed::default()
        }),
        None if matches!(mime_type, "image/png" | "image/jpeg" | "image/gif" | "image/webp" | "image/bmp") => {
            Err(ProcessError::Unidentifiable(format!(
                "could not decode {mime_type}"
            )))
        }
        None => Err(ProcessError::Unidentifiable(format!(
            "unsupported media type {mime_type}"
        ))),
    }
}

/// PNG: 8-byte signature + IHDR `width`/`height` (big-endian u32 at +16).
fn png_dims(bytes: &[u8]) -> Option<(u32, u32)> {
    const SIG: &[u8] = &[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    if bytes.len() < 24 || !bytes.starts_with(SIG) {
        return None;
    }
    if &bytes[12..16] != b"IHDR" {
        return None;
    }
    let width = u32::from_be_bytes(bytes[16..20].try_into().ok()?);
    let height = u32::from_be_bytes(bytes[20..24].try_into().ok()?);
    (width > 0 && height > 0).then_some((width, height))
}

/// JPEG: scan markers for a SOF segment carrying `height`/`width`
/// (big-endian u16, SOF0–SOF3/SOF5–SOF15 except DHT/DAC/DNL).
fn jpeg_dims(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 4 || bytes[0] != 0xFF || bytes[1] != 0xD8 {
        return None;
    }
    let mut i = 2;
    while i + 1 < bytes.len() {
        if bytes[i] != 0xFF {
            return None;
        }
        let marker = bytes[i + 1];
        // Standalone markers (no length field).
        if marker == 0x01 || (0xD0..=0xD9).contains(&marker) {
            i += 2;
            continue;
        }
        if i + 3 >= bytes.len() {
            return None;
        }
        let seg_len = u16::from_be_bytes([bytes[i + 2], bytes[i + 3]]) as usize;
        if seg_len < 2 || i + 2 + seg_len > bytes.len() {
            return None;
        }
        // SOF0–SOF15 except C4 (DHT), C8 (JPG), CC (DAC).
        if (0xC0..=0xCF).contains(&marker) && ![0xC4, 0xC8, 0xCC].contains(&marker) {
            if seg_len >= 7 {
                let height = u16::from_be_bytes([bytes[i + 5], bytes[i + 6]]) as u32;
                let width = u16::from_be_bytes([bytes[i + 7], bytes[i + 8]]) as u32;
                if width > 0 && height > 0 {
                    return Some((width, height));
                }
                return None;
            }
            return None;
        }
        i += 2 + seg_len;
    }
    None
}

/// GIF: 6-byte magic + logical screen descriptor (LE u16 pair at +6).
fn gif_dims(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 10 || (!bytes.starts_with(b"GIF87a") && !bytes.starts_with(b"GIF89a")) {
        return None;
    }
    let width = u16::from_le_bytes([bytes[6], bytes[7]]) as u32;
    let height = u16::from_le_bytes([bytes[8], bytes[9]]) as u32;
    (width > 0 && height > 0).then_some((width, height))
}

/// WebP: RIFF/WEBP + VP8 (`9D 01 2A` start code, 14-bit dims at +26),
/// VP8L (`2F` signature byte, 14-bit dims at +21) or VP8X (24-bit canvas at
/// +24, stored minus one).
fn webp_dims(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 30 || !bytes.starts_with(b"RIFF") || bytes.get(8..12) != Some(b"WEBP".as_slice()) {
        return None;
    }
    match bytes.get(12..16) {
        Some(b"VP8 ") => {
            // Lossy bitstream: 3-byte start code then 14-bit w/h.
            if bytes.len() < 30 || bytes[23..26] != [0x9D, 0x01, 0x2A] {
                return None;
            }
            let width = (u16::from_le_bytes([bytes[26], bytes[27]]) & 0x3FFF) as u32;
            let height = (u16::from_le_bytes([bytes[28], bytes[29]]) & 0x3FFF) as u32;
            (width > 0 && height > 0).then_some((width, height))
        }
        Some(b"VP8L") => {
            if bytes.len() < 25 || bytes[20] != 0x2F {
                return None;
            }
            let b0 = bytes[21] as u32;
            let b1 = bytes[22] as u32;
            let b2 = bytes[23] as u32;
            let b3 = bytes[24] as u32;
            let width = (b0 | ((b1 & 0x3F) << 8)) + 1;
            let height = ((b1 >> 6) | (b2 << 2) | ((b3 & 0x0F) << 10)) + 1;
            (width > 0 && height > 0).then_some((width, height))
        }
        Some(b"VP8X") => {
            if bytes.len() < 30 {
                return None;
            }
            let width = (u32::from_le_bytes([bytes[24], bytes[25], bytes[26], 0])) + 1;
            let height = (u32::from_le_bytes([bytes[27], bytes[28], bytes[29], 0])) + 1;
            (width > 0 && height > 0).then_some((width, height))
        }
        _ => None,
    }
}

/// BMP: `BM` + DIB header; width/height as LE i32 at +18/+22 (height may be
/// negative for top-down bitmaps — take the absolute value).
fn bmp_dims(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 26 || !bytes.starts_with(b"BM") {
        return None;
    }
    let width = i32::from_le_bytes(bytes[18..22].try_into().ok()?);
    let height = i32::from_le_bytes(bytes[22..26].try_into().ok()?);
    let (width, height) = (width.abs() as u32, height.abs() as u32);
    (width > 0 && height > 0).then_some((width, height))
}

/// Scale `(width, height)` to fit inside a `bound`x`bound` box (never
/// upscaling, never 0x0).
pub fn fit_within(width: u32, height: u32, bound: u32) -> (u32, u32) {
    let scale = (f64::from(bound) / f64::from(width))
        .min(f64::from(bound) / f64::from(height))
        .min(1.0);
    (
        ((f64::from(width) * scale).round() as u32).max(1),
        ((f64::from(height) * scale).round() as u32).max(1),
    )
}

/// Read MP4/QuickTime dimensions from the first `tkhd` box carrying them.
///
/// The `tkhd` payload always ends with `width`/`height` as 16.16 fixed-point
/// values (version 0 and 1 only differ in the prefix), so the box tail is
/// enough — no full ISO-BMFF model is built.
pub fn mp4_dimensions(bytes: &[u8]) -> (Option<u32>, Option<u32>) {
    find_tkhd_size(bytes).unwrap_or((None, None))
}

/// Walk the ISO-BMFF box tree for a `tkhd` box with a positive size.
fn find_tkhd_size(bytes: &[u8]) -> Option<(Option<u32>, Option<u32>)> {
    // Container boxes a `tkhd` can live in (`moov/trak/...`).
    const CONTAINERS: [&[u8; 4]; 7] = [b"moov", b"trak", b"mdia", b"minf", b"stbl", b"edts", b"udta"];
    let mut offset = 0usize;
    while offset + 8 <= bytes.len() {
        let size = u32::from_be_bytes(bytes[offset..offset + 4].try_into().ok()?) as u64;
        let kind: &[u8; 4] = bytes[offset + 4..offset + 8].try_into().ok()?;
        let (start, end) = if size == 1 {
            // 64-bit largesize.
            if offset + 16 > bytes.len() {
                return None;
            }
            let large = u64::from_be_bytes(bytes[offset + 8..offset + 16].try_into().ok()?);
            if large < 16 {
                return None;
            }
            (offset + 16, offset + large as usize)
        } else if size == 0 {
            // Box extends to the end of the file.
            (offset + 8, bytes.len())
        } else {
            if size < 8 {
                return None;
            }
            (offset + 8, offset + size as usize)
        };
        if start > bytes.len() || start > end {
            return None;
        }
        let end = end.min(bytes.len());
        let content = &bytes[start..end];
        if kind == b"tkhd" {
            let (width, height) = tkhd_size(content);
            // Skip audio-only tracks (0x0) and keep looking.
            if width.is_some() && height.is_some() {
                return Some((width, height));
            }
        } else if CONTAINERS.contains(&kind) {
            if let Some(found) = find_tkhd_size(content)
                && found.0.is_some()
            {
                return Some(found);
            }
        }
        if size == 0 {
            break;
        }
        offset = end;
    }
    None
}

/// The trailing 16.16 fixed-point width/height of a `tkhd` payload.
fn tkhd_size(content: &[u8]) -> (Option<u32>, Option<u32>) {
    if content.len() < 8 {
        return (None, None);
    }
    let tail = &content[content.len() - 8..];
    let width = u32::from_be_bytes(tail[0..4].try_into().ok().unwrap()) >> 16;
    let height = u32::from_be_bytes(tail[4..8].try_into().ok().unwrap()) >> 16;
    (
        (width > 0).then_some(width),
        (height > 0).then_some(height),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn png_dims_from_ihdr() {
        let mut bytes = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
        bytes.extend_from_slice(&13u32.to_be_bytes());
        bytes.extend_from_slice(b"IHDR");
        bytes.extend_from_slice(&24u32.to_be_bytes());
        bytes.extend_from_slice(&12u32.to_be_bytes());
        bytes.extend_from_slice(&[8, 2, 0, 0, 0]);
        let processed = process("image/png", &bytes).expect("png");
        assert_eq!((processed.width, processed.height), (Some(24), Some(12)));
        assert_eq!(processed.blurhash, None);
    }

    #[test]
    fn jpeg_dims_from_sof0() {
        // SOI + APP0 + SOF0 (24x12).
        let mut bytes = vec![0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10];
        bytes.extend_from_slice(b"JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00");
        bytes.extend_from_slice(&[0xFF, 0xC0, 0x00, 0x0B, 0x08]);
        bytes.extend_from_slice(&12u16.to_be_bytes());
        bytes.extend_from_slice(&24u16.to_be_bytes());
        bytes.extend_from_slice(&[0x01, 0x01, 0x11, 0x00]);
        let processed = process("image/jpeg", &bytes).expect("jpeg");
        assert_eq!((processed.width, processed.height), (Some(24), Some(12)));
    }

    #[test]
    fn gif_dims_from_logical_screen_descriptor() {
        let mut bytes = b"GIF89a".to_vec();
        bytes.extend_from_slice(&320u16.to_le_bytes());
        bytes.extend_from_slice(&200u16.to_le_bytes());
        let processed = process("image/gif", &bytes).expect("gif");
        assert_eq!((processed.width, processed.height), (Some(320), Some(200)));
        assert!(process("image/gif", b"GIF89a\x00").is_err());
    }

    #[test]
    fn webp_vp8x_dims_from_canvas() {
        // RIFF/WEBP + VP8X + 24-bit (canvas-1) 639x479 → 640x480.
        let mut bytes = b"RIFF".to_vec();
        bytes.extend_from_slice(&22u32.to_le_bytes());
        bytes.extend_from_slice(b"WEBPVP8X");
        bytes.extend_from_slice(&[0; 8]);
        bytes.extend_from_slice(&[0x7F, 0x02, 0x00, 0xDF, 0x01, 0x00]);
        let processed = process("image/webp", &bytes).expect("webp");
        assert_eq!((processed.width, processed.height), (Some(640), Some(480)));
        assert!(process("image/webp", b"RIFF\x00\x00\x00\x00WEBPXXXX").is_err());
    }

    #[test]
    fn bmp_dims_from_dib_header() {
        let mut bytes = b"BM".to_vec();
        bytes.extend_from_slice(&[0; 16]);
        bytes.extend_from_slice(&100i32.to_le_bytes());
        bytes.extend_from_slice(&(-50i32).to_le_bytes());
        let processed = process("image/bmp", &bytes).expect("bmp");
        assert_eq!((processed.width, processed.height), (Some(100), Some(50)));
    }

    #[test]
    fn corrupt_or_unsupported_bytes_are_unidentifiable() {
        // Right magic bytes, truncated payload: decoding fails.
        let mut png = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
        png.extend_from_slice(&[0; 16]);
        assert!(matches!(
            process("image/png", &png),
            Err(ProcessError::Unidentifiable(_))
        ));
        assert!(matches!(
            process("application/pdf", &[0; 32]),
            Err(ProcessError::Unidentifiable(_))
        ));
        // No pure-Rust parser for these: the ffmpeg job owns them.
        for mime in ["image/heic", "image/heif", "image/tiff", "image/avif"] {
            assert!(matches!(
                process(mime, &[0; 64]),
                Err(ProcessError::Unidentifiable(_))
            ));
        }
    }

    #[test]
    fn mp4_dimensions_from_tkhd_box() {
        // moov > trak > tkhd; payload ends with 16.16 width/height.
        let mut tkhd = vec![0u8; 84];
        let tail = [
            (640u32 << 16).to_be_bytes(),
            (480u32 << 16).to_be_bytes(),
        ]
        .concat();
        tkhd[76..84].copy_from_slice(&tail);
        let mut trak = Vec::new();
        trak.extend_from_slice(&((tkhd.len() + 8) as u32).to_be_bytes());
        trak.extend_from_slice(b"tkhd");
        trak.extend_from_slice(&tkhd);
        let mut moov = Vec::new();
        moov.extend_from_slice(&((trak.len() + 8) as u32).to_be_bytes());
        moov.extend_from_slice(b"moov");
        moov.extend_from_slice(&trak);
        let mut mp4 = vec![0u8, 0, 0, 0x14];
        mp4.extend_from_slice(b"ftypisom");
        mp4.extend_from_slice(&[0, 0, 0, 0]);
        mp4.extend_from_slice(b"isom");
        mp4.extend_from_slice(&moov);

        assert_eq!(mp4_dimensions(&mp4), (Some(640), Some(480)));

        // No moov at all: dimensions stay None, never fabricated.
        let mut bare = vec![0u8, 0, 0, 0x18];
        bare.extend_from_slice(b"ftypisom");
        bare.extend_from_slice(&[0; 16]);
        assert_eq!(mp4_dimensions(&bare), (None, None));
    }

    #[test]
    fn audio_and_webm_are_accepted_without_dimensions() {
        let ogg = process("audio/ogg", b"OggS some audio bytes here").expect("ogg");
        assert_eq!((ogg.width, ogg.height), (None, None));
        let webm = process("video/webm", &[0x1A, 0x45, 0xDF, 0xA3, 0, 0, 0, 0, 0, 0, 0, 0])
            .expect("webm");
        assert_eq!((webm.width, webm.height), (None, None));
    }

    #[test]
    fn fit_within_never_upscales_or_produces_zero() {
        assert_eq!(fit_within(100, 50, 460), (100, 50));
        assert_eq!(fit_within(920, 460, 460), (460, 230));
        assert_eq!(fit_within(1, 1000, 460), (1, 460));
    }
}

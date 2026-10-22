//! Media metadata: pure data + validation (no I/O, no bytes).

use crate::domain::id::DomainError;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MediaProperties {
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub orientation: Option<u8>,
    pub avg_color: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MediaMeta {
    pub id: String,
    pub owner_id: Option<String>,
    pub name: String,
    pub mime: String,
    pub size_bytes: u64,
    pub hash: String,
    /// SHA-256 hex of the stored bytes: the `BlobStore` content address
    /// (Phase 2.6 slice 8). `hash` stays the MD5: it feeds the client-visible
    /// packer `md5` and the per-owner upload dedup key. `#[serde(default)]`
    /// so pre-slice-8 rows still decode (they hold MD5-keyed blobs and are
    /// unsupported — see the domain-types wipe note — but must never crash
    /// a read).
    #[serde(default)]
    pub blob_key: String,
    pub blurhash: Option<String>,
    pub properties: MediaProperties,
    pub is_sensitive: bool,
    pub is_link: bool,
    pub url: String,
    pub thumbnail_url: Option<String>,
    pub comment: Option<String>,
    /// Remote origin URL for cache rows (`is_link == true`). `None` for
    /// local uploads. Together with the owning note's `file_ids` this is
    /// the idempotency key that keeps inbox replays from orphaning rows
    /// (Phase 2.6 slice 7); `#[serde(default)]` so pre-slice-7 rows decode.
    #[serde(default)]
    pub source_url: Option<String>,
    /// Cache variant within one `(source_url)` family (e.g. `original`,
    /// `preview`, `avatar`). Defaults to `original` so pre-change rows
    /// decode; validated to `1..=64` chars of alnum plus `-`/`_`.
    #[serde(default = "default_media_variant")]
    pub variant: String,
    /// Remote-cache media class: `avatar` | `preview` | `full` |
    /// `audio_video`. Plain `String` (not the enum) so unknown future
    /// classes decode instead of failing the row read; validated against
    /// the four known values. Defaults to `preview` for pre-change rows.
    #[serde(default = "default_media_class")]
    pub media_class: String,
    /// Eviction marker: `true` means the bytes are gone (dropped under
    /// cache pressure) but this metadata row plus `source_url` are
    /// retained so the bytes can be re-fetched. Defaults to `false` for
    /// pre-change rows.
    #[serde(default)]
    pub evicted: bool,
}

/// Default `MediaMeta::variant` for rows written before variants existed.
fn default_media_variant() -> String {
    "original".to_string()
}

/// Default `MediaMeta::media_class` for rows written before classes existed.
fn default_media_class() -> String {
    MediaClass::Preview.as_str().to_string()
}

/// Remote-cache media class (`docs/remote-media-cache-plan.md` §5).
/// Serialized as `avatar` | `preview` | `full` | `audio_video`.
///
/// Lives here (not in `application::ports`) because the dependency rule is
/// `domain` ← `application`: domain must not import ports. `ports.rs`
/// carries a mirror enum with identical semantics for the store/API
/// layer; keep the two in sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaClass {
    Avatar,
    #[default]
    Preview,
    Full,
    AudioVideo,
}

impl MediaClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            MediaClass::Avatar => "avatar",
            MediaClass::Preview => "preview",
            MediaClass::Full => "full",
            MediaClass::AudioVideo => "audio_video",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "avatar" => Some(MediaClass::Avatar),
            "preview" => Some(MediaClass::Preview),
            "full" => Some(MediaClass::Full),
            "audio_video" => Some(MediaClass::AudioVideo),
            _ => None,
        }
    }

    /// MIME-based default class. Avatars/emoji are a caller-context
    /// override (the caller passes `avatar` explicitly); everything else
    /// classifies by content type: audio/video → `AudioVideo`, images →
    /// `Preview`, all other types → `Full`.
    pub fn classify_mime(mime: &str) -> Self {
        if mime.starts_with("audio/") || mime.starts_with("video/") {
            MediaClass::AudioVideo
        } else if mime.starts_with("image/") {
            MediaClass::Preview
        } else {
            MediaClass::Full
        }
    }
}

impl MediaMeta {
    /// `max_file_size` is instance configuration; exceeding it surfaces
    /// `MAX_FILE_SIZE_EXCEEDED` at the application layer.
    pub fn validate(&self, max_file_size: u64) -> Result<(), DomainError> {
        let name_len = self.name.chars().count();
        if name_len == 0 || name_len > 256 {
            return Err(DomainError::Validation(
                "name must be 1..=256 chars".to_string(),
            ));
        }
        if self.name.contains('\0') || self.name.contains('/') || self.name.contains('\\') {
            return Err(DomainError::Validation(
                "name must not contain NUL, / or \\".to_string(),
            ));
        }
        let mime_len = self.mime.chars().count();
        if mime_len == 0 || mime_len > 128 {
            return Err(DomainError::Validation(
                "mime must be 1..=128 chars".to_string(),
            ));
        }
        if self.size_bytes > max_file_size {
            return Err(DomainError::Validation(
                "size_bytes exceeds max_file_size".to_string(),
            ));
        }
        // Hashes are hex/alnum text; raw binary blob bytes are rejected here.
        // `hash` is the client-visible MD5 (packer `md5`, per-owner dedup
        // key); `blob_key` is the SHA-256 blob-store address (slice 8).
        if self.hash.is_empty()
            || self.hash.len() > 128
            || !self.hash.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err(DomainError::Validation(
                "hash must be 1..=128 ASCII alphanumeric chars".to_string(),
            ));
        }
        if self.blob_key.is_empty()
            || self.blob_key.len() > 128
            || !self.blob_key.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err(DomainError::Validation(
                "blob_key must be 1..=128 ASCII alphanumeric chars".to_string(),
            ));
        }
        if let Some(blurhash) = &self.blurhash {
            if blurhash.chars().count() > 128 {
                return Err(DomainError::Validation(
                    "blurhash exceeds 128 chars".to_string(),
                ));
            }
        }
        let url_len = self.url.chars().count();
        if url_len == 0 || url_len > 1024 {
            return Err(DomainError::Validation(
                "url must be 1..=1024 chars".to_string(),
            ));
        }
        if let Some(thumbnail_url) = &self.thumbnail_url {
            if thumbnail_url.chars().count() > 512 {
                return Err(DomainError::Validation(
                    "thumbnail_url exceeds 512 chars".to_string(),
                ));
            }
        }
        if let Some(comment) = &self.comment {
            if comment.chars().count() > 512 {
                return Err(DomainError::Validation(
                    "comment exceeds 512 chars".to_string(),
                ));
            }
        }
        if let Some(source_url) = &self.source_url {
            if source_url.chars().count() > 1024 {
                return Err(DomainError::Validation(
                    "source_url exceeds 1024 chars".to_string(),
                ));
            }
        }
        let variant_len = self.variant.chars().count();
        if variant_len == 0
            || variant_len > 64
            || !self
                .variant
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(DomainError::Validation(
                "variant must be 1..=64 chars of alnum, - or _".to_string(),
            ));
        }
        if MediaClass::from_str(&self.media_class).is_none() {
            return Err(DomainError::Validation(
                "media_class must be avatar|preview|full|audio_video".to_string(),
            ));
        }
        Ok(())
    }
}

/// Slice 7 (H2+C3) MIME allowlist: the single shared sniffer for local and
/// remote ingest. It takes **no caller-supplied fallback**: the stored MIME
/// comes from sniffing only, so declared `Content-Type`s and ActivityPub
/// `mediaType`s can never introduce a type outside this path.
///
/// Allowlist: `image/png`, `image/jpeg`, `image/gif`, `image/webp`,
/// `image/avif`, `video/mp4`, `video/webm`, `video/mpeg`, `audio/mpeg`,
/// `audio/ogg`, `audio/wav`, `text/plain`, `application/octet-stream`.
/// Explicitly never `text/html`, `image/svg+xml`, `application/xhtml+xml`
/// or `text/xml`. (`video/mpeg` is not in the slice-7 allowlist text but the
/// magic table requires handling `0x000001BA`; the honest type is
/// `video/mpeg`, still `video/*` inline-safe. Former `application/pdf`
/// rows no longer sniff as PDF: PDF bytes that are not valid UTF-8 land on
/// `application/octet-stream`, short ASCII `%PDF` headers on `text/plain`;
/// both always serve as attachments.)
///
/// Order: magic bytes first, then the markup guard, then UTF-8.
/// Markup-looking content (leading `<` after an optional BOM/whitespace,
/// e.g. `<svg`, `<?xml`, `<html`) MUST NOT become text or xml — it returns
/// `application/octet-stream`. `text/plain` is returned ONLY for valid
/// UTF-8 that is not markup; the final fallback is
/// `application/octet-stream`.
pub fn sniff_mime(bytes: &[u8]) -> String {
    if bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]) {
        return "image/png".to_string();
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return "image/jpeg".to_string();
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return "image/gif".to_string();
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") {
        if bytes.get(8..12) == Some(b"WAVE".as_slice()) {
            return "audio/wav".to_string();
        }
        if bytes.get(8..12) == Some(b"WEBP".as_slice()) {
            return "image/webp".to_string();
        }
    }
    // `ftyp` boxes cover mp4 and avif; check the avif brands first so they
    // are not mislabeled. Any other `ftyp` (including QuickTime `qt`) maps
    // to `video/mp4`.
    if bytes.len() >= 12 && bytes.get(4..8) == Some(b"ftyp".as_slice()) {
        if bytes
            .get(8..12)
            .is_some_and(|brand| brand == b"avif" || brand == b"avis")
        {
            return "image/avif".to_string();
        }
        return "video/mp4".to_string();
    }
    // EBML header (`0x1A45DFA3`) → WebM.
    if bytes.starts_with(&[0x1A, 0x45, 0xDF, 0xA3]) {
        return "video/webm".to_string();
    }
    // Ogg container (`OggS`) → audio/ogg.
    if bytes.starts_with(b"OggS") {
        return "audio/ogg".to_string();
    }
    // MPEG audio: ID3v2 tag or a validated MPEG frame header. A bare
    // two-byte frame sync (`0xFF` + `0xE0…`) false-positives on binary —
    // notably the UTF-16LE BOM `FF FE` — so the version, layer, bitrate
    // and sampling fields must all be valid. (Full frame-chain validation
    // is out of scope: a mislabeled file plays as audio at worst, and
    // served files carry `nosniff` plus a sandboxed CSP anyway.)
    if bytes.starts_with(b"ID3") {
        return "audio/mpeg".to_string();
    }
    if looks_like_mpeg_frame(bytes) {
        return "audio/mpeg".to_string();
    }
    // MPEG program-stream pack header (`0x000001BA`) → video/mpeg.
    if bytes.starts_with(&[0x00, 0x00, 0x01, 0xBA]) {
        return "video/mpeg".to_string();
    }
    if starts_with_markup(bytes) {
        return "application/octet-stream".to_string();
    }
    if std::str::from_utf8(bytes).is_ok() {
        return "text/plain".to_string();
    }
    "application/octet-stream".to_string()
}

/// True for a plausible MPEG audio frame header (sync + valid version,
/// layer, bitrate and sampling fields). See the call site for why the bare
/// two-byte sync is not enough.
fn looks_like_mpeg_frame(bytes: &[u8]) -> bool {
    if bytes.len() < 4 || bytes[0] != 0xFF || bytes[1] & 0xE0 != 0xE0 {
        return false;
    }
    let version = (bytes[1] >> 3) & 0x03;
    let layer = (bytes[1] >> 1) & 0x03;
    let bitrate = bytes[2] >> 4;
    let sampling = (bytes[2] >> 2) & 0x03;
    version != 0 && layer != 0 && bitrate != 0 && bitrate != 0x0F && sampling != 3
}

/// True when the first non-BOM, non-whitespace byte is `<`.
fn starts_with_markup(bytes: &[u8]) -> bool {
    let mut rest = bytes;
    if rest.starts_with(&[0xEF, 0xBB, 0xBF]) {
        rest = &rest[3..];
    }
    let mut i = 0;
    while i < rest.len() && rest[i].is_ascii_whitespace() {
        i += 1;
    }
    rest.get(i) == Some(&b'<')
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_FILE_SIZE: u64 = 1024;

    fn valid_media() -> MediaMeta {
        MediaMeta {
            id: "mid00000000000001".to_string(),
            owner_id: Some("aid00000000000001".to_string()),
            name: "photo.png".to_string(),
            mime: "image/png".to_string(),
            size_bytes: 512,
            hash: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
            blob_key: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .to_string(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: false,
            url: "https://example.local/files/mid00000000000001".to_string(),
            thumbnail_url: None,
            comment: None,
            source_url: None,
            variant: default_media_variant(),
            media_class: default_media_class(),
            evicted: false,
        }
    }

    #[test]
    fn valid_media_passes() {
        assert!(valid_media().validate(MAX_FILE_SIZE).is_ok());
    }

    #[test]
    fn name_bounds() {
        let mut m = valid_media();
        m.name = "n".repeat(256);
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        m.name = "n".repeat(257);
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        for bad in ["", "a\0b", "a/b", "a\\b"] {
            m.name = bad.to_string();
            assert!(m.validate(MAX_FILE_SIZE).is_err(), "expected err: {bad:?}");
        }
    }

    #[test]
    fn mime_and_size_bounds() {
        let mut m = valid_media();
        m.mime = String::new();
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.mime = "m".repeat(129);
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.mime = "image/png".to_string();
        m.size_bytes = MAX_FILE_SIZE;
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        m.size_bytes = MAX_FILE_SIZE + 1;
        assert!(m.validate(MAX_FILE_SIZE).is_err());
    }

    #[test]
    fn hash_rejects_binary() {
        let mut m = valid_media();
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        m.hash = String::new();
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.hash = "h".repeat(129);
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        // Raw binary/blob bytes are not valid hashes.
        m.hash = "\u{0}\u{1}\u{2}binary".to_string();
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.hash = "not a hash!".to_string();
        assert!(m.validate(MAX_FILE_SIZE).is_err());
    }

    #[test]
    fn blob_key_rejects_empty_and_binary() {
        // Slice 8 (D6, written before the fix): the SHA-256 blob address
        // carries the same alnum bounds as the md5 field.
        let mut m = valid_media();
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        m.blob_key = String::new();
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.blob_key = "k".repeat(129);
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.blob_key = "not a key!".to_string();
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.blob_key = "\u{0}\u{1}\u{2}binary".to_string();
        assert!(m.validate(MAX_FILE_SIZE).is_err());
    }

    #[test]
    fn url_comment_bounds() {
        let mut m = valid_media();
        m.url = "u".repeat(1024);
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        m.url = "u".repeat(1025);
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.url = String::new();
        assert!(m.validate(MAX_FILE_SIZE).is_err());

        let mut m = valid_media();
        m.thumbnail_url = Some("t".repeat(513));
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.thumbnail_url = Some("t".repeat(512));
        m.comment = Some("c".repeat(513));
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.comment = Some("c".repeat(512));
        m.blurhash = Some("b".repeat(129));
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.blurhash = Some("b".repeat(128));
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
    }

    #[test]
    fn source_url_bounds() {
        let mut m = valid_media();
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        m.source_url = Some("u".repeat(1024));
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        m.source_url = Some("u".repeat(1025));
        assert!(m.validate(MAX_FILE_SIZE).is_err());
    }

    // --- Slice 7 (H2+C3) shared-sniffer regression tests ---

    #[test]
    fn sniff_detects_image_and_av_magic() {
        // PNG / JPEG / GIF.
        assert_eq!(
            sniff_mime(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0x00]),
            "image/png"
        );
        assert_eq!(sniff_mime(&[0xFF, 0xD8, 0xFF, 0xE0, 0x00]), "image/jpeg");
        assert_eq!(sniff_mime(b"GIF89a\x01\x00"), "image/gif");
        assert_eq!(sniff_mime(b"GIF87a\x01\x00"), "image/gif");
        // WebP vs WAV share the RIFF prefix; bytes 8..12 discriminate.
        let mut webp = b"RIFF\x00\x00\x00\x00WEBP".to_vec();
        assert_eq!(sniff_mime(&webp), "image/webp");
        webp[8..12].copy_from_slice(b"WAVE");
        webp.extend_from_slice(b"fmt ");
        assert_eq!(sniff_mime(&webp), "audio/wav");
        // ftyp: avif brands stay avif, everything else is mp4.
        let mut ftyp = vec![0x00, 0x00, 0x00, 0x20];
        ftyp.extend_from_slice(b"ftypavif");
        assert_eq!(sniff_mime(&ftyp), "image/avif");
        ftyp[4..8].copy_from_slice(b"ftyp");
        ftyp[8..12].copy_from_slice(b"isom");
        assert_eq!(sniff_mime(&ftyp), "video/mp4");
        // EBML → WebM; OggS → audio/ogg.
        assert_eq!(
            sniff_mime(&[0x1A, 0x45, 0xDF, 0xA3, 0x93, 0x42]),
            "video/webm"
        );
        assert_eq!(sniff_mime(b"OggS\x00\x02"), "audio/ogg");
        // MPEG audio: ID3 tag or frame sync.
        assert_eq!(sniff_mime(b"ID3\x04\x00\x00\x00"), "audio/mpeg");
        assert_eq!(sniff_mime(&[0xFF, 0xFB, 0x90, 0x00]), "audio/mpeg");
        // MPEG program stream → video/mpeg.
        assert_eq!(
            sniff_mime(&[0x00, 0x00, 0x01, 0xBA, 0x21, 0x00]),
            "video/mpeg"
        );
    }

    #[test]
    fn sniff_markup_never_becomes_text_or_xml() {
        // Slice 7 (H2): remote `text/html` and SVG attachments must never be
        // stored (or served) as html/svg, no matter the declared type — the
        // sniffer takes no fallback, so there is nothing to assert about
        // declared types here.
        for body in [
            "<!DOCTYPE html><html><body>hi</body></html>",
            "<html><body>hi</body></html>",
            "<svg xmlns=\"http://www.w3.org/2000/svg\"></svg>",
            "<?xml version=\"1.0\"?><rss/>",
            "  \n\t<svg></svg>",
        ] {
            assert_eq!(
                sniff_mime(body.as_bytes()),
                "application/octet-stream",
                "markup must not sniff as text: {body:?}"
            );
        }
        // A UTF-8 BOM does not smuggle markup past the guard.
        let mut bom_svg = vec![0xEF, 0xBB, 0xBF];
        bom_svg.extend_from_slice(b"<svg/>");
        assert_eq!(sniff_mime(&bom_svg), "application/octet-stream");
    }

    #[test]
    fn sniff_utf8_is_plain_and_binary_is_octet() {
        assert_eq!(sniff_mime(b"hello world"), "text/plain");
        assert_eq!(sniff_mime("héllo wörld".as_bytes()), "text/plain");
        // Non-UTF-8 bytes claiming to be text (e.g. a local upload with
        // `Content-Type: text/html`) must not stay text.
        assert_eq!(
            sniff_mime(&[0xFF, 0xFE, 0x00, 0x01, 0x02]),
            "application/octet-stream"
        );
        assert_eq!(
            sniff_mime(&[0x89, 0x89, 0x89, 0x89]),
            "application/octet-stream"
        );
        // A genuine MPEG frame header (sync + valid version/layer/bitrate)
        // still sniffs as audio, and so does an ID3 tag.
        assert_eq!(sniff_mime(&[0xFF, 0xFB, 0x90, 0x00]), "audio/mpeg");
        assert_eq!(sniff_mime(b"ID3\x04\x00\x00\x00"), "audio/mpeg");
    }

    // --- Remote-cache fields (variant / media_class / evicted) ---

    #[test]
    fn new_fields_default_on_valid_media() {
        let m = valid_media();
        assert_eq!(m.variant, "original");
        assert_eq!(m.media_class, "preview");
        assert!(!m.evicted);
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
    }

    #[test]
    fn pre_change_rows_decode_with_defaults() {
        // Rows written before the cache fields existed carry no
        // `variant`/`media_class`/`evicted` keys; serde defaults must fill
        // them so old rows never fail a read.
        let old_row = serde_json::json!({
            "id": "mid00000000000001",
            "owner_id": null,
            "name": "photo.png",
            "mime": "image/png",
            "size_bytes": 512,
            "hash": "d41d8cd98f00b204e9800998ecf8427e",
            "blob_key": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "blurhash": null,
            "properties": {"width": null, "height": null, "orientation": null, "avg_color": null},
            "is_sensitive": false,
            "is_link": true,
            "url": "https://example.local/files/mid00000000000001",
            "thumbnail_url": null,
            "comment": null,
            "source_url": "https://remote.invalid/pic.png"
        });
        let m: MediaMeta = serde_json::from_value(old_row).expect("old row must decode");
        assert_eq!(m.variant, "original");
        assert_eq!(m.media_class, "preview");
        assert!(!m.evicted);
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        // Round-trip keeps the new fields.
        let rt: MediaMeta = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(rt, m);
    }

    #[test]
    fn variant_bounds() {
        let mut m = valid_media();
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        for bad in ["", "has space", "has/slash", "semi;colon", "dot.name"] {
            m.variant = bad.to_string();
            assert!(m.validate(MAX_FILE_SIZE).is_err(), "expected err: {bad:?}");
        }
        m.variant = "v".repeat(64);
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        m.variant = "v".repeat(65);
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.variant = "preview-2_thumb".to_string();
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
    }

    #[test]
    fn media_class_values() {
        let mut m = valid_media();
        for good in ["avatar", "preview", "full", "audio_video"] {
            m.media_class = good.to_string();
            assert!(m.validate(MAX_FILE_SIZE).is_ok(), "expected ok: {good:?}");
        }
        for bad in ["", "image", "Preview", "AUDIO_VIDEO", "original"] {
            m.media_class = bad.to_string();
            assert!(m.validate(MAX_FILE_SIZE).is_err(), "expected err: {bad:?}");
        }
    }

    #[test]
    fn media_class_str_roundtrip_and_defaults() {
        assert_eq!(MediaClass::default(), MediaClass::Preview);
        for class in [
            MediaClass::Avatar,
            MediaClass::Preview,
            MediaClass::Full,
            MediaClass::AudioVideo,
        ] {
            assert_eq!(MediaClass::from_str(class.as_str()), Some(class));
        }
        assert_eq!(MediaClass::from_str("image"), None);
        assert_eq!(MediaClass::from_str(""), None);
        // Serde uses the same snake_case wire form as `as_str`.
        for class in [
            MediaClass::Avatar,
            MediaClass::Preview,
            MediaClass::Full,
            MediaClass::AudioVideo,
        ] {
            let wire = serde_json::to_string(&class).unwrap();
            assert_eq!(wire, format!("\"{}\"", class.as_str()));
            assert_eq!(serde_json::from_str::<MediaClass>(&wire).unwrap(), class);
        }
    }

    #[test]
    fn media_class_classify_mime() {
        // Avatar is caller-context, never inferred from MIME.
        assert_eq!(MediaClass::classify_mime("image/png"), MediaClass::Preview);
        assert_eq!(MediaClass::classify_mime("image/avif"), MediaClass::Preview);
        assert_eq!(
            MediaClass::classify_mime("audio/mpeg"),
            MediaClass::AudioVideo
        );
        assert_eq!(
            MediaClass::classify_mime("video/mp4"),
            MediaClass::AudioVideo
        );
        assert_eq!(MediaClass::classify_mime("text/plain"), MediaClass::Full);
        assert_eq!(
            MediaClass::classify_mime("application/octet-stream"),
            MediaClass::Full
        );
    }
}

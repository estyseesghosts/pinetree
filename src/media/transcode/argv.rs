use super::{TranscodeConfig, TranscodePlan};
use std::path::Path;

// ---------------------------------------------------------------------------
// ffmpeg command construction.
// ---------------------------------------------------------------------------

/// Build the ffmpeg argv for a plan. Contract (relied on by tests):
/// `-i` immediately precedes the input path, and the output path is the
/// final argument. Every command carries `-map_metadata 0` and no
/// strip/normalize filter: metadata (including EXIF and the orientation tag)
/// rides along where the target format holds it.
pub fn build_argv(
    ffmpeg_path: &str,
    input: &Path,
    output: &Path,
    plan: TranscodePlan,
    with_audio: bool,
    config: &TranscodeConfig,
) -> Vec<String> {
    let mut argv = vec![
        ffmpeg_path.to_string(),
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-y".to_string(),
        "-i".to_string(),
        input.to_string_lossy().into_owned(),
        "-map_metadata".to_string(),
        "0".to_string(),
    ];
    match plan {
        TranscodePlan::ImageToWebp | TranscodePlan::Passthrough => {
            argv.extend([
                "-c:v".to_string(),
                "libwebp".to_string(),
                "-quality".to_string(),
                config.webp_quality.to_string(),
                "-compression_level".to_string(),
                "4".to_string(),
            ]);
        }
        TranscodePlan::GifToWebm => {
            argv.extend([
                "-an".to_string(),
                "-c:v".to_string(),
                "libvpx-vp9".to_string(),
                "-crf".to_string(),
                config.vp9_crf.to_string(),
                "-b:v".to_string(),
                "0".to_string(),
                "-pix_fmt".to_string(),
                // GIF transparency survives as WebM alpha.
                "yuva420p".to_string(),
                "-cpu-used".to_string(),
                config.vp9_cpu_used.to_string(),
                "-row-mt".to_string(),
                "1".to_string(),
            ]);
        }
        TranscodePlan::VideoToWebm => {
            argv.extend([
                "-map".to_string(),
                "0:v:0".to_string(),
                "-c:v".to_string(),
                "libvpx-vp9".to_string(),
                "-crf".to_string(),
                config.vp9_crf.to_string(),
                "-b:v".to_string(),
                "0".to_string(),
                "-pix_fmt".to_string(),
                "yuv420p".to_string(),
                "-cpu-used".to_string(),
                config.vp9_cpu_used.to_string(),
                "-row-mt".to_string(),
                "1".to_string(),
            ]);
            if with_audio {
                argv.extend([
                    "-map".to_string(),
                    "0:a:0".to_string(),
                    "-c:a".to_string(),
                    "libopus".to_string(),
                    "-b:a".to_string(),
                    format!("{}k", config.opus_bitrate_kbps),
                ]);
            } else {
                argv.push("-an".to_string());
            }
        }
        TranscodePlan::VideoRemux => {
            // Stream copy into the output container (`.webm` via
            // `derived_extension`): no re-encode, no quality loss, metadata
            // rides along with `-map_metadata 0`. `with_audio` is ignored —
            // every stream is copied.
            argv.extend([
                "-map".to_string(),
                "0".to_string(),
                "-c".to_string(),
                "copy".to_string(),
            ]);
        }
    }
    argv.push(output.to_string_lossy().into_owned());
    argv
}

/// Build the ffmpeg argv for the `small` still-image preview: the derived
/// WebP bytes scaled to fit inside a `bound`×`bound` box, encoded WebP at
/// the configured quality. Same `-map_metadata 0` contract as the main
/// command.
pub fn build_preview_argv(
    ffmpeg_path: &str,
    input: &Path,
    output: &Path,
    bound: u32,
    config: &TranscodeConfig,
) -> Vec<String> {
    let argv = vec![
        ffmpeg_path.to_string(),
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-y".to_string(),
        "-i".to_string(),
        input.to_string_lossy().into_owned(),
        "-map_metadata".to_string(),
        "0".to_string(),
        "-vf".to_string(),
        format!("scale='min({bound},iw)':'min({bound},ih)':eval=frame"),
        "-c:v".to_string(),
        "libwebp".to_string(),
        "-quality".to_string(),
        config.webp_quality.to_string(),
        "-compression_level".to_string(),
        "4".to_string(),
        output.to_string_lossy().into_owned(),
    ];
    argv
}

#[cfg(test)]
mod tests {
    use super::super::derived_extension;
    use super::*;
    use std::path::Path;

    #[test]
    fn remux_argv_copies_streams_without_reencode() {
        let config = TranscodeConfig::default();
        let argv = build_argv(
            "ffmpeg",
            std::path::Path::new("in.vid"),
            std::path::Path::new("out.webm"),
            TranscodePlan::VideoRemux,
            true,
            &config,
        );
        let joined = argv.join(" ");
        assert!(joined.contains("-map 0"), "{joined}");
        assert!(joined.contains("-c copy"), "{joined}");
        assert!(!joined.contains("libvpx"), "{joined}");
        assert!(!joined.contains("libopus"), "{joined}");
        assert!(joined.ends_with("out.webm"));
        assert_eq!(derived_extension(TranscodePlan::VideoRemux), "webm");
    }

    #[test]
    fn argv_carries_metadata_and_quality_with_output_last() {
        let cfg = TranscodeConfig::default();
        let input = Path::new("/tmp/in.gif");
        let output = Path::new("/tmp/out.webm");
        for (plan, with_audio, video, audio) in [
            (TranscodePlan::ImageToWebp, false, "libwebp", ""),
            (TranscodePlan::GifToWebm, false, "libvpx-vp9", ""),
            (TranscodePlan::VideoToWebm, true, "libvpx-vp9", "libopus"),
            (TranscodePlan::VideoToWebm, false, "libvpx-vp9", ""),
        ] {
            let argv = build_argv("ffmpeg", input, output, plan, with_audio, &cfg);
            assert!(argv.contains(&"-map_metadata".to_string()), "{plan:?}");
            assert!(argv.contains(&"0".to_string()), "{plan:?}");
            assert!(
                !argv.iter().any(|arg| arg == "-map_metadata -1"),
                "{plan:?}"
            );
            assert!(argv.contains(&video.to_string()), "{plan:?}");
            if !audio.is_empty() {
                assert!(argv.contains(&audio.to_string()), "{plan:?}");
            }
            // Fake-ffmpeg harness contract: `-i` precedes the input, the
            // output is the final argument.
            let i = argv.iter().position(|a| a == "-i").expect("has -i");
            assert_eq!(argv[i + 1], "/tmp/in.gif");
            assert_eq!(argv.last().map(String::as_str), Some("/tmp/out.webm"));
        }
        // Quality settings are the recorded WP1 defaults.
        let argv = build_argv(
            "ffmpeg",
            input,
            output,
            TranscodePlan::ImageToWebp,
            false,
            &cfg,
        );
        assert!(argv.contains(&"80".to_string()));
        let argv = build_argv(
            "ffmpeg",
            input,
            output,
            TranscodePlan::VideoToWebm,
            true,
            &cfg,
        );
        assert!(argv.contains(&"30".to_string()));
        assert!(argv.contains(&"96k".to_string()));
    }
}

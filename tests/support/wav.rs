//! 試験用の WAV を作る。
//!
//! - 48kHz、16bit、2ch の PCM。正弦波 (440Hz) を入れる
//!
//! 呼び出し方:
//! ```ignore
//! #[path = "support/wav.rs"]
//! mod wav;
//! let bytes = wav::sine_wav(1.0);
//! ```

#![allow(dead_code)]

/// サンプリング周波数 (Hz)。
const SAMPLE_RATE: u32 = 48_000;
/// チャンネル数。
const CHANNELS: u16 = 2;
/// 1 サンプルのビット数。
const BITS_PER_SAMPLE: u16 = 16;
/// 正弦波の周波数 (Hz)。
const TONE_HZ: f32 = 440.0;
/// 正弦波の振幅 (i16 の最大値に対する割合)。
const AMPLITUDE: f32 = 0.5;

/// 指定した長さの正弦波の WAV (RIFF ヘッダー付き) を作る。
///
/// - `secs`: 長さ (秒)
/// - 戻り値: WAV ファイルのバイト列
///
/// # 例
/// `let bytes = sine_wav(0.5);`
pub fn sine_wav(secs: f32) -> Vec<u8> {
	let frames = (SAMPLE_RATE as f32 * secs) as u32;
	let block_align = CHANNELS * BITS_PER_SAMPLE / 8;
	let data_len = frames * block_align as u32;
	let mut out = Vec::with_capacity(44 + data_len as usize);
	out.extend_from_slice(b"RIFF");
	out.extend_from_slice(&(36 + data_len).to_le_bytes());
	out.extend_from_slice(b"WAVEfmt ");
	out.extend_from_slice(&16u32.to_le_bytes());
	out.extend_from_slice(&1u16.to_le_bytes());
	out.extend_from_slice(&CHANNELS.to_le_bytes());
	out.extend_from_slice(&SAMPLE_RATE.to_le_bytes());
	out.extend_from_slice(&(SAMPLE_RATE * block_align as u32).to_le_bytes());
	out.extend_from_slice(&block_align.to_le_bytes());
	out.extend_from_slice(&BITS_PER_SAMPLE.to_le_bytes());
	out.extend_from_slice(b"data");
	out.extend_from_slice(&data_len.to_le_bytes());
	for i in 0..frames {
		let phase = 2.0 * std::f32::consts::PI * TONE_HZ * i as f32 / SAMPLE_RATE as f32;
		let sample = (phase.sin() * AMPLITUDE * i16::MAX as f32) as i16;
		for _ in 0..CHANNELS {
			out.extend_from_slice(&sample.to_le_bytes());
		}
	}
	out
}

#!/usr/bin/env python3
"""
Video Transcriber — Download Instagram reel videos and transcribe via Groq Whisper.
Cost: ~$0.001 per minute of audio. Extremely fast (< 1s for 15s clips).
"""
import os
import io
import tempfile
import logging
import requests

logger = logging.getLogger(__name__)

GROQ_API_URL = "https://api.groq.com/openai/v1/audio/transcriptions"


def _get_groq_key():
    key = os.getenv("GROQ_API_KEY", "")
    if not key:
        raise ValueError("GROQ_API_KEY not set in environment")
    return key


def download_video(video_url, max_size_mb=50):
    """
    Download a video from URL into a temporary file.
    Returns the temp file path, or None on failure.
    """
    if not video_url:
        logger.warning("No video URL provided")
        return None

    try:
        logger.info(f"Downloading video: {video_url[:80]}...")
        resp = requests.get(video_url, stream=True, timeout=60, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        })
        resp.raise_for_status()

        # Check content length
        content_length = int(resp.headers.get("Content-Length", 0))
        if content_length > max_size_mb * 1024 * 1024:
            logger.warning(f"Video too large: {content_length / 1024 / 1024:.1f}MB > {max_size_mb}MB limit")
            return None

        # Write to temp file
        suffix = ".mp4"
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        total = 0
        for chunk in resp.iter_content(chunk_size=8192):
            tmp.write(chunk)
            total += len(chunk)
            if total > max_size_mb * 1024 * 1024:
                tmp.close()
                os.unlink(tmp.name)
                logger.warning("Video exceeded size limit during download")
                return None

        tmp.close()
        logger.info(f"Downloaded {total / 1024:.1f}KB to {tmp.name}")
        return tmp.name

    except Exception as e:
        logger.error(f"Failed to download video: {e}")
        return None


def transcribe_audio(file_path, language="en"):
    """
    Transcribe audio/video file using Groq Whisper API.
    Returns transcript text string, or empty string on failure.
    """
    api_key = _get_groq_key()

    try:
        logger.info(f"Transcribing: {file_path}")
        with open(file_path, "rb") as f:
            resp = requests.post(
                GROQ_API_URL,
                headers={"Authorization": f"Bearer {api_key}"},
                files={"file": (os.path.basename(file_path), f, "video/mp4")},
                data={
                    "model": "whisper-large-v3-turbo",
                    "language": language,
                    "response_format": "json"
                },
                timeout=60
            )

        if resp.status_code == 200:
            result = resp.json()
            transcript = result.get("text", "")
            logger.info(f"Transcript ({len(transcript)} chars): {transcript[:100]}...")
            return transcript
        else:
            logger.error(f"Groq transcription failed: {resp.status_code} — {resp.text[:300]}")
            return ""

    except Exception as e:
        logger.error(f"Transcription error: {e}")
        return ""


def transcribe_reel(video_url, language="en"):
    """
    Full pipeline: download video → transcribe → clean up temp file.
    Returns transcript text.
    """
    file_path = None
    try:
        file_path = download_video(video_url)
        if not file_path:
            return ""

        transcript = transcribe_audio(file_path, language=language)
        return transcript

    finally:
        # Clean up temp file
        if file_path and os.path.exists(file_path):
            try:
                os.unlink(file_path)
            except:
                pass


def batch_transcribe_reels(reels, language="en", max_concurrent=5):
    """
    Transcribe multiple reels sequentially (Groq is fast enough).
    Adds 'transcript' key to each reel dict.
    Returns the updated reels list.
    """
    for i, reel in enumerate(reels):
        video_url = reel.get("video_url", "")
        if not video_url:
            reel["transcript"] = ""
            logger.info(f"Reel {i+1}/{len(reels)}: No video URL, skipping")
            continue

        logger.info(f"Reel {i+1}/{len(reels)}: Transcribing...")
        try:
            reel["transcript"] = transcribe_reel(video_url, language=language)
        except Exception as e:
            logger.error(f"Reel {i+1} transcription failed: {e}")
            reel["transcript"] = ""

    transcribed = sum(1 for r in reels if r.get("transcript"))
    logger.info(f"Transcribed {transcribed}/{len(reels)} reels")
    return reels

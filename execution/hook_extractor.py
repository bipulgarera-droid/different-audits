#!/usr/bin/env python3
"""
Hook Extractor — Extract the "hook" (opening line) from reel transcripts.
Uses Groq LLM (llama-3.3-70b-versatile) for fast, cheap analysis.
Falls back to simple text extraction if API fails.
"""
import os
import logging
import requests
import json

logger = logging.getLogger(__name__)

GROQ_CHAT_URL = "https://api.groq.com/openai/v1/chat/completions"


def _get_groq_key():
    key = os.getenv("GROQ_API_KEY", "")
    if not key:
        raise ValueError("GROQ_API_KEY not set in environment")
    return key


def extract_hook_llm(transcript, caption=""):
    """
    Use Groq LLM to extract the hook from a transcript.
    Returns: {hook_text, hook_type, summary}
    """
    if not transcript or not transcript.strip():
        return {"hook_text": "", "hook_type": "unknown", "summary": ""}

    api_key = _get_groq_key()
    prompt = f"""Analyze this Instagram reel transcript and extract the HOOK (the opening 1-2 sentences designed to grab attention).

TRANSCRIPT: "{transcript}"

CAPTION (if helpful): "{caption}"

Respond in this exact JSON format only, no other text:
{{
    "hook_text": "the exact opening hook text (first 1-2 sentences)",
    "hook_type": "one of: question, bold_claim, story, shock, tutorial, relatable, controversy, statistic",
    "summary": "one sentence describing what the reel is about"
}}"""

    try:
        resp = requests.post(
            GROQ_CHAT_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 300,
                "response_format": {"type": "json_object"}
            },
            timeout=30
        )

        if resp.status_code == 200:
            content = resp.json()["choices"][0]["message"]["content"]
            result = json.loads(content)
            return {
                "hook_text": result.get("hook_text", ""),
                "hook_type": result.get("hook_type", "unknown"),
                "summary": result.get("summary", "")
            }
        else:
            logger.error(f"Groq LLM failed: {resp.status_code} — {resp.text[:300]}")
            return _extract_hook_simple(transcript)

    except Exception as e:
        logger.error(f"Hook extraction error: {e}")
        return _extract_hook_simple(transcript)


def _extract_hook_simple(transcript):
    """
    Simple fallback: take the first sentence as the hook.
    """
    if not transcript:
        return {"hook_text": "", "hook_type": "unknown", "summary": ""}

    # Split by sentence endings
    sentences = []
    current = ""
    for char in transcript:
        current += char
        if char in ".!?" and len(current.strip()) > 5:
            sentences.append(current.strip())
            current = ""
    if current.strip():
        sentences.append(current.strip())

    hook = sentences[0] if sentences else transcript[:100]
    return {
        "hook_text": hook,
        "hook_type": "unknown",
        "summary": transcript[:150] + "..." if len(transcript) > 150 else transcript
    }


def extract_hooks_batch(reels):
    """
    Extract hooks from a batch of reels (each must have 'transcript' key).
    Adds 'hook_text', 'hook_type', 'summary' keys to each reel dict.
    Returns the updated reels list.
    """
    for i, reel in enumerate(reels):
        transcript = reel.get("transcript", "")
        caption = reel.get("caption", "")

        if transcript:
            logger.info(f"Extracting hook {i+1}/{len(reels)}...")
            try:
                hook_data = extract_hook_llm(transcript, caption)
            except Exception as e:
                logger.error(f"Hook extraction failed for reel {i+1}: {e}")
                hook_data = _extract_hook_simple(transcript)
        else:
            # No transcript — use caption as fallback
            hook_data = {
                "hook_text": caption[:100] if caption else "",
                "hook_type": "caption_only",
                "summary": caption[:150] if caption else ""
            }

        reel["hook_text"] = hook_data["hook_text"]
        reel["hook_type"] = hook_data["hook_type"]
        reel["summary"] = hook_data["summary"]

    extracted = sum(1 for r in reels if r.get("hook_text"))
    logger.info(f"Extracted hooks for {extracted}/{len(reels)} reels")
    return reels


def generate_actionable_strategy(client_profile, viral_reels):
    """
    Analyzes the client's baseline against the competitor viral reels.
    Uses Groq Llama-3 to generate a 3-bullet-point executive strategy for the presentation slide.
    """
    if not viral_reels:
        return ["No competitor reels found to analyze.", "Start by posting consistently.", "Engage with your local community."]

    api_key = _get_groq_key()
    
    # Prepare Context Data
    client_engagement = client_profile.get("engagement_rate", 0)
    client_niche = client_profile.get("category", "your industry")
    
    # Summarize best reels to avoid token limits
    reels_summary = []
    for r in viral_reels[:7]:
        comp_name = r.get("competitor", {}).get("username", "Competitor")
        hook = r.get("hook_text", "")
        hk_type = r.get("hook_type", "unknown")
        reels_summary.append(f"- @{comp_name} used a {hk_type} hook: '{hook}' (Views: {r.get('views', 0)})")
        
    reels_text = "\n".join(reels_summary)

    prompt = f"""You are a top-tier Social Media Marketing Consultant.
You are presenting a social media audit to a client in the {client_niche} niche.
Their current engagement rate is {client_engagement}%.

Here are the most viral reels their top local competitors recently posted:
{reels_text}

Write exactly 3 distinct, highly actionable bullet points summarizing what the client should do to replicate this success.
Keep each bullet point to 1-2 concise sentences. Be specific, referencing the hook styles or tactics used above. Do not be generic. Focus on hooks, formats, and styles.

Respond in this exact JSON format only, no other text:
{{
    "strategy_bullets": [
        "First bullet text",
        "Second bullet text",
        "Third bullet text"
    ]
}}"""

    try:
        resp = requests.post(
            GROQ_CHAT_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.5,
                "max_tokens": 500,
                "response_format": {"type": "json_object"}
            },
            timeout=30
        )

        if resp.status_code == 200:
            content = resp.json()["choices"][0]["message"]["content"]
            result = json.loads(content)
            bullets = result.get("strategy_bullets", [])
            if len(bullets) >= 3:
                return bullets[:3]
                
        logger.error(f"Groq LLM strategy failed: {resp.status_code} — {resp.text[:300]}")
    except Exception as e:
        logger.error(f"Strategy extraction error: {e}")

    # Fallback
    return [
        "Study the viral hooks your competitors are using in the swipe file below.",
        "Replicate their opening sentence formats but adapt them to your unique brand voice.",
        "Focus on hooks that invoke curiosity or state a negative claim to stop the scroll."
    ]

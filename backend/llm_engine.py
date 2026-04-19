import requests
import json
import re

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "llama3"


# =========================
# SAFE REQUEST
# =========================
def safe_request(payload, stream=False):
    try:
        response = requests.post(
            OLLAMA_URL,
            json=payload,
            stream=stream,
            timeout=60   # increased timeout (important)
        )
        return response
    except Exception as e:
        print("❌ LLM Request Error:", e)
        return None


# =========================
# CLEAN JSON RESPONSE (STRONG)
# =========================
def clean_json(text):
    if not text:
        return ""

    text = text.strip()

    # remove markdown blocks
    text = text.replace("```json", "").replace("```", "").strip()

    # extract JSON safely using regex
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return match.group(0)

    return text


# =========================
# 🧠 JSON QUERY GENERATOR
# =========================
def ask_llm(prompt):
    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0,        # 🔥 VERY IMPORTANT (forces consistency)
            "top_p": 0.9
        }
    }

    response = safe_request(payload)

    if response is None:
        raise Exception("❌ LLM not reachable")

    try:
        data = response.json()
        text = data.get("response", "")

        cleaned = clean_json(text)

        print("\n🧠 CLEANED LLM OUTPUT:\n", cleaned)

        return cleaned

    except Exception as e:
        print("❌ LLM JSON Error:", e)
        raise Exception("❌ Failed to parse LLM response")


# =========================
# 🧠 STREAM ANSWER
# =========================
def stream_answer(context, question):

    prompt = f"""
You are a data analyst AI.

IMPORTANT:
- Use ONLY provided data
- Negative profit = LOSS
- Positive profit = PROFIT

DATA:
{context}

QUESTION:
{question}

TASK:
- Give clear structured answer
- Use bullet points if needed
- Keep concise

FINAL ANSWER:
"""

    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": True,
        "options": {
            "temperature": 0.3
        }
    }

    response = safe_request(payload, stream=True)

    if response is None:
        yield "⚠️ LLM unavailable"
        return

    for line in response.iter_lines():
        if not line:
            continue

        try:
            decoded = line.decode("utf-8")
            data = json.loads(decoded)
            yield data.get("response", "")
        except Exception:
            continue


# =========================
# 🧠 NON-STREAM ANSWER
# =========================
def get_answer(context, question):

    full_response = ""

    for chunk in stream_answer(context, question):
        full_response += chunk

    return full_response.strip()
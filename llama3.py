import requests
import json

OLLAMA_URL = "http://192.168.50.33:11434/api/generate"

def ask_ollama_stream(prompt: str, model="llama3") -> str:
    payload = {"model": model, "prompt": prompt}

    result_text = ""

    with requests.post(OLLAMA_URL, json=payload, stream=True) as r:
        r.raise_for_status()

        for line in r.iter_lines():
            if not line:
                continue
            data = json.loads(line)
            chunk = data.get("response", "")
            result_text += chunk

    return result_text


if __name__ == "__main__":
    reply = ask_ollama_stream("Give me a short summary of DFIR triage steps.")
    print(reply)

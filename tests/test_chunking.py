"""Test chunking logic on a real Aon insights URL."""

import json

import requests
from bs4 import BeautifulSoup

URL = "https://www.aon.com/en/insights/articles/property-risk-in-natural-resources-the-shift-from-severity-to-duration"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}


def fetch_soup(url):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def extract_elements_before(soup):
    """BEFORE: naive extraction from full page."""
    soup = BeautifulSoup(str(soup), "html.parser")  # clone
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()

    content_area = soup.find("main") or soup.find("article") or soup
    elements = []
    for idx, tag in enumerate(content_area.find_all(["h1", "h2", "h3", "h4", "p", "li"])):
        text = tag.get_text(strip=True)
        if text and len(text) > 10:
            elements.append(
                {"type": "text", "id": str(idx), "content": text, "tag": tag.name}
            )
    return elements


def extract_elements_after(soup):
    """AFTER: focused extraction — only from article body sections."""
    soup = BeautifulSoup(str(soup), "html.parser")  # clone

    # Aon pages use section.column-content for article body text.
    # Collect content from those sections only, skipping carousels/sidebar/nav.
    content_sections = soup.find_all("section", class_="column-content")

    elements = []
    for section in content_sections:
        # Skip disclaimer/legal boilerplate sections
        if section.find("div", class_="disclaimer-block__block"):
            continue
        for tag in section.find_all(["h1", "h2", "h3", "h4", "p", "li"]):
            text = tag.get_text(strip=True)
            if text and len(text) > 10:
                elements.append(
                    {
                        "type": "text",
                        "id": str(len(elements)),
                        "content": text,
                        "tag": tag.name,
                    }
                )
    return elements


def merge_chunks(elements, max_chunk_chars=2000, overlap_chars=200):
    parsed_json = json.dumps({"document": {"elements": elements}})
    parsed_dict = json.loads(parsed_json)
    texts = [
        e["content"].strip()
        for e in parsed_dict["document"]["elements"]
        if e.get("type") == "text" and e.get("content", "").strip()
    ]

    chunks = []
    current_chunk = []
    current_len = 0

    for text in texts:
        text_len = len(text)
        if current_len > 0 and current_len + text_len + 1 > max_chunk_chars:
            chunk_text = "\n".join(current_chunk)
            chunks.append(chunk_text)
            overlap_text = (
                chunk_text[-overlap_chars:] if len(chunk_text) > overlap_chars else ""
            )
            current_chunk = [overlap_text, text] if overlap_text else [text]
            current_len = len(overlap_text) + text_len + 1
        else:
            current_chunk.append(text)
            current_len += text_len + 1

    if current_chunk:
        chunks.append("\n".join(current_chunk))

    return texts, chunks


if __name__ == "__main__":
    print(f"Fetching: {URL}\n")
    soup = fetch_soup(URL)

    # --- BEFORE ---
    before_elements = extract_elements_before(soup)
    before_texts, before_chunks = merge_chunks(before_elements)
    print("=" * 60)
    print("BEFORE (naive extraction)")
    print("=" * 60)
    print(f"  Raw elements: {len(before_elements)}")
    print(f"  Merged chunks: {len(before_chunks)}")
    print()
    for i, chunk in enumerate(before_chunks):
        print(f"  Chunk {i}: {len(chunk)} chars | {chunk[:80]}...")
    print()

    # --- AFTER ---
    after_elements = extract_elements_after(soup)
    after_texts, after_chunks = merge_chunks(after_elements)
    print("=" * 60)
    print("AFTER (filtered extraction)")
    print("=" * 60)
    print(f"  Raw elements: {len(after_elements)}")
    print(f"  Merged chunks: {len(after_chunks)}")
    print()
    print("--- Raw Elements ---")
    for i, el in enumerate(after_elements):
        preview = (
            el["content"][:120] + "..." if len(el["content"]) > 120 else el["content"]
        )
        print(f"  [{i}] <{el['tag']}> ({len(el['content'])} chars): {preview}")
    print()
    for i, chunk in enumerate(after_chunks):
        preview = chunk[:200] + "..." if len(chunk) > 200 else chunk
        print(f"--- Chunk {i} ({len(chunk)} chars) ---")
        print(preview)
        print()

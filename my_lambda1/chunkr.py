import os
import asyncio
from chunkr_ai import Chunkr
from chunkr_ai.models import (
    Configuration,
    SegmentationStrategy,
    ErrorHandlingStrategy,
)
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# -------------------------
# Load env
# -------------------------
load_dotenv()
CHUNKR_API_KEY = os.getenv("CHUNKR_API_KEY")

if not CHUNKR_API_KEY:
    raise ValueError("CHUNKR_API_KEY missing")

# -------------------------
# Clean text extractor
# -------------------------
def extract_text(chunk):
    text = ""

    if getattr(chunk, "content", None):
        text = chunk.content

    elif getattr(chunk, "html", None):
        text = chunk.html

    elif getattr(chunk, "segments", None):
        texts = [
            seg.content for seg in chunk.segments
            if getattr(seg, "content", None)
        ]
        text = "\n".join(texts)

    # Remove HTML
    if "<" in text and ">" in text:
        soup = BeautifulSoup(text, "html.parser")
        text = soup.get_text(separator="\n")

    return text.strip()

# -------------------------
# Page number extractor
# -------------------------
def get_page_number(chunk):
    if hasattr(chunk, "metadata") and chunk.metadata:
        page = chunk.metadata.get("page_number")
        if page is not None:
            return int(page)
    return None

# -------------------------
# MAIN: Strict page mode
# -------------------------
async def extract_pages_strict(filepath):
    client = Chunkr(api_key=CHUNKR_API_KEY)

    try:
        config = Configuration(
            segmentation_strategy=SegmentationStrategy.PAGE,
            error_handling=ErrorHandlingStrategy.CONTINUE,
            expires_in=3600,
            segment_processing={
                "page": {
                    "strategy": "Ignore",   # 🔥 STRICT PAGE MODE
                    "format": "Markdown",
                    "crop_image": "All",
                }
            },
        )

        print(f"🔹 Uploading: {filepath}")
        task = await client.upload(filepath, config)

        print("🔹 Processing...")
        task = await task.poll()

        chunks = getattr(task.output, "chunks", [])

        pages = {}

        for chunk in chunks:
            page_no = get_page_number(chunk)
            if page_no is None:
                continue

            text = extract_text(chunk)
            if not text:
                continue

            pages[page_no] = text

        return pages, len(pages)

    finally:
        await client.close()


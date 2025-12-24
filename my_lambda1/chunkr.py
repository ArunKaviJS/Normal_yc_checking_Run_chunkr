import os
import asyncio
from chunkr_ai import Chunkr
from chunkr_ai.models import (
    Configuration,
    LlmProcessing,
    FallbackStrategy,
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
    raise ValueError("❌ CHUNKR_API_KEY not found in environment variables")

# -------------------------
# Helper: Extract clean text
# -------------------------
def extract_text_from_chunk(chunk_obj):
    """
    Extract clean text from a Chunkr chunk safely.
    """
    chunk_text = ""

    # Priority order
    if getattr(chunk_obj, "llm", None):
        chunk_text = chunk_obj.llm

    elif getattr(chunk_obj, "content", None):
        chunk_text = chunk_obj.content

    elif getattr(chunk_obj, "segments", None):
        texts = []
        for seg in chunk_obj.segments:
            if getattr(seg, "content", None):
                texts.append(seg.content)
        chunk_text = "\n".join(texts)

    elif getattr(chunk_obj, "html", None):
        chunk_text = chunk_obj.html

    # Strip HTML if present
    if "<" in chunk_text and ">" in chunk_text:
        soup = BeautifulSoup(chunk_text, "html.parser")
        chunk_text = soup.get_text(separator="\n")

    return chunk_text.strip()


# -------------------------
# Helper: Correct page number extraction
# -------------------------
def get_page_number(chunk_obj, default_page=1):
    """
    Extract real page number from Chunkr metadata.
    """

    # 1️⃣ Chunk-level metadata
    if hasattr(chunk_obj, "metadata") and chunk_obj.metadata:
        page = chunk_obj.metadata.get("page_number")
        if page is not None:
            return int(page)

    # 2️⃣ Segment-level metadata
    if hasattr(chunk_obj, "segments"):
        for seg in chunk_obj.segments:
            if hasattr(seg, "metadata") and seg.metadata:
                page = seg.metadata.get("page_number")
                if page is not None:
                    return int(page)

    return default_page


# -------------------------
# Main async processor
# -------------------------
async def process_file_async(filepath):
    client = Chunkr(api_key=CHUNKR_API_KEY)

    # 🔥 Page-accurate configuration
    config = Configuration(
        segmentation_strategy=SegmentationStrategy.PAGE,
        error_handling=ErrorHandlingStrategy.CONTINUE,
        llm_processing=LlmProcessing(
            llm_model_id="gemini-pro-2.5",
            fallback_strategy=FallbackStrategy.none(),
            temperature=0.0,
        ),
        expires_in=3600,
        segment_processing={
            "page": {
                "strategy": "Auto",        # ✅ preserves physical pages
                "format": "Markdown",
                "crop_image": "All",
                "extended_context": False,
                "description": False,
            }
        },
    )

    print(f"🔹 Uploading file to Chunkr: {filepath}")
    task = await client.upload(filepath, config)

    print("🔹 Processing document...")
    task = await task.poll()

    chunks = getattr(task.output, "chunks", [])

    if not chunks:
        print("⚠️ No chunks returned from Chunkr")
        return "", 0

    # -------------------------
    # Group text by page
    # -------------------------
    pages = {}

    for chunk in chunks:
        page_no = get_page_number(chunk)
        text = extract_text_from_chunk(chunk)

        if not text:
            continue

        pages.setdefault(page_no, []).append(text)

    # -------------------------
    # Build final ordered output
    # -------------------------
    final_pages = []

    for page_no in sorted(pages.keys()):
        # remove duplicates while preserving order
        unique_text = list(dict.fromkeys(pages[page_no]))
        page_text = "\n\n".join(unique_text).strip()

        labeled_page = (
            f"===== CHUNKR PAGE NUMBER {page_no} =====\n"
            f"{page_text}"
        )
        final_pages.append(labeled_page)

    await client.close()

    all_text = "\n\n".join(final_pages)
    page_count = len(final_pages)

    return all_text, page_count

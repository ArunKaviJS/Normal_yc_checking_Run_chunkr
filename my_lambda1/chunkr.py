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

# -------------------------
# Helper: Extract clean text
# -------------------------
def extract_text_from_chunk(chunk_obj):
    """
    Extract clean text from a Chunkr chunk safely.
    """
    chunk_text = ""

    if hasattr(chunk_obj, "llm") and chunk_obj.llm:
        chunk_text = chunk_obj.llm

    elif hasattr(chunk_obj, "content") and chunk_obj.content:
        chunk_text = chunk_obj.content

    elif hasattr(chunk_obj, "segments") and chunk_obj.segments:
        texts = []
        for seg in chunk_obj.segments:
            if hasattr(seg, "content") and seg.content:
                texts.append(seg.content)
        chunk_text = "\n".join(texts)

    elif hasattr(chunk_obj, "html") and chunk_obj.html:
        chunk_text = chunk_obj.html

    # Remove HTML tags if present
    if "<" in chunk_text and ">" in chunk_text:
        soup = BeautifulSoup(chunk_text, "html.parser")
        chunk_text = soup.get_text(separator="\n")

    return chunk_text.strip()


# -------------------------
# Helper: Get real page number
# -------------------------
def get_page_number(chunk_obj, default_page=1):
    """
    Safely extract page number from chunk or its segments.
    """

    # 1️⃣ Direct chunk page number
    if hasattr(chunk_obj, "page_number") and chunk_obj.page_number is not None:
        return int(chunk_obj.page_number)

    # 2️⃣ Segment-level page number
    if hasattr(chunk_obj, "segments"):
        for seg in chunk_obj.segments:
            if hasattr(seg, "page_number") and seg.page_number is not None:
                return int(seg.page_number)

    return default_page


# -------------------------
# Main async processor
# -------------------------
async def process_file_async(filepath):
    client = Chunkr(api_key=CHUNKR_API_KEY)

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
                "crop_image": "All",
                "format": "Markdown",
                "strategy": "LLM",
                "extended_context": True,
                "description": True,
            }
        },
    )

    print(f"🔹 Uploading file to Chunkr: {filepath}")
    task = await client.upload(filepath, config)

    print("🔹 Processing document...")
    task = await task.poll()

    chunks = task.output.chunks if hasattr(task.output, "chunks") else []

    # -------------------------
    # Group content by page
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
        page_text = "\n\n".join(dict.fromkeys(pages[page_no])).strip()
        labeled_page = f"===== CHUNKR PAGE NUMBER {page_no} =====\n{page_text}"
        final_pages.append(labeled_page)

    await client.close()

    all_text = "\n\n".join(final_pages)
    page_count = len(final_pages)

    return all_text, page_count


# -------------------------

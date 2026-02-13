"""
ChefAgent - MCP Server Tools
DuckDuckGo ile yemek tarifi arama ve Crawl4AI ile web scraping araçları.
"""

import asyncio
import json
import logging
from typing import Optional

import redis.asyncio as aioredis
from ddgs import DDGS
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig

from config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_SCRAPE_CACHE_TTL

logger = logging.getLogger("chef_agent.tools")

# ─── Redis bağlantısı (lazy init) ───
_redis_client: Optional[aioredis.Redis] = None


async def get_redis() -> aioredis.Redis:
    """Redis bağlantısını lazy olarak oluşturur ve döner."""
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
        )
    return _redis_client


async def close_redis() -> None:
    """Redis bağlantısını kapatır."""
    global _redis_client
    if _redis_client:
        await _redis_client.close()
        _redis_client = None


# ════════════════════════════════════════════════════════════════
# Tool 1: search_recipes  –  DuckDuckGo ile yemek tarifi arama
# ════════════════════════════════════════════════════════════════

async def search_recipes(query: str, max_results: int = 5) -> str:
    """
    DuckDuckGo üzerinden yemek tarifi arar.

    Args:
        query: Aranacak yemek tarifi sorgusu (örn. "karnıyarık tarifi").
        max_results: Döndürülecek maksimum sonuç sayısı (varsayılan: 5).

    Returns:
        JSON formatında arama sonuçları listesi. Her sonuç title, url ve snippet içerir.
    """
    try:
        logger.info(f"🔍 DuckDuckGo araması: '{query}' (max: {max_results})")

        # "tarifi" anahtar kelimesini ekleyerek yemek odaklı arama yap
        search_query = f"{query} tarifi"

        # DDGS sync olduğu için executor'da çalıştırıyoruz
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            None,
            lambda: list(DDGS().text(search_query, max_results=max_results))
        )

        # Sonuçları düzenle
        formatted_results = []
        for r in results:
            formatted_results.append({
                "title": r.get("title", ""),
                "url": r.get("href", ""),
                "snippet": r.get("body", ""),
            })

        result_json = json.dumps(formatted_results, ensure_ascii=False, indent=2)
        logger.info(f"✅ {len(formatted_results)} sonuç bulundu")
        return result_json

    except Exception as e:
        error_msg = f"Arama sırasında hata oluştu: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return json.dumps({"error": error_msg}, ensure_ascii=False)


# ════════════════════════════════════════════════════════════════
# Tool 2: scrape_recipe  –  Crawl4AI ile tarif sayfasını çekme
# ════════════════════════════════════════════════════════════════

async def scrape_recipe(url: str) -> str:
    """
    Verilen URL'deki yemek tarifi sayfasını Markdown formatında çeker.
    Sonuçlar Redis'te cache'lenir (TTL: 1 saat).

    Args:
        url: Tarif sayfasının URL'si.

    Returns:
        Sayfanın Markdown formatında içeriği.
    """
    try:
        logger.info(f"🌐 Sayfa çekiliyor: {url}")

        # ── Redis cache kontrolü ──
        redis_client = await get_redis()
        cache_key = f"scrape:{url}"

        cached = await redis_client.get(cache_key)
        if cached:
            logger.info("💾 Cache'den döndürülüyor")
            return cached

        # ── Crawl4AI ile sayfayı çek ──
        config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS)

        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url=url, config=config)

            if not result.success:
                error_msg = f"Sayfa çekilemedi: {result.error_message}"
                logger.error(f"❌ {error_msg}")
                return json.dumps({"error": error_msg}, ensure_ascii=False)

            # Markdown içeriğini al (çok uzunsa kırp)
            markdown_content = result.markdown or ""
            if len(markdown_content) > 8000:
                markdown_content = markdown_content[:8000] + "\n\n... (içerik kırpıldı)"

            # ── Redis'e cache'le ──
            await redis_client.setex(cache_key, REDIS_SCRAPE_CACHE_TTL, markdown_content)
            logger.info(f"✅ Sayfa başarıyla çekildi ({len(markdown_content)} karakter)")

            return markdown_content

    except Exception as e:
        error_msg = f"Sayfa çekme sırasında hata oluştu: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return json.dumps({"error": error_msg}, ensure_ascii=False)


# ════════════════════════════════════════════════════════════════
# Tool Registry  –  Araç adından fonksiyona eşleme
# ════════════════════════════════════════════════════════════════

TOOL_FUNCTIONS = {
    "search_recipes": search_recipes,
    "scrape_recipe": scrape_recipe,
}


async def execute_tool(name: str, args: dict) -> str:
    """
    Araç adını ve argümanlarını alır, ilgili fonksiyonu çalıştırır.

    Args:
        name: Araç adı ("search_recipes" veya "scrape_recipe").
        args: Araç fonksiyonuna geçilecek argümanlar.

    Returns:
        Araç fonksiyonunun döndürdüğü string sonuç.
    """
    func = TOOL_FUNCTIONS.get(name)
    if func is None:
        return json.dumps({"error": f"Bilinmeyen araç: {name}"}, ensure_ascii=False)

    logger.info(f"🛠️  Araç çalıştırılıyor: {name}({args})")
    return await func(**args)

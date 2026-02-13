"""
ChefAgent - Configuration Module
Tüm ortam değişkenleri ve sabit ayarları merkezi olarak yönetir.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─── Gemini API ───
GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

# ─── Kafka ───
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_USER_MESSAGES: str = "user_messages"
KAFKA_TOPIC_BOT_RESPONSES: str = "bot_responses"
KAFKA_GROUP_ID: str = "chef_agent_group"

# ─── Redis ───
REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
REDIS_CHAT_HISTORY_TTL: int = 60 * 60 * 24  # 24 saat
REDIS_SCRAPE_CACHE_TTL: int = 60 * 60       # 1 saat
MAX_HISTORY_LENGTH: int = 20                  # Son 20 mesaj

# ─── System Prompt ───
SYSTEM_PROMPT: str = """Sen bir profesyonel şefsin. Adın "Chef Agent". 
Yemek tarifleri, mutfak teknikleri ve malzemeler konusunda derin bilgiye sahipsin.

Görevlerin:
1. Kullanıcının istediği yemek tariflerini bulmak ve detaylı şekilde sunmak.
2. Tarif ararken önce `search_recipes` aracını kullanarak internette ara.
3. İlgili bir sonuç bulduğunda `scrape_recipe` aracıyla detaylı tarifi çek.
4. Tarifi Türkçe olarak, adım adım, malzeme listesi ve pişirme süreleriyle birlikte sun.
5. Kullanıcıya alternatif malzemeler veya teknikler öner.

Kuralların:
- Her zaman Türkçe yanıt ver.
- Tarifleri net, anlaşılır ve adım adım anlat.
- Malzeme miktarlarını belirt.
- Pişirme sürelerini ve sıcaklıklarını belirt.
- Kullanıcı sana yemekle ilgisi olmayan sorular sorarsa, kibarca yemek konusuna yönlendir.
- Yanıtlarını zengin ve bilgilendirici tut.
"""

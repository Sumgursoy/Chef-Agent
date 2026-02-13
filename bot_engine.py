"""
ChefAgent - Bot Engine (Ana Orchestrator)
Kafka consumer/producer, Redis chat geçmişi ve Gemini Function Calling döngüsü.
"""

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Optional

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from google import genai
from google.genai import types

from config import (
    GEMINI_API_KEY,
    GEMINI_MODEL,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC_BOT_RESPONSES,
    KAFKA_TOPIC_USER_MESSAGES,
    MAX_HISTORY_LENGTH,
    REDIS_CHAT_HISTORY_TTL,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PORT,
    SYSTEM_PROMPT,
)
from mcp_server_tools import close_redis, execute_tool

# ─── Logging ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(name)-20s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("chef_agent.engine")


# ════════════════════════════════════════════════════════════════
# Gemini Tool Definitions  –  Function Declarations
# ════════════════════════════════════════════════════════════════

SEARCH_RECIPES_DECL = types.FunctionDeclaration(
    name="search_recipes",
    description="DuckDuckGo üzerinden yemek tarifi arar. Kullanıcının istediği yemek ile ilgili sonuçlar döner.",
    parameters_json_schema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Aranacak yemek tarifi sorgusu, örn: 'karnıyarık tarifi'",
            },
            "max_results": {
                "type": "integer",
                "description": "Döndürülecek maksimum sonuç sayısı (varsayılan: 5)",
            },
        },
        "required": ["query"],
    },
)

SCRAPE_RECIPE_DECL = types.FunctionDeclaration(
    name="scrape_recipe",
    description="Verilen URL'deki yemek tarifi sayfasını Markdown formatında çeker. Arama sonuçlarından gelen URL'ler için kullanılır.",
    parameters_json_schema={
        "type": "object",
        "properties": {
            "url": {
                "type": "string",
                "description": "Çekilecek tarif sayfasının tam URL'si",
            },
        },
        "required": ["url"],
    },
)

GEMINI_TOOLS = types.Tool(function_declarations=[SEARCH_RECIPES_DECL, SCRAPE_RECIPE_DECL])


# ════════════════════════════════════════════════════════════════
# Redis Chat History Manager
# ════════════════════════════════════════════════════════════════

class ChatHistoryManager:
    """Redis üzerinde kullanıcı chat geçmişini yönetir."""

    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

    def _key(self, user_id: str) -> str:
        return f"chat:{user_id}"

    async def get_history(self, user_id: str) -> list[dict]:
        """Kullanıcının chat geçmişini Redis'ten çeker."""
        raw = await self.redis.get(self._key(user_id))
        if raw is None:
            return []
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return []

    async def add_message(self, user_id: str, role: str, text: str) -> None:
        """Chat geçmişine yeni bir mesaj ekler."""
        history = await self.get_history(user_id)
        history.append({
            "role": role,
            "text": text,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        # Son N mesajı tut
        if len(history) > MAX_HISTORY_LENGTH:
            history = history[-MAX_HISTORY_LENGTH:]

        await self.redis.setex(
            self._key(user_id),
            REDIS_CHAT_HISTORY_TTL,
            json.dumps(history, ensure_ascii=False),
        )

    async def build_contents(self, user_id: str, new_message: str) -> list[types.Content]:
        """
        Redis'teki geçmişi + yeni mesajı Gemini'nin beklediği
        Content listesine dönüştürür.
        """
        history = await self.get_history(user_id)
        contents: list[types.Content] = []

        for msg in history:
            role = "user" if msg["role"] == "user" else "model"
            contents.append(
                types.Content(
                    role=role,
                    parts=[types.Part.from_text(text=msg["text"])],
                )
            )

        # Yeni kullanıcı mesajını ekle
        contents.append(
            types.Content(
                role="user",
                parts=[types.Part.from_text(text=new_message)],
            )
        )

        return contents


# ════════════════════════════════════════════════════════════════
# Gemini Orchestrator  –  Function Calling Döngüsü
# ════════════════════════════════════════════════════════════════

class GeminiOrchestrator:
    """Gemini API ile iletişimi ve function calling döngüsünü yönetir."""

    MAX_TOOL_ROUNDS = 5  # Sonsuz döngüyü önlemek için

    def __init__(self):
        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self.config = types.GenerateContentConfig(
            tools=[GEMINI_TOOLS],
            system_instruction=SYSTEM_PROMPT,
            automatic_function_calling=types.AutomaticFunctionCallingConfig(
                disable=True  # Manuel tool calling
            ),
        )

    async def generate_response(self, contents: list[types.Content]) -> str:
        """
        Gemini'ye mesajları gönderir ve function calling döngüsünü yönetir.
        Model bir metin yanıtı verene kadar araç çağrılarını işler.
        """
        current_contents = list(contents)
        round_count = 0

        while round_count < self.MAX_TOOL_ROUNDS:
            round_count += 1
            logger.info(f"🤖 Gemini çağrısı (round {round_count})...")

            try:
                # Gemini API çağrısını executor'da çalıştır (sync SDK)
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.client.models.generate_content(
                        model=GEMINI_MODEL,
                        contents=current_contents,
                        config=self.config,
                    ),
                )
            except Exception as e:
                logger.error(f"❌ Gemini API hatası: {e}")
                return f"Üzgünüm, bir hata oluştu: {str(e)}"

            # ── Function call var mı kontrol et ──
            if response.function_calls:
                logger.info(f"🔧 {len(response.function_calls)} araç çağrısı algılandı")

                # Model'in function call yanıtını contents'e ekle
                current_contents.append(response.candidates[0].content)

                # Her bir tool call'u çalıştır
                tool_response_parts = []
                for fc in response.function_calls:
                    tool_name = fc.name
                    tool_args = dict(fc.args) if fc.args else {}

                    logger.info(f"  → {tool_name}({tool_args})")

                    # Aracı çalıştır
                    tool_result = await execute_tool(tool_name, tool_args)

                    # Sonucu part olarak hazırla
                    tool_response_parts.append(
                        types.Part.from_function_response(
                            name=tool_name,
                            response={"result": tool_result},
                        )
                    )

                # Tüm tool sonuçlarını tek bir Content olarak ekle
                current_contents.append(
                    types.Content(role="tool", parts=tool_response_parts)
                )

                # Döngüye devam – model tekrar yanıt verecek
                continue

            # ── Metin yanıtı var ──
            if response.text:
                logger.info("✅ Gemini metin yanıtı oluşturdu")
                return response.text

            # ── Beklenmeyen durum ──
            logger.warning("⚠️ Gemini'den ne tool call ne metin geldi")
            return "Üzgünüm, yanıt oluşturulamadı. Lütfen tekrar deneyin."

        logger.warning("⚠️ Maksimum araç çağrısı turuna ulaşıldı")
        return "Üzgünüm, tarif aranırken çok fazla adım gerekti. Lütfen daha spesifik bir soru sorun."


# ════════════════════════════════════════════════════════════════
# Kafka Event Loop  –  Ana Döngü
# ════════════════════════════════════════════════════════════════

class ChefAgentEngine:
    """
    Ana event-driven motor.
    Kafka'dan mesajları alır, Gemini ile işler, sonucu geri yazar.
    """

    def __init__(self):
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self.redis: Optional[aioredis.Redis] = None
        self.history_manager: Optional[ChatHistoryManager] = None
        self.orchestrator = GeminiOrchestrator()
        self._running = False

    async def start(self) -> None:
        """Tüm bağlantıları kurar ve event loop'u başlatır."""
        logger.info("🚀 ChefAgent Engine başlatılıyor...")

        # ── Redis ──
        self.redis = aioredis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True
        )
        try:
            await self.redis.ping()
            logger.info("✅ Redis bağlantısı başarılı")
        except Exception as e:
            logger.error(f"❌ Redis bağlantı hatası: {e}")
            raise

        self.history_manager = ChatHistoryManager(self.redis)

        # ── Kafka Consumer ──
        self.consumer = AIOKafkaConsumer(
            KAFKA_TOPIC_USER_MESSAGES,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
        )

        # ── Kafka Producer ──
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )

        # Kafka bağlantılarını başlat (retry ile)
        await self._connect_kafka_with_retry()

        self._running = True
        logger.info("=" * 60)
        logger.info("🍳 ChefAgent Engine hazır! Mesajları bekliyorum...")
        logger.info("=" * 60)

        # ── Event Loop ──
        await self._consume_loop()

    async def _connect_kafka_with_retry(self, max_retries: int = 10, delay: float = 3.0) -> None:
        """Kafka bağlantısını retry mekanizmasıyla kurar."""
        for attempt in range(1, max_retries + 1):
            try:
                await self.consumer.start()
                await self.producer.start()
                logger.info("✅ Kafka bağlantısı başarılı")
                return
            except Exception as e:
                logger.warning(
                    f"⚠️ Kafka bağlantı denemesi {attempt}/{max_retries} başarısız: {e}"
                )
                if attempt < max_retries:
                    await asyncio.sleep(delay)
                else:
                    logger.error("❌ Kafka'ya bağlanılamadı!")
                    raise

    async def _consume_loop(self) -> None:
        """Kafka'dan gelen mesajları sürekli dinler ve işler."""
        try:
            async for message in self.consumer:
                if not self._running:
                    break

                try:
                    await self._process_message(message.value)
                except Exception as e:
                    logger.error(f"❌ Mesaj işleme hatası: {e}", exc_info=True)

        except asyncio.CancelledError:
            logger.info("🛑 Consumer loop iptal edildi")
        except Exception as e:
            logger.error(f"❌ Consumer loop hatası: {e}", exc_info=True)

    async def _process_message(self, data: dict) -> None:
        """Tek bir kullanıcı mesajını işler."""
        user_id = data.get("user_id", "anonymous")
        user_message = data.get("message", "")
        timestamp = data.get("timestamp", datetime.now(timezone.utc).isoformat())

        logger.info(f"📩 Mesaj alındı | user: {user_id} | mesaj: {user_message[:80]}")

        if not user_message.strip():
            logger.warning("⚠️ Boş mesaj atlandı")
            return

        # 1) Redis'ten geçmişi al ve Gemini contents oluştur
        contents = await self.history_manager.build_contents(user_id, user_message)

        # 2) Gemini function calling döngüsünü çalıştır
        bot_response = await self.orchestrator.generate_response(contents)

        # 3) Geçmişe kaydet
        await self.history_manager.add_message(user_id, "user", user_message)
        await self.history_manager.add_message(user_id, "assistant", bot_response)

        # 4) Yanıtı Kafka'ya bas
        response_payload = {
            "user_id": user_id,
            "response": bot_response,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        await self.producer.send_and_wait(KAFKA_TOPIC_BOT_RESPONSES, response_payload)
        logger.info(f"📤 Yanıt gönderildi | user: {user_id} | uzunluk: {len(bot_response)}")

    async def stop(self) -> None:
        """Tüm bağlantıları temiz şekilde kapatır."""
        logger.info("🛑 ChefAgent Engine kapatılıyor...")
        self._running = False

        if self.consumer:
            await self.consumer.stop()
            logger.info("  → Kafka Consumer durduruldu")

        if self.producer:
            await self.producer.stop()
            logger.info("  → Kafka Producer durduruldu")

        if self.redis:
            await self.redis.close()
            logger.info("  → Redis bağlantısı kapatıldı")

        await close_redis()  # Tool modülündeki redis bağlantısı
        logger.info("✅ ChefAgent Engine kapatıldı")


# ════════════════════════════════════════════════════════════════
# Main Entry Point
# ════════════════════════════════════════════════════════════════

async def main():
    engine = ChefAgentEngine()

    # Graceful shutdown handler
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()

    def _signal_handler():
        logger.info("📡 Shutdown sinyali alındı")
        shutdown_event.set()

    # Windows'ta SIGTERM desteklenmez, sadece SIGINT kullanıyoruz
    try:
        loop.add_signal_handler(signal.SIGINT, _signal_handler)
        loop.add_signal_handler(signal.SIGTERM, _signal_handler)
    except NotImplementedError:
        # Windows'ta signal handler alternatifi
        signal.signal(signal.SIGINT, lambda s, f: _signal_handler())

    # Engine'i başlat ve shutdown bekle
    try:
        engine_task = asyncio.create_task(engine.start())
        shutdown_task = asyncio.create_task(shutdown_event.wait())

        done, pending = await asyncio.wait(
            [engine_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    finally:
        await engine.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Güle güle!")
        sys.exit(0)

"""
ChefAgent - Test Producer & Response Listener
Kafka'ya kullanıcı mesajı gönderir ve bot yanıtlarını dinler.
"""

import asyncio
import json
import sys
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_BOT_RESPONSES,
    KAFKA_TOPIC_USER_MESSAGES,
)

USER_ID = f"test_user_{uuid.uuid4().hex[:6]}"


async def response_listener(stop_event: asyncio.Event) -> None:
    """bot_responses topic'ini dinler ve yanıtları ekrana basar."""
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_BOT_RESPONSES,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"producer_listener_{USER_ID}",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
    )

    try:
        await consumer.start()
        print("👂 Bot yanıtları dinleniyor...\n")

        async for message in consumer:
            data = message.value

            # Sadece bu kullanıcıya ait yanıtları göster
            if data.get("user_id") == USER_ID:
                print("\n" + "=" * 60)
                print("🍳 Chef Agent Yanıtı:")
                print("-" * 60)
                print(data.get("response", "(boş yanıt)"))
                print("=" * 60)
                print(f"\n💬 Mesajınız ({USER_ID}): ", end="", flush=True)

            if stop_event.is_set():
                break

    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()


async def message_sender() -> None:
    """Konsoldan kullanıcı mesajı alır ve Kafka'ya gönderir."""
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    stop_event = asyncio.Event()

    # Kafka'ya bağlan (retry ile)
    for attempt in range(10):
        try:
            await producer.start()
            break
        except Exception as e:
            print(f"⚠️  Kafka bağlantı denemesi {attempt + 1}/10: {e}")
            await asyncio.sleep(3)
    else:
        print("❌ Kafka'ya bağlanılamadı!")
        return

    # Response listener'ı arka planda başlat
    listener_task = asyncio.create_task(response_listener(stop_event))

    print("=" * 60)
    print("🍳 ChefAgent - Yemek Tarifi Chatbot")
    print(f"   Kullanıcı ID: {USER_ID}")
    print("   Çıkmak için 'q' veya 'quit' yazın")
    print("=" * 60)

    try:
        while True:
            print(f"\n💬 Mesajınız ({USER_ID}): ", end="", flush=True)

            # Async input
            loop = asyncio.get_event_loop()
            user_input = await loop.run_in_executor(None, sys.stdin.readline)
            user_input = user_input.strip()

            if not user_input:
                continue

            if user_input.lower() in ("q", "quit", "exit", "çık"):
                print("\n👋 Güle güle!")
                break

            # Kafka'ya mesaj gönder
            payload = {
                "user_id": USER_ID,
                "message": user_input,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            await producer.send_and_wait(KAFKA_TOPIC_USER_MESSAGES, payload)
            print("📤 Mesaj gönderildi, yanıt bekleniyor...")

    except (KeyboardInterrupt, EOFError):
        print("\n👋 Güle güle!")
    finally:
        stop_event.set()
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            pass
        await producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(message_sender())
    except KeyboardInterrupt:
        print("\n👋 Güle güle!")
        sys.exit(0)

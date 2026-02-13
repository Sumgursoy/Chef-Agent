"""
ChefAgent - Streamlit Chat Arayüzü
Kafka üzerinden bot_engine ile iletişim kuran modern sohbet UI.
"""

import json
import queue
import threading
import time
import uuid
from datetime import datetime, timezone

import streamlit as st
from kafka import KafkaProducer, KafkaConsumer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_BOT_RESPONSES,
    KAFKA_TOPIC_USER_MESSAGES,
)

# ─── Sayfa Ayarları ───
st.set_page_config(
    page_title="🍳 Chef Agent",
    page_icon="🍳",
    layout="centered",
)

# ─── Custom CSS ───
st.markdown("""
<style>
    /* Ana arka plan */
    .stApp {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    }

    /* Başlık stili */
    .chef-header {
        text-align: center;
        padding: 1.5rem 0;
        margin-bottom: 1rem;
    }
    .chef-header h1 {
        font-size: 2.5rem;
        background: linear-gradient(135deg, #f39c12, #e74c3c, #f39c12);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
        margin-bottom: 0.3rem;
    }
    .chef-header p {
        color: #a0a0b0;
        font-size: 1rem;
    }

    /* Sohbet mesaj stilleri */
    .stChatMessage {
        border-radius: 12px !important;
        margin-bottom: 0.5rem !important;
    }

    /* Sidebar stili */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #16213e, #1a1a2e);
    }
    [data-testid="stSidebar"] .stMarkdown h3 {
        color: #f39c12;
    }

    /* Input box */
    .stChatInput > div {
        border-radius: 12px !important;
    }

    /* Durum göstergesi */
    .status-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    .status-connected {
        background: rgba(46, 204, 113, 0.2);
        color: #2ecc71;
        border: 1px solid rgba(46, 204, 113, 0.3);
    }
    .status-disconnected {
        background: rgba(231, 76, 60, 0.2);
        color: #e74c3c;
        border: 1px solid rgba(231, 76, 60, 0.3);
    }
</style>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════
# Session State Başlatma
# ════════════════════════════════════════════════════════════════

if "user_id" not in st.session_state:
    st.session_state.user_id = f"user_{uuid.uuid4().hex[:8]}"

if "messages" not in st.session_state:
    st.session_state.messages = []

if "kafka_connected" not in st.session_state:
    st.session_state.kafka_connected = False

if "producer" not in st.session_state:
    st.session_state.producer = None


# ════════════════════════════════════════════════════════════════
# Thread-Safe Yanıt Kuyruğu (global, Streamlit rerun'lardan etkilenmez)
# ════════════════════════════════════════════════════════════════

# Bu global dict, arka plan thread'i tarafından doldurulur
if "response_queue" not in st.session_state:
    st.session_state.response_queue = queue.Queue()

if "listener_started" not in st.session_state:
    st.session_state.listener_started = False


def _background_listener(response_q: queue.Queue, user_id: str):
    """
    Arka planda sürekli çalışan Kafka consumer.
    bot_responses topic'ini dinler ve yanıtları kuyruğa koyar.
    """
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_BOT_RESPONSES,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"streamlit_{user_id}",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            consumer_timeout_ms=1000,  # 1 saniyede bir timeout
        )
    except Exception:
        return

    while True:
        try:
            # poll ile mesajları al (timeout olursa boş döner, döngü devam eder)
            raw_msgs = consumer.poll(timeout_ms=2000)
            for tp, messages in raw_msgs.items():
                for msg in messages:
                    data = msg.value
                    if data.get("user_id") == user_id:
                        response_q.put(data.get("response", ""))
        except Exception:
            time.sleep(1)


def ensure_listener_running():
    """Arka plan listener thread'inin çalıştığından emin ol."""
    if not st.session_state.listener_started:
        t = threading.Thread(
            target=_background_listener,
            args=(st.session_state.response_queue, st.session_state.user_id),
            daemon=True,
        )
        t.start()
        st.session_state.listener_started = True


# ════════════════════════════════════════════════════════════════
# Kafka Producer
# ════════════════════════════════════════════════════════════════

def init_kafka_producer():
    """Kafka Producer bağlantısı kur."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            request_timeout_ms=5000,
        )
        st.session_state.producer = producer
        st.session_state.kafka_connected = True
        return True
    except Exception:
        st.session_state.kafka_connected = False
        return False


def send_and_wait_response(message: str, user_id: str, timeout: float = 90.0) -> str:
    """Mesajı Kafka'ya gönder ve kuyruktaki yanıtı bekle."""
    # Producer hazır mı?
    if st.session_state.producer is None:
        if not init_kafka_producer():
            return "❌ Kafka bağlantısı kurulamadı. Docker servislerini kontrol edin."

    # Kuyruğu temizle (önceki stale mesajları at)
    while not st.session_state.response_queue.empty():
        try:
            st.session_state.response_queue.get_nowait()
        except queue.Empty:
            break

    # Mesajı gönder
    payload = {
        "user_id": user_id,
        "message": message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        st.session_state.producer.send(KAFKA_TOPIC_USER_MESSAGES, payload)
        st.session_state.producer.flush()
    except Exception as e:
        st.session_state.kafka_connected = False
        return f"❌ Mesaj gönderilemedi: {e}"

    # Yanıtı kuyruktan bekle
    try:
        response = st.session_state.response_queue.get(timeout=timeout)
        return response
    except queue.Empty:
        return "⏱️ Yanıt zaman aşımına uğradı. Bot engine çalışıyor mu kontrol edin."


# ════════════════════════════════════════════════════════════════
# Sidebar
# ════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("### ⚙️ Sistem Bilgisi")

    if st.session_state.kafka_connected:
        st.markdown(
            '<span class="status-badge status-connected">● Kafka Bağlı</span>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<span class="status-badge status-disconnected">● Kafka Bağlı Değil</span>',
            unsafe_allow_html=True,
        )

    st.markdown(f"**Kullanıcı ID:** `{st.session_state.user_id}`")
    st.markdown(f"**Kafka:** `{KAFKA_BOOTSTRAP_SERVERS}`")

    st.divider()

    st.markdown("### 🍽️ Örnek Sorular")
    examples = [
        "Karnıyarık tarifi ver",
        "Pratik çorba tarifleri öner",
        "Cheesecake nasıl yapılır?",
        "Vejetaryen yemek önerileri",
    ]

    for ex in examples:
        if st.button(ex, key=f"ex_{ex}", use_container_width=True):
            st.session_state.example_input = ex

    st.divider()

    if st.button("🗑️ Sohbeti Temizle", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

    st.divider()
    st.markdown(
        "<p style='color:#555; font-size:0.75rem; text-align:center;'>"
        "ChefAgent v1.0 • Gemini + Kafka + Redis"
        "</p>",
        unsafe_allow_html=True,
    )


# ════════════════════════════════════════════════════════════════
# Ana Sohbet Alanı
# ════════════════════════════════════════════════════════════════

# Başlık
st.markdown(
    """
    <div class="chef-header">
        <h1>🍳 Chef Agent</h1>
        <p>Profesyonel şefiniz her zaman yanınızda. Bir tarif sorun!</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# Kafka bağlantısını ve listener'ı başlat
if not st.session_state.kafka_connected:
    init_kafka_producer()
ensure_listener_running()

# Mevcut mesajları göster
for msg in st.session_state.messages:
    avatar = "👤" if msg["role"] == "user" else "🍳"
    with st.chat_message(msg["role"], avatar=avatar):
        st.markdown(msg["content"])

# Örnek soru tıklandıysa input olarak kullan
default_input = st.session_state.pop("example_input", None)

# Chat input
prompt = st.chat_input("Bir yemek tarifi sorun...", key="chat_input")

if default_input:
    prompt = default_input

if prompt:
    # Kullanıcı mesajını ekle
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user", avatar="👤"):
        st.markdown(prompt)

    # Bot yanıtını bekle
    with st.chat_message("assistant", avatar="🍳"):
        with st.spinner("🔍 Tarif aranıyor, lütfen bekleyin..."):
            response_text = send_and_wait_response(
                prompt, st.session_state.user_id, timeout=90.0
            )
            st.markdown(response_text)

    # Bot yanıtını kaydet
    st.session_state.messages.append({"role": "assistant", "content": response_text})

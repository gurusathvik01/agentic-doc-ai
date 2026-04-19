from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.routes.chat import router as chat_router

# ==============================
# 🚀 APP INIT
# ==============================
app = FastAPI(title="Agentic Doc AI")

# ==============================
# 🔥 MIDDLEWARE
# ==============================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================
# 🔥 ROUTES
# ==============================
app.include_router(chat_router)

# ==============================
# 🏠 ROOT
# ==============================
@app.get("/")
def home():
    return {
        "message": "🚀 Agentic Doc AI Running",
        "endpoints": ["/chat", "/docs", "/health"]
    }

# ==============================
# ❤️ HEALTH CHECK
# ==============================
@app.get("/health")
def health():
    return {"status": "ok"}
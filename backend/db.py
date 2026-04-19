import os
import pandas as pd
from pymongo import MongoClient
import mysql.connector
from dotenv import load_dotenv

# =========================
# 🔥 LOAD ENV VARIABLES
# =========================
load_dotenv()

# =========================
# 🔐 MONGODB CONNECTION
# =========================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_db = mongo_client["agentic_ai"]
except Exception as e:
    print("MongoDB connection failed:", e)
    mongo_db = None


# =========================
# 📦 SAVE TO MONGODB
# =========================
def save_to_mongo(collection_name, data):
    if mongo_db is None:
        return

    collection = mongo_db[collection_name]

    if isinstance(data, pd.DataFrame):
        data = data.to_dict(orient="records")

    try:
        if isinstance(data, list) and data:
            collection.insert_many(data)
        elif data:
            collection.insert_one(data)
    except Exception as e:
        print("MongoDB Error:", e)


# =========================
# 🔥 MONGO QUERY
# =========================
def query_mongo(collection_name, query=None, pipeline=None):
    if mongo_db is None:
        return []

    try:
        collection = mongo_db[collection_name]

        if pipeline:
            data = list(collection.aggregate(pipeline))
        else:
            data = list(collection.find(query or {}, {"_id": 0}))

        return [{k.lower(): v for k, v in row.items()} for row in data]

    except Exception as e:
        print("Mongo Query Error:", e)
        return []


# =========================
# 🛢️ MYSQL CONNECTION
# =========================
def get_mysql_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD"),  # 🔥 PUT PASSWORD IN .env
            database=os.getenv("MYSQL_DB", "agentic_ai")
        )
        return conn

    except Exception as e:
        print("MySQL connection failed:", e)
        return None


# =========================
# 🔥 MYSQL QUERY
# =========================
def query_mysql(sql):
    conn = get_mysql_connection()

    if conn is None:
        return []

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql)
        result = cursor.fetchall()

        cursor.close()
        conn.close()

        return [{k.lower(): v for k, v in row.items()} for row in result]

    except Exception as e:
        print("MySQL Query Error:", e)
        return []


# =========================
# 📦 SAVE TO MYSQL
# =========================
def save_to_mysql(table_name, df: pd.DataFrame):
    conn = get_mysql_connection()

    if conn is None:
        return

    try:
        cursor = conn.cursor()

        cols = ", ".join([f"`{col}` TEXT" for col in df.columns])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({cols})")

        for _, row in df.iterrows():
            values = tuple(str(x) for x in row)
            placeholders = ", ".join(["%s"] * len(values))

            cursor.execute(
                f"INSERT INTO `{table_name}` VALUES ({placeholders})",
                values
            )

        conn.commit()

        cursor.close()
        conn.close()

    except Exception as e:
        print("MySQL Save Error:", e)


# =========================
# 📦 FETCH MYSQL (DF)
# =========================
def fetch_from_mysql(table_name):
    conn = get_mysql_connection()

    if conn is None:
        return pd.DataFrame()

    try:
        query = f"SELECT * FROM `{table_name}`"
        df = pd.read_sql(query, conn)

        df.columns = [c.lower() for c in df.columns]

        conn.close()
        return df

    except Exception as e:
        print("MySQL Fetch Error:", e)
        return pd.DataFrame()


# =========================
# 📊 CSV SUPPORT
# =========================
def query_csv(file_path):
    try:
        if not os.path.exists(file_path):
            return []

        df = pd.read_csv(file_path)

        df.columns = [c.lower() for c in df.columns]

        return df.to_dict(orient="records") if not df.empty else []

    except Exception as e:
        print("CSV Error:", e)
        return []


# =========================
# 📦 COLLECTION HELPERS
# =========================
def get_users_collection():
    if mongo_db is None:
        return None
    return mongo_db["users"]


def get_chat_collection():
    if mongo_db is None:
        return None
    return mongo_db["chat_history"]


users_collection = get_users_collection()
chat_collection = get_chat_collection()
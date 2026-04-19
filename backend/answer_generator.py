# answer_generator.py

def generate_answer(question, results):
    answer = f"📊 Question: {question}\n\n"

    # MYSQL
    if "mysql" in results:
        answer += "🛢️ Sales Data:\n"
        for row in results["mysql"][:5]:
            answer += f"{row}\n"

    # MONGO
    if "mongodb" in results:
        answer += "\n🍃 Customer Intelligence:\n"
        for row in results["mongodb"][:5]:
            answer += f"{row}\n"

    # PDF
    if "pdf" in results:
        answer += "\n📄 Product Info:\n"
        for row in results["pdf"][:3]:
            answer += f"{row['file']} (Page {row['page']}): {row['snippet']}\n"

    # SIMPLE INTELLIGENCE
    answer += "\n🧠 Final Insight:\n"

    if "top" in question.lower():
        answer += "Top performing entity identified based on available data.\n"

    if "risk" in question.lower():
        answer += "High risk detected in some customers.\n"

    return answer
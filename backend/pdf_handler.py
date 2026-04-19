import pdfplumber

def query_pdf():
    try:
        data = []

        with pdfplumber.open("backend/data/report.pdf") as pdf:
            for page in pdf.pages:
                text = page.extract_text()

                if not text:
                    continue

                lines = text.split("\n")

                for line in lines:
                    # Normalize line
                    line = line.strip()

                    if "purchased" not in line or "worth" not in line:
                        continue

                    parts = line.split(" purchased ")

                    if len(parts) != 2:
                        continue

                    # 🔥 FIXED NAME NORMALIZATION
                    name = parts[0].strip().upper()

                    rest = parts[1].split(" worth ")

                    if len(rest) != 2:
                        continue

                    product = rest[0].strip()

                    try:
                        amount = int(rest[1].strip())
                    except:
                        continue

                    data.append({
                        "FirstName": name,          # 🔥 normalized
                        "pdf_product": product,
                        "pdf_amount": amount
                    })

        print("✅ PDF DATA:", data)  # Debug (remove later)
        return data

    except Exception as e:
        print("❌ PDF ERROR:", e)
        return []
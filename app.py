import os
from PyPDF2 import PdfReader
import re
import streamlit as st
import logging
import weaviate

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Weaviate 클라이언트 설정
WEAVIATE_URL = "http://localhost:8080"
client = weaviate.Client(WEAVIATE_URL)

# Weaviate에 데이터 스키마 생성
def create_weaviate_schema():
    try:
        existing_classes = [cls['class'] for cls in client.schema.get()["classes"]]
        if "Document" in existing_classes:
            logger.info("Document 클래스가 이미 존재합니다. 스키마 생성을 건너뜁니다.")
            return

        schema = {
            "classes": [
                {
                    "class": "Document",
                    "description": "A document from PDF files",
                    "properties": [
                        {"name": "filename", "dataType": ["string"], "description": "The name of the PDF file"},
                        {"name": "content", "dataType": ["text"], "description": "The original content of the PDF"},
                        {"name": "processed_content", "dataType": ["text"], "description": "The processed content of the PDF"},
                        {"name": "category", "dataType": ["string"], "description": "The classified category of the document"},
                        {"name": "income_level", "dataType": ["int"], "description": "Income level from 1 to 10"},
                        {"name": "recommendation", "dataType": ["string"], "description": "Financial product recommendation"}
                    ]
                }
            ]
        }
        client.schema.create(schema)
    except Exception as e:
        logger.error(f"Weaviate 스키마 생성 중 오류: {e}")
        st.error("Weaviate 스키마를 생성할 수 없습니다. 오류를 확인해주세요.")

# Weaviate에서 카테고리별로 파일명만 조회
def get_filenames_by_category():
    try:
        query = """
        {
          Get {
            Document {
              filename
              category
            }
          }
        }
        """
        result = client.query.raw(query)
        documents = result.get("data", {}).get("Get", {}).get("Document", [])

        # 카테고리별로 파일명 분류 및 중복 제거
        categorized_filenames = {
            '적금': set(),
            '예금': set(),
            '대출': set(),
            '청년': set()
        }
        for doc in documents:
            category = doc.get("category", "청년")
            filename = doc.get("filename", "Unknown")
            categorized_filenames[category].add(filename)

        # set을 list로 변환
        return {category: list(filenames) for category, filenames in categorized_filenames.items()}
    except Exception as e:
        logger.error(f"Weaviate에서 데이터를 가져오는 중 오류: {e}")
        return {
            '적금': [],
            '예금': [],
            '대출': [],
            '청년': []
        }

# 텍스트 분류
def classify_product(text):
    # 가중치 설정
    keywords = {
        '적금': {
            'words': ['적금', '저축', '월 적립', '자동 이체', '정기', '납입', '출금 제한'],
            'weight': 2
        },
        '예금': {
            'words': ['예금', '정기예금', '거치', '이자', '파킹 통장', '단리', '복리', '고정 금리', '변동 금리'],
            'weight': 3
        },
        '대출': {
            'words': ['대출', '신용대출', '담보대출', '이자율', '원리금', '상환', '보증'],
            'weight': 5
        },
        '청년': {
            'words': ['청년', '청년내일저축계좌', '청년도약계좌', '청년희망적금'],
            'weight': 10
        }
    }

    category_scores = {category: 0 for category in keywords}

    # 적금 및 예금 키워드를 먼저 처리하여 청년 키워드가 있더라도 해당 카테고리로 분류되도록 함
    for category in ['적금', '예금', '대출']:
        for word in keywords[category]['words']:
            if word in text:
                category_scores[category] += keywords[category]['weight']

    # 청년 키워드는 마지막에 처리하여 우선순위를 낮춤
    for word in keywords['청년']['words']:
        if word in text and category_scores['적금'] == 0 and category_scores['예금'] == 0:
            category_scores['청년'] += keywords['청년']['weight']

    # 가장 높은 점수를 받은 카테고리 반환
    sorted_categories = sorted(category_scores.items(), key=lambda x: x[1], reverse=True)
    top_category, top_score = sorted_categories[0]
    return '청년' if top_score == 0 else top_category

# 소득분위 계산
def calculate_income_level(asset_size, monthly_salary):
    income_score = (asset_size / 1_000_000) + (monthly_salary / 10_000)
    income_level = min(10, max(1, int(income_score / 2)))
    return income_level

# 소득분위와 나이에 따라 금융 상품 추천
def classify_product_by_income_level(income_level, wants_loan, age):
    if 19 <= age <= 34:
        return "청년"  # 청년 관련 금융 상품은 청년에서 추천
    if income_level <= 3:
        return "적금"
    elif income_level >= 8:
        return "예금"
    else:
        return "대출" if wants_loan else "예금"

# PDF 파일에서 텍스트 추출
def extract_text_from_pdfs(pdf_folder):
    texts = []
    filenames = []
    for filename in os.listdir(pdf_folder):
        if filename.lower().endswith('.pdf'):
            filepath = os.path.join(pdf_folder, filename)
            try:
                reader = PdfReader(filepath)
                text = ''
                for page_num, page in enumerate(reader.pages, start=1):
                    extracted_text = page.extract_text()
                    if extracted_text:
                        text += extracted_text
                    else:
                        logger.warning(f"페이지 {page_num}에서 텍스트를 추출할 수 없습니다: {filename}")
                texts.append(text)
                filenames.append(filename)
                logger.info(f"성공적으로 텍스트를 추출했습니다: {filename}")
            except Exception as e:
                logger.error(f"{filename} 파일을 읽는 중 오류가 발생했습니다: {e}")
    return filenames, texts

# 텍스트 전처리
def preprocess_text(text):
    text = re.sub(r'[^가-힣a-zA-Z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def main():
    st.title("📄 PDF 내용 추출, 소득분위에 따른 금융 상품 추천 및 Weaviate에 저장")

    # 1. PDF 파일 추출 및 Weaviate에 저장
    st.header("1️⃣ PDF 내용 추출 및 DB 저장")
    pdf_folder = st.text_input("📁 PDF 파일이 저장된 폴더 경로를 입력하세요", "/Users/im-woojin/Desktop/신한은행/신한은행_데이터")

    if st.button("🔍 PDF 내용 추출 및 DB 저장"):
        if not os.path.exists(pdf_folder):
            st.error("입력한 폴더 경로가 존재하지 않습니다. 다시 확인해주세요.")
            return

        create_weaviate_schema()

        with st.spinner("📄 PDF 파일에서 텍스트를 추출 중입니다..."):
            filenames, documents = extract_text_from_pdfs(pdf_folder)
            if not filenames:
                st.warning("해당 폴더에 PDF 파일이 없거나 텍스트를 추출할 수 없습니다.")
                return
            processed_documents = [preprocess_text(doc) for doc in documents]

            for filename, content, proc_content in zip(filenames, documents, processed_documents):
                category = classify_product(proc_content)
                client.data_object.create(
                    {
                        "filename": filename,
                        "content": content,
                        "processed_content": proc_content,
                        "category": category,
                        "income_level": 0,
                        "recommendation": "미지정"
                    },
                    "Document"
                )

        st.success("🚀 모든 문서가 Weaviate에 성공적으로 저장되었습니다!")

    # 2. DB에 저장된 리스트 조회
    st.header("2️⃣ DB에 저장된 리스트 조회하기")
    if st.button("🧐 Vector DB 저장된 리스트 조회하기"):
        with st.spinner("Weaviate에서 파일 이름 및 카테고리를 가져오는 중입니다..."):
            categorized_filenames = get_filenames_by_category()
            st.write("### 카테고리별 저장된 파일 이름 목록:")

            for category, filenames in categorized_filenames.items():
                with st.expander(f"🗂️ {category}"):
                    if filenames:
                        for filename in filenames:
                            st.write(f"- {filename}")
                    else:
                        st.write("해당 카테고리에 파일이 없습니다.")

    # 3. 사용자 정보 입력
    st.header("3️⃣ 사용자 정보 입력")
    asset_size = st.number_input("💰 자산 규모 (원)", min_value=0, format="%d")
    st.write(f"자산 규모: {asset_size:,}원")

    monthly_salary = st.number_input("💵 월급 (원)", min_value=0, format="%d")
    st.write(f"월급: {monthly_salary:,}원")

    age = st.number_input("🎂 나이 (만 나이)", min_value=0)

    wants_loan = st.checkbox("📋 대출을 희망하십니까?")

    # 4. 제품 추천
    if st.button("🎯 제품 추천"):
        income_level = calculate_income_level(asset_size, monthly_salary)
        recommendation = classify_product_by_income_level(income_level, wants_loan, age)
        st.write(f"소득분위: {income_level}, 추천 상품: {recommendation}")

        # 추천 제품 후보군
        categorized_filenames = get_filenames_by_category()

        if recommendation == "청년" and 19 <= age <= 34:
            st.write("청년 관련 금융 상품 후보군:")
            if categorized_filenames["청년"]:
                for filename in categorized_filenames["청년"]:
                    st.write(f"- {filename}")
            else:
                st.write("청년 관련 금융 상품 후보군이 없습니다.")
        elif recommendation == "대출" and categorized_filenames["대출"]:
            st.write("추천된 대출 후보군:")
            for filename in categorized_filenames["대출"]:
                st.write(f"- {filename}")
        elif recommendation in ["적금", "예금"] and categorized_filenames[recommendation]:
            st.write(f"추천된 {recommendation} 후보군:")
            for filename in categorized_filenames[recommendation]:
                st.write(f"- {filename}")
        else:
            st.write(f"{recommendation} 후보군이 없습니다.")

if __name__ == "__main__":
    main()

import os
from PyPDF2 import PdfReader
import re
import streamlit as st
import logging
import weaviate
import openai
from dotenv import load_dotenv

# 1. .env 파일에서 환경 변수를 로드합니다.
load_dotenv()

# 2. 환경 변수에서 OpenAI API 키를 가져옵니다.
openai.api_key = os.getenv("OPENAI_API_KEY")

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
                        {"name": "processed_content", "dataType": ["text"], "description": "The processed content of the PDF"}
                    ]
                }
            ]
        }
        client.schema.create(schema)
    except Exception as e:
        logger.error(f"Weaviate 스키마 생성 중 오류: {e}")
        st.error("Weaviate 스키마를 생성할 수 없습니다. 오류를 확인해주세요.")

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
    try:
        text = re.sub(r'[^가-힣a-zA-Z0-9\s]', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    except Exception as e:
        logger.error(f"텍스트 전처리 중 오류 발생: {e}")
        return ""

# 텍스트 분류 함수
def classify_product(text):
    try:
        # 키워드와 가중치 설정
        keywords = {
            '채권': {
                'words': [
                    '채권', '국채', '회사채', '신용등급', '발행자', '거치 기간', '유동성',
                    '표면 이율', '발행기관', '발행금액', '수익률', '발행일', '할인 채권',
                    '채권 시장', '투자 등급', '기간 채권', '만기 채권', '국내 채권',
                    '해외 채권', '채권 펀드', '장기 채권', '단기 채권', '채권 등급'
                ],
                'weight': 5
            },
            '적금': {
                'words': ['적금', '저축', '월 적립', '자동 이체', '정기', '납입', '출금 제한'],
                'weight': 5
            },
            '예금': {
                'words': ['예금', '정기예금', '거치', '이자', '파킹 통장', '단리', '복리', '고정 금리', '변동 금리'],
                'weight': 5
            },
            '청년': {
                'words': ['청년', '청년내일저축계좌', '청년도약계좌', '청년희망적금'],
                'weight': 8
            }
        }

        # 카테고리별 점수 초기화
        category_scores = {category: 0 for category in keywords}

        # 각 카테고리의 키워드에 대해 텍스트 내 등장 여부 확인
        for category, data in keywords.items():
            for word in data['words']:
                if re.search(r'\b' + re.escape(word) + r'\b', text, re.IGNORECASE):
                    category_scores[category] += data['weight']

        # 점수를 기준으로 카테고리 정렬
        sorted_categories = sorted(category_scores.items(), key=lambda x: x[1], reverse=True)
        top_category, top_score = sorted_categories[0]

        # 점수가 0보다 큰 경우 해당 카테고리 반환, 아니면 '미지정' 반환
        return top_category if top_score > 0 else '미지정'
    except Exception as e:
        logger.error(f"상품 분류 중 오류 발생: {e}")
        return "미지정"


# 소득분위 계산 함수
def calculate_income_level(asset_size, monthly_salary):
    try:
        if asset_size <= 5000000:
            asset_level = 1
        elif asset_size <= 10000000:
            asset_level = 2
        elif asset_size <= 20000000:
            asset_level = 3
        elif asset_size <= 30000000:
            asset_level = 4
        elif asset_size <= 50000000:
            asset_level = 5
        elif asset_size <= 70000000:
            asset_level = 6
        elif asset_size <= 100000000:
            asset_level = 7
        elif asset_size <= 200000000:
            asset_level = 8
        elif asset_size <= 500000000:
            asset_level = 9
        else:
            asset_level = 10

        if monthly_salary <= 1500000:
            salary_level = 1
        elif monthly_salary <= 2000000:
            salary_level = 2
        elif monthly_salary <= 2500000:
            salary_level = 3
        elif monthly_salary <= 3000000:
            salary_level = 4
        elif monthly_salary <= 3500000:
            salary_level = 5
        elif monthly_salary <= 4000000:
            salary_level = 6
        elif monthly_salary <= 4500000:
            salary_level = 7
        elif monthly_salary <= 5000000:
            salary_level = 8
        elif monthly_salary <= 7000000:
            salary_level = 9
        else:
            salary_level = 10

        average_level = (asset_level + salary_level) / 2
        return round(average_level)
    except Exception as e:
        logger.error(f"소득분위 계산 중 오류 발생: {e}")
        return 0

# MBTI 기반 금융 상품 추천
def classify_product_with_mbti(income_level, wants_loan, age, mbti):
    try:
        # I vs E: 안정성 vs. 고수익
        if 'I' in mbti:
            risk_preference = "안정성"
        else:
            risk_preference = "고수익"

        # J vs P: 장기성 vs. 단기성
        if 'J' in mbti:
            term_preference = "장기성"
        else:
            term_preference = "단기성"

        if wants_loan:
            return "채권"

        if risk_preference == "안정성":
            if income_level < 6:
                return "적금"
            else:
                return "예금"
        else:  # 고수익 선호
            if age <= 34 and term_preference == "장기성":
                return "청년"
            else:
                return "채권"
    except Exception as e:
        logger.error(f"MBTI 기반 상품 추천 중 오류 발생: {e}")
        return "미지정"

# Weaviate에 데이터 저장
def save_to_weaviate(filename, content, processed_content):
    try:
        # 중복 체크: 동일한 파일명이 존재하는지 확인
        response = client.query.get("Document", ["filename"]).with_where({
            "path": ["filename"],
            "operator": "Equal",
            "valueText": filename
        }).do()
        documents = response.get("data", {}).get("Get", {}).get("Document", [])
        if documents:
            logger.info(f"{filename} 파일이 이미 저장되어 있습니다. 중복 저장을 방지합니다.")
            return

        # 중복이 아닐 경우에만 저장
        data_object = {
            "filename": filename,
            "content": content,
            "processed_content": processed_content
        }
        client.data_object.create(data_object, "Document")
        logger.info(f"{filename} 파일이 성공적으로 Weaviate에 저장되었습니다.")
    except Exception as e:
        logger.error(f"Weaviate에 데이터를 저장하는 중 오류: {e}")
        st.error(f"{filename} 파일을 저장하는 중 오류가 발생했습니다. 오류: {e}")




# Weaviate에서 카테고리별 문서 가져오기
def get_documents_by_category(category):
    try:
        response = client.query.get("Document", ["filename", "keywords"]).with_where({
            "path": ["category"],
            "operator": "Equal",
            "valueText": category
        }).do()
        documents = response.get("data", {}).get("Get", {}).get("Document", [])
        return documents
    except Exception as e:
        logger.error(f"Weaviate에서 {category} 데이터를 가져오는 중 오류: {e}")
        return []

# Weaviate에서 RAG 쿼리 수행
def perform_rag_query(query):
    try:
        response = client.query.get("Document", ["content", "keywords", "category"]).with_near_text({
            "concepts": [query],
            "certainty": 0.5  # 확실성 값을 조정해 더 많은 결과 반환
        }).with_limit(5).do()
        documents = response.get("data", {}).get("Get", {}).get("Document", [])
        return documents
    except Exception as e:
        logger.error(f"RAG 쿼리 중 오류 발생: {e}")
        return []

# 숫자를 한글로 변환하는 함수
def number_to_korean(num):
    units = ["", "만", "억", "조", "경"]
    num_str = str(int(num))
    length = len(num_str)
    korean_num = ""

    for idx, digit in enumerate(num_str):
        if digit != "0":
            korean_num += digit + units[(length - idx - 1) // 4]

    return f"{int(num):,}원 ({korean_num})"

# MBTI 기반 금융 상품 추천 함수
# MBTI 기반 금융 상품 추천 함수
def classify_product_with_mbti(income_level, wants_loan, age, mbti):
    try:
        # I vs E: 안정성 vs. 고수익
        if 'I' in mbti:
            risk_preference = "안정성"
        else:
            risk_preference = "고수익"

        # J vs P: 장기성 vs. 단기성
        if 'J' in mbti:
            term_preference = "장기성"
        else:
            term_preference = "단기성"

        # N vs S: 미래 수익 변동성 vs. 현재 고정 이자율
        if 'N' in mbti:
            return_type_preference = "미래 수익 변동성"
        else:
            return_type_preference = "현재 고정 이자율"

        # 상품 추천 로직
        if wants_loan:
            return "채권"

        if risk_preference == "안정성":
            if income_level < 6:
                return "적금" if return_type_preference == "현재 고정 이자율" else "예금"
            else:
                return "예금" if return_type_preference == "현재 고정 이자율" else "채권"
        else:  # 고수익 선호
            if age <= 34 and term_preference == "장기성":
                return "청년" if return_type_preference == "미래 수익 변동성" else "채권"
            else:
                return "채권" if return_type_preference == "미래 수익 변동성" else "예금"
    except Exception as e:
        logger.error(f"MBTI 기반 상품 추천 중 오류 발생: {e}")
        return "미지정"

def fetch_all_documents():
    try:
        response = client.query.get("Document", ["filename", "content", "processed_content", "category", "income_level"]).do()
        documents = response.get("data", {}).get("Get", {}).get("Document", [])
        return documents
    except Exception as e:
        logger.error(f"Weaviate에서 데이터를 가져오는 중 오류: {e}")
        return []

def perform_rag_based_analysis_and_mapping(user_query, mbti=None):
    try:
        # Weaviate에서 문서 가져오기
        documents = perform_rag_query(user_query)
        if not documents:
            return "관련 문서를 찾을 수 없습니다."

        # 문서 내용을 LLM에게 전달하여 분석하도록 프롬프트 구성
        context = "\n\n".join([doc['content'] for doc in documents])
        analysis_prompt = f"""
        너는 금융 및 데이터 분석 전문가 AI입니다. 다음은 관련된 문서들입니다:

        {context}

        1. 각 문서에서 주요 키워드를 추출해줘.
        2. 문서를 서로 유사성에 따라 클러스터링해줘.
        3. 주어진 MBTI 유형({mbti})에 따라 적절한 금융 상품을 매핑해줘.
        """

        # LLM에 프롬프트 전달
        completion = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",  # 또는 "gpt-4"
            messages=[
                {"role": "user", "content": analysis_prompt}
            ],
            max_tokens=1000,
            temperature=0.7
        )
        answer = completion.choices[0].message['content'].strip()
        return answer
    except Exception as e:
        logger.error(f"RAG 기반 분석 중 오류 발생: {e}")
        return "RAG 기반 분석을 수행하는 중 오류가 발생했습니다."


def check_weaviate_data():
    try:
        response = client.query.get("Document", ["filename"]).do()
        documents = response.get("data", {}).get("Get", {}).get("Document", [])
        if not documents:
            logger.error("Weaviate에 문서가 없습니다.")
            return False
        else:
            logger.info(f"Weaviate에 {len(documents)}개의 문서가 있습니다.")
            return True
    except Exception as e:
        logger.error(f"Weaviate 데이터 점검 중 오류: {e}")
        return False

import nltk
from nltk.tokenize import sent_tokenize
nltk.download('punkt')

# Weaviate에서 모든 문서 가져오기
# 문서 내용을 요약하는 함수
def summarize_text(text, max_sentences=2):
    try:
        sentences = sent_tokenize(text)
        if len(sentences) > max_sentences:
            return " ".join(sentences[:max_sentences])
        return text
    except Exception as e:
        logger.error(f"텍스트 요약 중 오류 발생: {e}")
        return text


# Weaviate에서 모든 문서 가져오기
def fetch_all_documents():
    try:
        response = client.query.get("Document", ["filename", "content"]).do()
        documents = response.get("data", {}).get("Get", {}).get("Document", [])
        return documents
    except Exception as e:
        logger.error(f"Weaviate에서 데이터를 가져오는 중 오류: {e}")
        return []

# LLM 기반 분석 및 질문 처리
def handle_user_query(user_query):
    try:
        # Weaviate에서 문서 가져오기
        documents = fetch_all_documents()
        if not documents:
            return "Weaviate에서 문서를 찾을 수 없습니다."

        # 문서 수를 제한하고 내용을 요약
        max_documents = 100000000 # 필요한 경우 조정
        context = "\n\n".join([
            summarize_text(doc['content'], max_sentences=1)  # 한 문장만 요약
            for doc in documents[:max_documents]
        ])

        # LLM에 전달할 프롬프트 생성
        prompt = f"""
        다음은 일부 문서의 요약 내용입니다:
        {context}

        사용자의 질문: {user_query}
        문서를 분석하고 질문에 적절히 답변해 주세요.
        """

        # LLM 호출
        completion = openai.ChatCompletion.create(
            model="gpt-4-turbo",
            messages=[
                {"role": "system", "content": "너는 문서를 기반으로 분석하고 질문에 답변하는 AI입니다."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=4096,  # 필요한 경우, max_tokens 값을 더 줄일 수 있습니다
            temperature=0.7
        )
        answer = completion.choices[0].message['content'].strip()
        return answer
    except Exception as e:
        logger.error(f"LLM 질문 처리 중 오류 발생: {e}")
        return "LLM 질문을 처리하는 중 오류가 발생했습니다."



def perform_grouping_and_mapping(user_query):
    try:
        # Weaviate에서 문서 가져오기
        documents = fetch_all_documents()
        if not documents:
            return "Weaviate에서 문서를 찾을 수 없습니다."

        # 문서 내용을 요약하여 LLM에 전달할 컨텍스트 생성
        context = "\n\n".join([summarize_text(doc['content'], max_sentences=2) for doc in documents[:10]])

        # LLM에 전달할 프롬프트 생성
        prompt = f"""
        다음은 문서 내용입니다:
        {context}

        사용자의 질문: {user_query}
        문서를 적절히 분석하고 그룹화한 후, 금융 상품을 매핑해 주세요.
        """

        # LLM 호출
        completion = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "너는 금융 문서 분석 전문가 AI입니다."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1000,
            temperature=0.7
        )
        answer = completion.choices[0].message['content'].strip()
        return answer
    except Exception as e:
        logger.error(f"LLM 기반 그룹화 및 매핑 중 오류 발생: {e}")
        return "LLM 기반 그룹화 및 매핑을 수행하는 중 오류가 발생했습니다."

def main():
    st.title("📄 PDF 내용 추출 및 Weaviate 저장 시스템")

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
                save_to_weaviate(filename, content, proc_content)  # category 인수 제거

        st.success("🚀 모든 문서가 Weaviate에 성공적으로 저장되었습니다!")

    # 2. DB 시각화
    st.header("2️⃣ DB 시각화")
    if st.button("📊 모든 문서 보기"):
        documents = fetch_all_documents()
        if documents:
            st.write("DB에 저장된 문서들:")
            for doc in documents:
                st.write(f"**파일명**: {doc['filename']}")
                st.write(f"**내용 요약**: {doc['content'][:200]}...")  # 내용의 일부만 출력
                st.write("---")
        else:
            st.warning("DB에 저장된 문서가 없습니다.")

    # 3. LLM 기반 대화 시스템
    st.header("3️⃣ LLM 기반 대화 시스템")
    user_query = st.text_input("💬 질문을 입력하세요")

    if st.button("💡 질문 처리"):
        with st.spinner("LLM에서 응답을 생성하고 있습니다..."):
            answer = handle_user_query(user_query)
            st.subheader("🤖 LLM의 응답")
            st.write(answer)

    # 4. 사용자 정보 입력 및 MBTI 기반 금융 상품 추천
    st.header("4️⃣ 사용자 정보 입력 및 MBTI 기반 금융 상품 추천")
    # 스크롤 가능한 컨테이너 사용
    with st.container():
        with st.form("user_input_form"):
            asset_size = st.number_input("💰 자산 규모 (원)", min_value=0, format="%d", value=0)
            monthly_salary = st.number_input("💵 월급 (원)", min_value=0, format="%d", value=0)
            age = st.number_input("🎂 나이 (만 나이)", min_value=0, value=0)
            wants_loan = st.checkbox("📋 채권을 희망하십니까?", value=False)
            mbti = st.text_input("🧠 MBTI 유형을 입력하세요 (예: INTJ, ENFP)", value="")

            # 자산 규모와 월급을 한글로 변환하여 표시
            st.write(f"입력한 자산 규모: {number_to_korean(asset_size)}")
            st.write(f"입력한 월급: {number_to_korean(monthly_salary)}")

            # 제출 버튼 추가
            submit_button = st.form_submit_button("🎯 제품 추천")

        if submit_button:
            income_level = calculate_income_level(asset_size, monthly_salary)
            recommendation = classify_product_with_mbti(income_level, wants_loan, age, mbti)
            st.write(f"소득분위: {income_level}, 추천 상품: {recommendation}")

if __name__ == "__main__":
    main()

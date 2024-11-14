import os
import re
from PyPDF2 import PdfReader
import streamlit as st
import logging
import weaviate
import openai
from dotenv import load_dotenv
import json
from weaviate import Client
import nltk
from nltk.tokenize import sent_tokenize
import time
import requests
import platform

# 환경 변수 로드
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
WEAVIATE_URL = os.getenv("WEAVIATE_URL")

# UnicodeEncodeError 방지를 위한 custom session 생성
session = requests.Session()
session.headers['User-Agent'] = 'OpenAI-Python'
openai.requestssession = session
openai.disable_telemetry = True

# Weaviate 클라이언트 설정
client = Client(
    url=WEAVIATE_URL,
    timeout_config=(5, 15)  # (connect timeout, read timeout)
)

# 로깅 설정
logging.basicConfig(level=logging.INFO, encoding='utf-8')
logger = logging.getLogger(__name__)

# NLTK 데이터 다운로드
nltk.download('punkt')

# Weaviate connection check function
# Weaviate 연결 확인 기능 개선 (재시도 추가)
def check_weaviate_connection(retries=3):
    for attempt in range(retries):
        try:
            if client.is_ready():
                logger.info("Successfully connected to Weaviate server.")
                return True
            else:
                logger.error("Cannot connect to Weaviate server. Attempt %d", attempt + 1)
        except Exception as e:
            logger.error(f"Error checking Weaviate connection: {e}")
            st.error(f"Attempt {attempt + 1} to connect to Weaviate failed. Error: {e}")
    st.error("All connection attempts to Weaviate failed.")
    return False

# Get finance products from Weaviate
def get_finance_products():
    try:
        # "summary" 필드 제거
        response = client.query.get("Document", ["filename", "category", "mbti", "content"]).do()
        products = response.get("data", {}).get("Get", {}).get("Document", [])
        if not products:
            logger.warning("No documents found in Weaviate.")
            st.warning("데이터베이스에 문서가 없습니다.")
        return products
    except Exception as e:
        logger.error(f"Error calling Weaviate API: {e}")
        st.error("문서를 불러오는 중 오류가 발생했습니다.")
        return None


# Add finance products to Weaviate
# MBTI 유형과 금융 상품 예시 데이터 추가
def add_finance_products(filename):
    mbti_types = ["ISTJ", "ISFJ", "INFJ", "INTJ", "ISTP", "ISFP", "INFP", "INTP",
                  "ESTP", "ESFP", "ENFP", "ENTP", "ESTJ", "ESFJ", "ENFJ", "ENTJ"]
    categories = ["적금", "예금", "채권", "청년"]

    for mbti in mbti_types:
        for category in categories:
            document = {
                "filename": filename,  # 함수에 전달된 파일명을 그대로 사용
                "content": f"This is a sample document content for {mbti} in category {category}.",
                "mbti": mbti,
                "category": category
            }
            try:
                client.data_object.create(data_object=document, class_name="Document")
                logger.info(f"MBTI {mbti}와 카테고리 {category}의 문서가 {filename} 이름으로 추가되었습니다.")
            except Exception as e:
                logger.error(f"{mbti}와 {category}에 대한 문서를 추가하는 중 오류 발생: {e}")



# Display finance products in Streamlit
def display_finance_products_st():
    products = get_finance_products()
    if products:
        st.write("<h2>🗃️ Weaviate에 저장된 금융 상품 목록:</h2>", unsafe_allow_html=True)
        for product in products:
            product_name = product.get("name", "No name")
            product_category = product.get("category", "No category")
            product_mbti = product.get("mbti", "No MBTI")
            product_summary = product.get("summary", "No summary available")

            # 요약 설명에서 우대 조건은 리스트 형태로 정리
            summary_parts = product_summary.split("우대 이자율:")
            main_summary = summary_parts[0].strip()
            if len(summary_parts) > 1:
                benefits_summary = summary_parts[1].strip()
            else:
                benefits_summary = "우대 조건 정보가 없습니다."

            st.markdown(
                f"""
                <div style="background-color: #f7f9fc; border: 1px solid #e0e6ef; border-radius: 12px; padding: 20px; margin-bottom: 20px; box-shadow: 0px 4px 8px rgba(0, 0, 0, 0.1);">
                    <h3 style="color: #2c3e50; font-size: 1.5em; margin-bottom: 5px;">📄 {product_name}</h3>
                    <p style="font-size: 1.1em; color: #555;"><b>카테고리:</b> <span style="color: #27ae60;">{product_category}</span> | <b>MBTI 유형:</b> <span style="color: #3498db;">{product_mbti}</span></p>
                    <p style="margin: 10px 0; font-size: 1.1em; color: #34495e;"><b>기본 요약:</b> {main_summary}</p>
                    <p style="margin: 10px 0; font-size: 1.1em; color: #8e44ad;"><b>우대 조건:</b> {benefits_summary}</p>
                </div>
                """,
                unsafe_allow_html=True
            )
    else:
        st.write("<p style='color: #e74c3c;'>⚠️ 금융 상품을 불러올 수 없습니다.</p>", unsafe_allow_html=True)

# Create Weaviate schema
# Weaviate 스키마 생성 함수
def create_weaviate_schema():
    try:
        existing_classes = [cls['class'] for cls in client.schema.get()["classes"]]

        if "FinanceProduct" not in existing_classes:
            finance_product_schema = {
                "class": "Document",
                "description": "Information about finance products",
                "properties": [
                    {"name": "filename", "dataType": ["text"], "description": "Product filename"},
                    {"name": "content", "dataType": ["text"], "description": "Document content"},
                    {"name": "mbti", "dataType": ["text"], "description": "MBTI type"},
                    {"name": "category", "dataType": ["text"], "description": "Product category (e.g., 적금, 예금, 채권, 청년)"}
                ]
            }
            client.schema.create_class(finance_product_schema)
            logger.info("FinanceProduct schema created.")
        else:
            logger.info("FinanceProduct schema already exists.")
    except Exception as e:
        logger.error(f"Error creating Weaviate schema: {e}")


# PDF 파일에서 문자 읽어오기
def extract_text_from_pdfs(uploaded_files):
    texts = []
    filenames = []
    for uploaded_file in uploaded_files:
        if uploaded_file.type == 'application/pdf':
            try:
                reader = PdfReader(uploaded_file)
                text = ''.join(page.extract_text() for page in reader.pages if page.extract_text())
                texts.append(text)
                filenames.append(uploaded_file.name)
                logger.info(f"텍스트 추출 성공: {uploaded_file.name}")
            except Exception as e:
                logger.error(f"{uploaded_file.name} 읽기 오류: {e}")
    return filenames, texts

# 불필요한 특수 문자 제거
def preprocess_text(text):
    try:
        text = re.sub(r'[^가-힣a-zA-Z0-9\s]', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    except Exception as e:
        logger.error(f"텍스트 전처리 중 오류 발생: {e}")
        return ""



# LLM을 통해 MBTI 및 카테고리 예측
# LLM을 통해 MBTI 및 카테고리 예측
# LLM을 통해 MBTI 및 카테고리 예측
def classify_with_llm(text):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "주어진 금융 상품 설명에서 적절한 카테고리와 MBTI 유형을 추출해 주세요. "
                        "카테고리는 적금, 예금, 채권, 청년 중 하나일 수 있으며, "
                        "MBTI는 ISTJ, ISFJ, INFJ, INTJ, ISTP, ISFP, INFP, INTP, "
                        "ESTP, ESFP, ENFP, ENTP, ESTJ, ESFJ, ENFJ, ENTJ 중 하나입니다."
                    )
                },
                {"role": "user", "content": text}
            ],
            max_tokens=50,
            temperature=0.5
        )
        llm_response = response.choices[0].message['content'].strip()

        # LLM 응답에서 카테고리와 MBTI 추출
        category_match = re.search(r"카테고리:\s*(\S+)", llm_response)
        mbti_match = re.search(
            r"MBTI:\s*(ISTJ|ISFJ|INFJ|INTJ|ISTP|ISFP|INFP|INTP|ESTP|ESFP|ENFP|ENTP|ESTJ|ESFJ|ENFJ|ENTJ)",
            llm_response,
            re.IGNORECASE
        )

        category = category_match.group(1) if category_match else "미지정"
        mbti = mbti_match.group(1).upper() if mbti_match else "미지정"

        return category, mbti
    except Exception as e:
        logger.error(f"LLM 분류 중 오류 발생: {e}")
        return "미지정", "미지정"



# Text classification function
def classify_product(text):
    try:
        # Keywords and weights
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

        # Initialize category scores
        category_scores = {category: 0 for category in keywords}

        # Check for keyword presence in text
        for category, data in keywords.items():
            for word in data['words']:
                if re.search(r'\b' + re.escape(word) + r'\b', text, re.IGNORECASE):
                    category_scores[category] += data['weight']

        # Sort categories by score
        sorted_categories = sorted(category_scores.items(), key=lambda x: x[1], reverse=True)
        top_category, top_score = sorted_categories[0]

        return top_category if top_score > 0 else '미지정'
    except Exception as e:
        logger.error(f"Error classifying product: {e}")
        return "미지정"

# Income level calculation function
def calculate_income_level(asset_size, monthly_salary):
    try:
        # 자산 수준 계산
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

        # 월급 수준 계산
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
        logger.error(f"수익 분위 계산 중 오류 발생: {e}")
        return 0

# MBTI-based finance product recommendation function
def classify_product_with_mbti(income_level, age, mbti):
    try:
        # 성향 파악
        if 'I' in mbti.upper():
            risk_preference = "안정성"
        else:
            risk_preference = "고수익"

        if 'J' in mbti.upper():
            term_preference = "장기성"
        else:
            term_preference = "단기성"

        if 'N' in mbti.upper():
            return_type_preference = "미래 수익 변동성"
        else:
            return_type_preference = "현재 고정 이자율"

        # 소득 수준에 따른 기본 추천
        if income_level <= 3:
            base_recommendation = "적금"
        elif income_level <= 6:
            base_recommendation = "예금"
        else:
            base_recommendation = "채권"

        # 추가 추천 메시지 구성
        recommendation_message = f"{base_recommendation} (소득 분위 기준)"

        if base_recommendation == "적금":
            if risk_preference == "고수익" or return_type_preference == "미래 수익 변동성":
                recommendation_message += " - 하지만 고수익을 원하신다면 예금을 고려해보세요."
        elif base_recommendation == "예금":
            if risk_preference == "고수익" and return_type_preference == "미래 수익 변동성":
                recommendation_message += " - 더 높은 수익을 원하신다면 채권도 추천됩니다."
            elif term_preference == "단기성":
                recommendation_message += " - 단기적인 투자 성향이라면 적금도 적합할 수 있습니다."
        elif base_recommendation == "채권":
            if risk_preference == "안정성" and return_type_preference == "현재 고정 이자율":
                recommendation_message += " - 안정성을 원하시면 예금도 고려해보세요."

        return base_recommendation, recommendation_message
    except Exception as e:
        logger.error(f"MBTI 기반 추천 중 오류 발생: {e}")
        return "미지정", "추천 과정에서 오류가 발생했습니다."



# Save data to Weaviate with LLM classification
def save_to_weaviate_with_llm(filename, content, processed_content):
    try:
        # LLM을 사용하여 카테고리와 MBTI 분류
        category, mbti = classify_with_llm(content)

        # Weaviate에 저장할 데이터 객체 구성
        data_object = {
            "filename": filename,
            "content": content,
            "processed_content": processed_content,
            "category": category,
            "mbti": mbti
        }
        client.data_object.create(data_object=data_object, class_name="Document")
        logger.info(f"{filename} successfully saved to Weaviate with LLM classification.")
    except Exception as e:
        logger.error(f"Error saving data to Weaviate: {e}")
        st.error(f"An error occurred while saving {filename}. Error: {e}")


# Save data to Weaviate (including category)
def save_to_weaviate(filename, content, processed_content, category=None):
    try:
        summary = generate_summary(content)
        data_object = {
            "filename": filename,
            "content": content,
            "processed_content": processed_content,
            "summary": summary,  # 요약 추가
            "category": category
        }
        client.data_object.create(data_object=data_object, class_name="Document")
        logger.info(f"{filename} successfully saved to Weaviate with summary.")
    except Exception as e:
        logger.error(f"Error saving data to Weaviate: {e}")
        st.error(f"An error occurred while saving {filename}. Error: {e}")


# Convert number to Korean currency format
def number_to_korean(num):
    units = ["", "만", "억", "조", "경"]
    num_str = str(int(num))
    length = len(num_str)
    korean_num = ""

    for idx, digit in enumerate(num_str):
        if digit != "0":
            korean_num += digit + units[(length - idx - 1) // 4]

    return f"{int(num):,}원 ({korean_num})"

# Weaviate delete document function
def delete_document(filename):
    try:
        response = client.query.get("Document", ["_additional { id }"]).with_where({
            "path": ["filename"],
            "operator": "Equal",
            "valueText": filename
        }).do()
        documents = response.get("data", {}).get("Get", {}).get("Document", [])
        if documents:
            document_id = documents[0].get("_additional", {}).get("id")
            client.data_object.delete(uuid=document_id, class_name="Document")
            logger.info(f"{filename} document successfully deleted.")
        else:
            logger.warning(f"Cannot find document {filename}.")
            st.error(f"Cannot find document {filename}.")
    except Exception as e:
        logger.error(f"Error deleting document {filename}: {e}")
        st.error(f"An error occurred while deleting {filename}.")

# Weaviate update document function
def update_document(filename, new_content):
    try:
        response = client.query.get("Document", ["_additional { id }"]).with_where({
            "path": ["filename"],
            "operator": "Equal",
            "valueText": filename
        }).do()
        documents = response.get("data", {}).get("Get", {}).get("Document", [])
        if documents:
            document_id = documents[0].get("_additional", {}).get("id")
            client.data_object.update(
                data_object={"content": new_content},
                class_name="Document",
                uuid=document_id
            )
            logger.info(f"{filename} document successfully updated.")
        else:
            logger.warning(f"Cannot find document {filename}.")
            st.error(f"Cannot find document {filename}.")
    except Exception as e:
        logger.error(f"Error updating document {filename}: {e}")
        st.error(f"An error occurred while updating {filename}.")

# Perform grouping and mapping in Weaviate
def perform_grouping_and_mapping():
    try:
        # Get documents from Weaviate
        response = client.query.get("Document", ["filename", "content"]).do()
        documents = response.get("data", {}).get("Get", {}).get("Document", [])

        if not documents:
            return "No documents in Weaviate."

        # Construct content for LLM
        context = "\n\n".join([f"{doc['filename']}: {doc['content'][:200]}" for doc in documents])
        prompt = f"""
        그룹화하고 각 문서를 금융 상품에 매핑해 주세요.

        문서 목록:
        {context}

        1. 각 문서를 관련성에 따라 그룹화해 주세요.
        2. 각 그룹에 적절한 금융 상품 카테고리를 매핑해 주세요.
        """

        # Call LLM
        completion = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1000,
            temperature=0.7
        )
        grouped_answer = completion.choices[0].message['content'].strip()

        # Save grouped data to Weaviate
        save_grouped_data_to_weaviate(grouped_answer)

        return grouped_answer
    except Exception as e:
        logger.error(f"Error performing grouping and mapping: {e}")
        return "An error occurred during grouping and mapping."


def generate_summary(text):
    max_retries = 5
    retry_delay = 2
    for attempt in range(max_retries):
        try:
            truncated_text = text[:5000]
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[{"role": "system", "content": "주어진 텍스트에서 이자율과 우대 조건만 간결하게 요약해 주세요."},
                          {"role": "user", "content": truncated_text}],
                max_tokens=500,
                temperature=0.5
            )
            return response.choices[0].message['content'].strip()
        except openai.error.RateLimitError as e:
            logger.error(f"Rate limit error: {e}. Retrying...")
            time.sleep(retry_delay)
            retry_delay *= 2
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return "Error generating summary."



# Save grouped data to Weaviate
def save_grouped_data_to_weaviate(grouped_data):
    try:
        groups = grouped_data.split("\n\n")
        for i, group in enumerate(groups, start=1):
            data_object = {
                "group_id": f"group_{i}",
                "group_content": group,
            }
            client.data_object.create(data_object=data_object, class_name="GroupedDocument")
            logger.info(f"Group {i} data saved to Weaviate.")
    except Exception as e:
        logger.error(f"Error saving group data to Weaviate: {e}")
        st.error(f"An error occurred while saving group data.")

# LLM response handler
def handle_llm_response(response):
    try:
        if ':' in response:
            command_part, arguments_part = response.split(':', 1)
            command = command_part.strip().upper()
            arguments = arguments_part.strip()

            if command == "DELETE_DOCUMENT":
                filename = arguments
                delete_document(filename)
                return f"{filename} 문서를 삭제했습니다."

            elif command == "ADD_DOCUMENT":
                parts = arguments.split(':', 1)
                if len(parts) == 2:
                    filename, content = parts
                    filename = filename.strip()
                    content = content.strip()
                    category = classify_product(content)
                    save_to_weaviate(filename, content, preprocess_text(content), category)
                    return f"{filename} 문서를 카테고리 '{category}'로 DB에 추가했습니다."
                else:
                    return "ADD_DOCUMENT 명령의 인자가 부족합니다."

            elif command == "UPDATE_DOCUMENT":
                parts = arguments.split(':', 1)
                if len(parts) == 2:
                    filename, new_content = parts
                    filename = filename.strip()
                    new_content = new_content.strip()
                    update_document(filename, new_content)
                    return f"{filename} 문서를 업데이트했습니다."
                else:
                    return "UPDATE_DOCUMENT 명령의 인자가 부족합니다."

            elif command == "PERFORM_GROUPING_AND_MAPPING":
                result = perform_grouping_and_mapping()
                return result

            else:
                logger.warning(f"Unknown command received: {command}")
                return "알 수 없는 명령입니다. 지원되지 않는 명령입니다."
        else:
            logger.warning("LLM response format incorrect.")
            return "LLM 응답 형식이 올바르지 않습니다."

    except Exception as e:
        logger.error(f"Error handling LLM response: {e}")
        return "응답 처리 중 오류가 발생했습니다."


# LLM-based conversation system with Weaviate operations
import requests


# 특정 MBTI 유형과 카테고리로 필터링된 금융 상품 가져오기
# 특정 MBTI 유형과 카테고리로 필터링된 금융 상품 가져오기
def get_filtered_finance_products(mbti_type=None, category=None):
    conditions = []
    if mbti_type:
        conditions.append(f'{{ path: ["mbti"], operator: Equal, valueText: "{mbti_type}" }}')
    if category:
        conditions.append(f'{{ path: ["category"], operator: Equal, valueText: "{category}" }}')

    query_conditions = ', '.join(conditions)
    graphql_query = f"""
    {{
      Get {{
        Document(where: {{
          operator: And,
          operands: [
            {query_conditions}
          ]
        }}) {{
          filename
          category
          mbti
          content
        }}
      }}
    }}
    """

    response = requests.post(
        WEAVIATE_URL + "/v1/graphql",
        headers={"Content-Type": "application/json"},
        json={"query": graphql_query}
    )

    if response.status_code == 200:
        products = response.json().get('data', {}).get('Get', {}).get('Document', [])
        logger.info(f"Filtered products: {products}")
        return products
    else:
        st.error(f"요청 실패: 상태 코드 {response.status_code}, 오류 메시지: {response.text}")
        return []

# Streamlit에서 사용자 입력 받고 필터링된 결과 출력
def display_filtered_products():
    st.title("📄 MBTI 기반 금융 상품 추천")
    mbti_type = st.text_input("MBTI 유형을 입력하세요 (예: ISTP)")
    category = st.selectbox("상품 카테고리 선택", ["", "적금", "예금", "채권", "청년"])

    if st.button("추천 상품 보기"):
        products = get_filtered_finance_products(mbti_type=mbti_type, category=category)
        if products:
            st.write(f"Weaviate에 저장되어 있는 {mbti_type} 유형의 '{category}' 카테고리 금융 상품 목록:")
            for product in products:
                st.write(f"- 파일명: {product['filename']}")
                st.write(f"  카테고리: {product['category']}")
                st.write(f"  MBTI 유형: {product['mbti']}")
                st.write(f"  상품 설명: {product.get('summary', '설명이 없습니다.')}")
                st.write("---")
        else:
            st.write("해당 조건에 맞는 금융 상품이 없습니다.")


def handle_user_query(user_query):
    if 'messages' not in st.session_state:
        st.session_state.messages = []

    st.session_state.messages.append({"role": "user", "content": user_query})

    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "너는 금융 어시스턴트야. 사용자가 금융 상품에 대해 질문할 때 적절한 상품을 추천해줘."},
            {"role": "user", "content": user_query}
        ],
        max_tokens=1000,
        temperature=0.5
    )
    llm_answer = response.choices[0].message['content'].strip()

    mbti_type, category = None, None
    if "적금" in user_query:
        category = "적금"
    elif "예금" in user_query:
        category = "예금"
    elif "채권" in user_query:
        category = "채권"
    elif "청년" in user_query:
        category = "청년"

    for mbti in ["ISTJ", "ISFJ", "INFJ", "INTJ", "ISTP", "ISFP", "INFP", "INTP",
                 "ESTP", "ESFP", "ENFP", "ENTP", "ESTJ", "ESFJ", "ENFJ", "ENTJ"]:
        if mbti in user_query.upper():
            mbti_type = mbti
            break

    if mbti_type or category:
        products = get_filtered_finance_products(mbti_type=mbti_type, category=category)
        if products:
            product_response = "🔎 검색 결과:\n"
            for product in products:
                # 요약 요청을 위해 상품 설명 내용을 LLM에 전달
                content_summary = generate_summary(product['content'])  # 상품 설명 필드 요약
                product_response += f"- **파일명**: {product['filename']}\n  **카테고리**: {product['category']}\n  **MBTI 유형**: {product['mbti']}\n  **요약 설명**: {content_summary}\n"
            st.session_state.messages.append({"role": "assistant", "content": product_response})
        else:
            st.session_state.messages.append({"role": "assistant", "content": "해당 조건에 맞는 금융 상품이 없습니다."})
    else:
        st.session_state.messages.append({"role": "assistant", "content": llm_answer})

import os
import re
from PyPDF2 import PdfReader
import streamlit as st
import logging
import weaviate
import openai
from dotenv import load_dotenv
import json
from weaviate import Client
import nltk
from nltk.tokenize import sent_tokenize
import time

# Load environment variables
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
WEAVIATE_URL = os.getenv("WEAVIATE_URL")

# Weaviate client setup
client = Client(
    url=WEAVIATE_URL,
    timeout_config=(5, 15)  # (connect timeout, read timeout)
)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# NLTK data download
nltk.download('punkt')

# Weaviate connection check function
def check_weaviate_connection(retries=3):
    for attempt in range(retries):
        try:
            if client.is_ready():
                logger.info("Weaviate 서버에 성공적으로 연결되었습니다.")
                return True
            else:
                logger.error("Weaviate 서버에 연결할 수 없습니다. 시도 횟수: %d", attempt + 1)
        except Exception as e:
            logger.error(f"Weaviate 연결 확인 중 오류 발생: {e}")
            st.error(f"Weaviate 연결 시도 {attempt + 1} 실패. 오류: {e}")
    st.error("Weaviate에 대한 모든 연결 시도가 실패했습니다.")
    return False

# Weaviate schema creation function
def create_weaviate_schema():
    try:
        existing_classes = [cls['class'] for cls in client.schema.get()["classes"]]
        if "Document" not in existing_classes:
            finance_product_schema = {
                "class": "Document",
                "description": "Information about finance products",
                "properties": [
                    {"name": "filename", "dataType": ["text"], "description": "Product filename"},
                    {"name": "content", "dataType": ["text"], "description": "Document content"},
                    {"name": "mbti", "dataType": ["text"], "description": "MBTI type"},
                    {"name": "category", "dataType": ["text"], "description": "Product category"}
                ]
            }
            client.schema.create_class(finance_product_schema)
            logger.info("Document schema created.")
        else:
            logger.info("Document schema already exists.")
    except Exception as e:
        logger.error(f"Error creating Weaviate schema: {e}")

# Add finance products to Weaviate
def add_finance_products(filename):
    mbti_types = ["ISTJ", "ISFJ", "INFJ", "INTJ", "ISTP", "ISFP", "INFP", "INTP",
                  "ESTP", "ESFP", "ENFP", "ENTP", "ESTJ", "ESFJ", "ENFJ", "ENTJ"]
    categories = ["적금", "예금", "채권", "청년"]

    for mbti in mbti_types:
        for category in categories:
            document = {
                "filename": filename,  # 입력한 파일명을 그대로 사용
                "content": f"This is a sample document content for {mbti} in category {category}.",
                "mbti": mbti,
                "category": category
            }
            try:
                client.data_object.create(data_object=document, class_name="Document")
                logger.info(f"Document for MBTI {mbti} and category {category} added with filename {filename}.")
            except Exception as e:
                logger.error(f"Error adding document for {mbti} and {category}: {e}")

# Main function
def main():
    st.title("📄 금융 상품 추천 AI")

    # Weaviate 연결 확인
    if not check_weaviate_connection():
        st.error("Weaviate 서버에 연결할 수 없습니다. 서버 상태를 확인하세요.")
        return

    # Weaviate 스키마 생성 (생략)

    # 사이드바 및 메인 페이지 UI
    st.sidebar.markdown(
        "<h2 style='font-size: 1.5em;'>🦐 사용자 정보 입력 및 MBTI 기반 금융 상품 추천</h2>",
        unsafe_allow_html=True
    )
    menu = ["Home", "Admin Page"]

    choice = st.sidebar.selectbox(
        "📋 메뉴 선택",
        menu,
        index=menu.index(st.session_state.get("page", "Home")) if 'page' in st.session_state else 0,
        key="unique_menu_selectbox_key"
    )

    # 사용자 입력 폼
    with st.sidebar.form("user_input_form"):
        asset_size = st.number_input("💸 자산 규모 (원)", min_value=0, format="%d", value=0)
        monthly_salary = st.number_input("💵 월급 (원)", min_value=0, format="%d", value=0)
        age = st.number_input("🎂 나이 (만 나이)", min_value=0, value=0)
        mbti = st.text_input("🤔 MBTI 유형을 입력하세요 (예: INTJ, ENFP)", value="")

        submit_button = st.form_submit_button("🏇 상품 추천")

    if submit_button:
        income_level = calculate_income_level(asset_size, monthly_salary)
        base_recommendation, recommendation_message = classify_product_with_mbti(income_level, age, mbti)

        # MBTI 성향 설명
        mbti_personality = {
            "I": "안정적인 투자 성향",
            "E": "고수익 투자 성향",
            "N": "미래 수익 변동성을 선호",
            "S": "현재 고정 이자율을 선호",
            "J": "장기적인 투자 성향",
            "P": "단기적인 투자 성향"
        }
        mbti_explanation = f"{mbti.upper()}: " + ", ".join(
            [mbti_personality.get(char, "") for char in mbti.upper() if char in mbti_personality]
        )

        # 추천 결과 표시
        st.sidebar.markdown(
            f"""
            <div style="border: 1px solid #ddd; padding: 10px; border-radius: 5px;">
                <h4>추천 결과</h4>
                <p><b>소득 분위:</b> {income_level}</p>
                <p><b>추천 상품:</b> {base_recommendation}</p>
                <p><b>추가 설명:</b> {recommendation_message}</p>
                <p><b>MBTI 투자 성향:</b> {mbti_explanation}</p>
                {"<p><b>추가 혜택:</b> 청년 전용 상품도 고려해보세요!</p>" if age <= 34 else ""}
            </div>
            """,
            unsafe_allow_html=True
        )

    if choice == "Home":
        st.markdown(
            """
            <div style='text-align: center;'>
                <h1 style='font-size: 3em;'>🏠 메인 페이지</h1>
            </div>
            """,
            unsafe_allow_html=True
        )
        st.header("💬 LLM 기반 대화 시스템")

        if 'messages' not in st.session_state:
            st.session_state.messages = []

        # 대화 내역 표시
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        # 사용자 입력 받기
        user_input = st.chat_input("💬 질문을 입력하세요")
        if user_input:
            with st.chat_message("user"):
                st.markdown(user_input)
            with st.spinner("LLM에서 응답을 생성하고 있습니다..."):
                handle_user_query(user_input)
            assistant_message = st.session_state.messages[-1]
            if assistant_message["role"] == "assistant":
                with st.chat_message("assistant"):
                    st.markdown(assistant_message["content"])

    elif choice == "Admin Page":
        if 'admin_authenticated' not in st.session_state:
            st.session_state.admin_authenticated = False

        if not st.session_state.admin_authenticated:
            password = st.text_input("🔒 관리자 패스워드를 입력하세요", type="password")
            if st.button("🔑 로그인"):
                if password == os.getenv("ADMIN_PASSWORD"):
                    st.session_state.admin_authenticated = True
                    st.success("패스워드가 일치합니다. 아래 버튼을 눌러 관리자 페이지로 이동하세요.")
                    if st.button("관리자 페이지로 이동"):
                        st.session_state.page = 'Admin Page'
                else:
                    st.error("잘못된 패스워드입니다. 다시 시도해주세요.")
        else:
            st.header("📂 관리자 페이지 - PDF 업로드 및 DB 저장")
            filename = st.text_input("저장할 파일명을 입력하세요 (예: sample_product.pdf)")
            uploaded_files = st.file_uploader("📁 PDF 파일을 업로드하세요", type=["pdf"], accept_multiple_files=True)
            if st.button("📥 PDF 내용 추출하고 DB 저장", key="extract_save"):
                if not uploaded_files or not filename:
                    st.error("PDF 파일과 파일명을 모두 입력하세요.")
                    return

                with st.spinner("📖 PDF 파일에서 텍스트를 추출 중입니다..."):
                    filenames, documents = extract_text_from_pdfs(uploaded_files)

                    if not filenames:
                        st.warning("업로드된 파일에서 텍스트를 추출할 수 없습니다.")
                        return

                    processed_documents = [preprocess_text(doc) for doc in documents]
                    for content, proc_content in zip(documents, processed_documents):
                        save_to_weaviate_with_llm(filename, content, proc_content)
                    st.success("🚀 모든 문서가 Weaviate에 성공적으로 저장되었습니다!")

if __name__ == "__main__":
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    main()


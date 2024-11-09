import os
from PyPDF2 import PdfReader
import re
import streamlit as st
import logging
import weaviate
import openai
from dotenv import load_dotenv
import json
from weaviate import Client
import nltk
from nltk.tokenize import sent_tokenize

# 환경 변수 로드
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
WEAVIATE_URL = os.getenv("WEAVIATE_URL")

# Weaviate 클라이언트 설정
client = Client(
    url=WEAVIATE_URL,
    timeout_config=(5, 15)  # (연결 타임아웃, 읽기 타임아웃)
)

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# NLTK 데이터 다운로드
nltk.download('punkt')

# Weaviate 연결 상태 확인 함수
def check_weaviate_connection():
    try:
        if client.is_ready():
            logger.info("Weaviate 서버에 성공적으로 연결되었습니다.")
            return True
        else:
            logger.error("Weaviate 서버에 연결할 수 없습니다.")
            st.error("Weaviate 서버에 연결할 수 없습니다. 서버 상태를 확인하세요.")
            return False
    except Exception as e:
        logger.error(f"Weaviate 연결 확인 중 오류 발생: {e}")
        st.error(f"Weaviate 연결 확인 중 오류가 발생했습니다: {e}")
        return False

# Weaviate에서 금융 상품 데이터 조회 함수
def get_finance_products():
    try:
        response = client.query.get("FinanceProduct", ["name", "description"]).do()
        products = response.get("data", {}).get("Get", {}).get("FinanceProduct", [])
        return products
    except Exception as e:
        logger.error(f"Weaviate API 호출 중 오류 발생: {e}")
        return None

# Weaviate에 금융 상품 데이터 추가 함수
def add_finance_products():
    try:
        # 이미 데이터가 있는지 확인
        existing_products = get_finance_products()
        if existing_products:
            logger.info("금융 상품 데이터가 이미 존재합니다. 추가하지 않습니다.")
            return

        # 샘플 금융 상품 데이터 리스트
        finance_products = [
            {
                "name": "청년희망적금",
                "description": "청년들을 위한 고금리 적금 상품으로, 정부 지원 혜택이 있습니다."
            },
            {
                "name": "신한은행 정기예금",
                "description": "안정적인 이자율을 제공하는 신한은행의 정기예금 상품입니다."
            },
            {
                "name": "국채 투자 상품",
                "description": "국가에서 발행하는 채권에 투자하여 안정적인 수익을 얻을 수 있습니다."
            },
            {
                "name": "신한은행 적립식 펀드",
                "description": "장기적으로 투자할 수 있는 적립식 펀드 상품입니다."
            }
        ]
        # Weaviate에 각 금융 상품 데이터 추가
        for product in finance_products:
            client.data_object.create(data_object=product, class_name="FinanceProduct")
        logger.info("금융 상품 데이터가 Weaviate에 성공적으로 추가되었습니다.")
    except Exception as e:
        logger.error(f"금융 상품 데이터를 추가하는 중 오류 발생: {e}")
        st.error(f"금융 상품 데이터를 추가하는 중 오류가 발생했습니다: {e}")

# Streamlit에서 조회 결과를 출력하는 함수
def display_finance_products():
    products = get_finance_products()
    if products:
        st.write("Weaviate에 저장된 금융 상품 목록:")
        for product in products:
            product_name = product.get("name", "이름 없음")
            product_description = product.get("description", "설명 없음")
            st.write(f"상품명: {product_name}")
            st.write(f"설명: {product_description}")
            st.write("---")
    else:
        st.write("금융 상품을 불러오지 못했습니다.")

# Weaviate에 데이터 스키마 생성
def create_weaviate_schema():
    try:
        existing_classes = [cls['class'] for cls in client.schema.get()["classes"]]
        if "FinanceProduct" not in existing_classes:
            finance_product_schema = {
                "class": "FinanceProduct",
                "description": "금융 상품에 대한 정보",
                "properties": [
                    {"name": "name", "dataType": ["text"], "description": "상품명"},
                    {"name": "description", "dataType": ["text"], "description": "상품 설명"}
                ]
            }
            client.schema.create_class(finance_product_schema)
            logger.info("FinanceProduct 스키마가 생성되었습니다.")
        if "Document" not in existing_classes:
            document_schema = {
                "class": "Document",
                "description": "A document from PDF files",
                "properties": [
                    {"name": "filename", "dataType": ["text"], "description": "The name of the PDF file"},
                    {"name": "content", "dataType": ["text"], "description": "The original content of the PDF"},
                    {"name": "processed_content", "dataType": ["text"], "description": "The processed content of the PDF"},
                    {"name": "category", "dataType": ["text"], "description": "Document category"}
                ]
            }
            client.schema.create_class(document_schema)
            logger.info("Document 스키마가 생성되었습니다.")
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

# MBTI 기반 금융 상품 추천 함수
def classify_product_with_mbti(income_level, wants_loan, age, mbti):
    try:
        # I vs E: 안정성 vs. 고수익
        if 'I' in mbti.upper():
            risk_preference = "안정성"
        else:
            risk_preference = "고수익"

        # J vs P: 장기성 vs. 단기성
        if 'J' in mbti.upper():
            term_preference = "장기성"
        else:
            term_preference = "단기성"

        # N vs S: 미래 수익 변동성 vs. 현재 고정 이자율
        if 'N' in mbti.upper():
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

# Weaviate에 데이터 저장 함수 수정 (카테고리 포함)
def save_to_weaviate(filename, content, processed_content, category=None):
    try:
        data_object = {
            "filename": filename,
            "content": content,
            "processed_content": processed_content,
            "category": category
        }
        client.data_object.create(data_object=data_object, class_name="Document")
        logger.info(f"{filename} 파일이 성공적으로 Weaviate에 저장되었습니다.")

        # 데이터 저장 후 바로 조회하여 확인
        response = client.query.get("Document", ["filename"]).with_where({
            "path": ["filename"],
            "operator": "Equal",
            "valueText": filename
        }).do()
        documents = response.get("data", {}).get("Get", {}).get("Document", [])
        if documents:
            logger.info(f"{filename} 파일이 Weaviate에 저장되었음을 확인했습니다.")
        else:
            logger.error(f"{filename} 파일이 Weaviate에 저장되지 않았습니다.")
    except Exception as e:
        logger.error(f"Weaviate에 데이터를 저장하는 중 오류: {e}")
        st.error(f"{filename} 파일을 저장하는 중 오류가 발생했습니다. 오류: {e}")

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

# Streamlit 애플리케이션
def main():
    st.title("📄 PDF 내용 추출 및 Weaviate 저장 시스템")

    # Weaviate 연결 확인
    if not check_weaviate_connection():
        return

    # Weaviate 스키마 생성
    create_weaviate_schema()

    # PDF 파일 추출 및 Weaviate에 저장
    st.header("1️⃣ PDF 내용 추출 및 DB 저장")
    pdf_folder = st.text_input("📁 PDF 파일이 저장된 폴더 경로를 입력하세요", "/Users/im-woojin/Desktop/신한은행/신한은행_데이터")
    if st.button("🔍 PDF 내용 추출 및 DB 저장", key="extract_save"):
        if not os.path.exists(pdf_folder):
            st.error("입력한 폴더 경로가 존재하지 않습니다. 다시 확인해주세요.")
            return

        with st.spinner("📄 PDF 파일에서 텍스트를 추출 중입니다..."):
            filenames, documents = extract_text_from_pdfs(pdf_folder)
            if not filenames:
                st.warning("해당 폴더에 PDF 파일이 없거나 텍스트를 추출할 수 없습니다.")
                return
            processed_documents = [preprocess_text(doc) for doc in documents]
            for filename, content, proc_content in zip(filenames, documents, processed_documents):
                category = classify_product(proc_content)
                save_to_weaviate(filename, content, proc_content, category)
            st.success("🚀 모든 문서가 Weaviate에 성공적으로 저장되었습니다!")

        # 금융 상품 데이터 추가
        add_finance_products()
        st.success("💰 금융 상품 샘플 데이터가 Weaviate에 추가되었습니다.")

    # DB 시각화
    st.header("2️⃣ DB 시각화")
    if st.button("📊 모든 문서 보기", key="view_documents"):
        try:
            response = client.query.get("Document", ["filename", "content"]).do()
            documents = response.get("data", {}).get("Get", {}).get("Document", [])
            if documents:
                st.write("DB에 저장된 문서들:")
                st.write(documents)  # 조회된 데이터 구조 확인
                for doc in documents:
                    st.write(f"**파일명**: {doc.get('filename', 'N/A')}")
                    st.write(f"**내용 요약**: {doc.get('content', '')[:200]}...")  # 내용의 일부만 출력
                    st.write("---")
            else:
                st.warning("DB에 저장된 문서가 없습니다.")
        except Exception as e:
            logger.error(f"문서 조회 중 오류 발생: {e}")
            st.error(f"문서 조회 중 오류가 발생했습니다: {e}")

    # 금융 상품 조회
    st.header("3️⃣ 금융 상품 조회")
    if st.button("금융 상품 조회", key="view_finance_products"):
        display_finance_products()

    # 4. LLM 기반 대화 시스템
    st.header("4️⃣ LLM 기반 대화 시스템")
    user_query = st.text_input("💬 질문을 입력하세요")
    if st.button("💡 질문 처리", key="process_query"):
        with st.spinner("LLM에서 응답을 생성하고 있습니다..."):
            # LLM의 응답에 따라 Weaviate 조작 반영
            result = handle_user_query(user_query)
            st.subheader("🤖 LLM의 응답")
            st.write(result)

        # 대화 기록 출력
        st.write("### 📝 대화 기록")
        for message in st.session_state.messages:
            if message["role"] == "user":
                st.write(f"**사용자:** {message['content']}")
            elif message["role"] == "assistant":
                st.write(f"**AI:** {message['content']}")

    # 5. 사용자 정보 입력 및 MBTI 기반 금융 상품 추천
    st.header("5️⃣ 사용자 정보 입력 및 MBTI 기반 금융 상품 추천")
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

# LLM 응답에 따라 Weaviate 조작 함수 호출
def handle_llm_response(response):
    try:
        # 명령어 형식 파싱
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
                    # 카테고리 분석 및 저장
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
                return response  # 알 수 없는 명령어인 경우 그대로 반환

        else:
            return response  # 명령어 형식이 아닌 경우 그대로 반환

    except Exception as e:
        logger.error(f"LLM 응답 처리 중 오류 발생: {e}")
        return "응답 처리 중 오류가 발생했습니다."

# LLM 기반 대화 시스템에서 Weaviate 조작 통합
def handle_user_query(user_query):
    if 'messages' not in st.session_state:
        st.session_state.messages = []

    st.session_state.messages.append({"role": "user", "content": user_query})

    # 시스템 프롬프트 추가
    system_prompt = """
    너는 Weaviate 데이터베이스를 조작할 수 있는 AI 어시스턴트야. 수행할 수 있는 명령은 다음과 같아:
    - 문서 추가: 'ADD_DOCUMENT', 인자 'filename'과 'content'를 사용.
    - 문서 삭제: 'DELETE_DOCUMENT', 인자 'filename'을 사용.
    - 문서 업데이트: 'UPDATE_DOCUMENT', 인자 'filename'과 'new_content'를 사용.
    - 그룹화 및 매핑 수행: 'PERFORM_GROUPING_AND_MAPPING'.
    명령을 수행하고자 할 때는 다음 형식으로 응답해줘:
    [COMMAND]: [ARGUMENTS]
    예를 들어, 'DELETE_DOCUMENT: filename.pdf', 'ADD_DOCUMENT: filename.pdf: content', 'UPDATE_DOCUMENT: filename.pdf: new_content'.
    명령을 수행할 필요가 없다면 평소처럼 대화를 이어가줘.
    """

    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(st.session_state.messages)

    try:
        completion = openai.ChatCompletion.create(
            model="gpt-4",
            messages=messages,
            max_tokens=500,
            temperature=0.7
        )
        answer = completion.choices[0].message['content'].strip()

        # LLM 응답을 해석하여 Weaviate 조작 반영
        result = handle_llm_response(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})
        save_conversation_history(st.session_state.messages)
        return result
    except Exception as e:
        logger.error(f"LLM 질문 처리 중 오류 발생: {e}")
        return "LLM 질문을 처리하는 중 오류가 발생했습니다."

# 대화 기록 파일 경로
CONVERSATION_HISTORY_FILE = "conversation_history.json"

# 대화 기록을 파일로부터 로드
def load_conversation_history():
    if os.path.exists(CONVERSATION_HISTORY_FILE):
        with open(CONVERSATION_HISTORY_FILE, "r") as f:
            return json.load(f)
    return []

# 대화 기록을 파일에 저장
def save_conversation_history(conversations):
    with open(CONVERSATION_HISTORY_FILE, "w") as f:
        json.dump(conversations, f)

# Weaviate에서 문서 삭제 함수
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
            logger.info(f"{filename} 문서가 성공적으로 삭제되었습니다.")
        else:
            logger.warning(f"{filename} 문서를 찾을 수 없습니다.")
            st.error(f"{filename} 문서를 찾을 수 없습니다.")
    except Exception as e:
        logger.error(f"{filename} 문서 삭제 중 오류: {e}")
        st.error(f"{filename} 문서를 삭제하는 중 오류가 발생했습니다.")

# Weaviate에서 문서 업데이트 함수
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
            logger.info(f"{filename} 문서가 성공적으로 업데이트되었습니다.")
        else:
            logger.warning(f"{filename} 문서를 찾을 수 없습니다.")
            st.error(f"{filename} 문서를 찾을 수 없습니다.")
    except Exception as e:
        logger.error(f"{filename} 문서 업데이트 중 오류: {e}")
        st.error(f"{filename} 문서를 업데이트하는 중 오류가 발생했습니다.")

# Weaviate에서 문서 그룹화 및 매핑 수행
def perform_grouping_and_mapping():
    try:
        # Weaviate에서 문서 가져오기
        response = client.query.get("Document", ["filename", "content"]).do()
        documents = response.get("data", {}).get("Get", {}).get("Document", [])

        if not documents:
            return "Weaviate에 문서가 없습니다."

        # LLM에 전달할 문서 내용 구성
        context = "\n\n".join([f"{doc['filename']}: {doc['content'][:200]}" for doc in documents])  # 각 문서 요약
        prompt = f"""
        다음 문서들을 그룹화하고, 각 문서를 금융 상품에 맞게 매핑해 주세요.

        문서 목록:
        {context}

        1. 각 문서를 관련성에 따라 그룹화해 주세요.
        2. 각 그룹에 적절한 금융 상품 카테고리를 매핑해 주세요.
        """

        # LLM 호출하여 그룹화 및 매핑 요청
        completion = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1000,
            temperature=0.7
        )
        grouped_answer = completion.choices[0].message['content'].strip()

        # 그룹화된 데이터를 Weaviate에 저장
        save_grouped_data_to_weaviate(grouped_answer)

        return grouped_answer
    except Exception as e:
        logger.error(f"그룹화 및 매핑 수행 중 오류 발생: {e}")
        return "그룹화 및 매핑 작업 중 오류가 발생했습니다."

# 그룹화된 데이터를 Weaviate에 저장하는 함수
def save_grouped_data_to_weaviate(grouped_data):
    try:
        # 그룹화된 데이터를 저장할 클래스 생성 (필요 시)
        existing_classes = [cls['class'] for cls in client.schema.get()["classes"]]
        if "GroupedDocument" not in existing_classes:
            grouped_document_schema = {
                "class": "GroupedDocument",
                "description": "Grouped documents after LLM processing",
                "properties": [
                    {"name": "group_id", "dataType": ["text"], "description": "Group ID"},
                    {"name": "group_content", "dataType": ["text"], "description": "Content of the group"}
                ]
            }
            client.schema.create_class(grouped_document_schema)
            logger.info("GroupedDocument 스키마가 생성되었습니다.")

        # 각 그룹을 분리하여 Weaviate에 저장
        groups = grouped_data.split("\n\n")  # 그룹 간 구분
        for i, group in enumerate(groups, start=1):
            data_object = {
                "group_id": f"group_{i}",
                "group_content": group,
            }
            client.data_object.create(data_object=data_object, class_name="GroupedDocument")
            logger.info(f"그룹 {i} 데이터가 Weaviate에 저장되었습니다.")
    except Exception as e:
        logger.error(f"그룹 데이터를 Weaviate에 저장하는 중 오류 발생: {e}")
        st.error(f"그룹 데이터를 저장하는 중 오류가 발생했습니다.")

if __name__ == "__main__":
    main()

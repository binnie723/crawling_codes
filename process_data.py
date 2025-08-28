import pandas as pd
import re
import os

output_directory = "./acc/acc_output"


def match_re(inputs, re_expression):
    if not isinstance(inputs, str):
        return None
    matched = re.search(re_expression, inputs, re.DOTALL)
    if matched:
        group_result = matched.group(1)
        if group_result is not None:
            return group_result.strip()
    return None


def extract_filtered_accessory(text):
    if not isinstance(text, str):
        return None
    matched_keyword = re.search(r'부속품[:]\s*', text)
    if matched_keyword:
        remaining_text = text[matched_keyword.end():]
        lines = remaining_text.splitlines()
        
        if lines:
            first_line_content = lines[0].strip()
            
            exclude_keywords = ["매장가", "시중가","TC", "상태등급", "보테가베네타", "샤넬", "루이비통", "구찌", "디올", "에르메스", "페라가모", "프라다", "생로랑", "지방시", "발렌시아가"]
            
            for keyword in exclude_keywords:
                
                if keyword in first_line_content:
                    return None
            
            return first_line_content
    return None

if __name__ == "__main__":

    df_raw = pd.read_csv(output_directory + '/raw_data.csv')
    raw_data = df_raw["상품 상세"].values.tolist()
    df_raw['상품명'] = df_raw['상품명'].fillna('') 

    df_processed = pd.DataFrame({
        'rank': df_raw['순위'],
        'page': df_raw['페이지'],
        '상품명': df_raw['상품명'].apply(lambda x: re.sub(r'\[명품다올동래]\s*', '', x)),
        '판매가': df_raw['상품가격'],
        '상태': None,
        '각인': None,
        '색상': None,
        '소재': None,
        '사이즈': None,
        '부속품': None,
        '구입시기': None,
        '구입가': None
    })

    df_processed["상태"] = df_raw["상품 상세"].apply(
        lambda x: match_re(x, r'상태\s*/\s*([^\n]+)')
    )

    df_processed["각인"] = df_raw["상품 상세"].apply(
        lambda x: match_re(x, r'([A-Z]{1,2})\s*각인')
    )
    df_processed["색상"] = df_raw["상품 상세"].apply(
        lambda x: match_re(x, r'([^\n]+)\s*(?:색상|컬러|스킨)')
    )
    df_processed["소재"] = df_raw["상품 상세"].apply(
        lambda x: match_re(x, r'([^\n]+)\s*소재')
    )

    df_processed["사이즈"] = df_raw["상품 상세"].apply(
        lambda x: match_re(x, r'사이즈\s*/\s*([^\n]+)')
    )

    df_processed["부속품"] = df_raw["상품 상세"].apply(
        lambda x: match_re(x, r'구성품\s*/\s*([^\n]+)')
    )

    df_processed["구입시기"] = df_raw["상품 상세"].apply(
        lambda x: match_re(x, r'(\d{4}년 \d{1,2})월')
    )

    df_processed["구입가"] = df_raw["상품 상세"].apply(
        lambda x: match_re(x, r'(?:[^\n:]*?\s*)(?:매장가|시중가) \s*([^\n]+)\s*(?:입니다|입니다.)')
    )
    

    df_processed.to_csv(os.path.join(output_directory, "acc_data.csv"), index=False, encoding="utf-8-sig")
    print("분석 결과가" + str(output_directory) + "에 저장되었습니다.")


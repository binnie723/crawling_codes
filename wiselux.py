import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import pandas as pd
from bs4 import BeautifulSoup
import shutil
import base64
import re
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CATEGORY_DIR = os.path.join(BASE_DIR, "scarf_muffler")  # 카테고리 변경시 변경경

CAT_OUTPUT_DIR = os.path.join(CATEGORY_DIR, "scarf_muffler_output")  # 카테고리 변경시 변경 
THUMBNAIL_DIR = os.path.join(CATEGORY_DIR, "썸네일")
DETAIL_IMAGES_DIR = os.path.join(CATEGORY_DIR, "상세사진")

if not os.path.exists(CAT_OUTPUT_DIR):
    os.makedirs(CAT_OUTPUT_DIR)
if not os.path.exists(THUMBNAIL_DIR):
    os.makedirs(THUMBNAIL_DIR)
if not os.path.exists(DETAIL_IMAGES_DIR):
    os.makedirs(DETAIL_IMAGES_DIR)

def setup_driver():
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    
    user_agent_string = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    chrome_options.add_argument(f"user-agent={user_agent_string}")
    
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    
    chrome_options.add_argument("--incognito")
    
    chrome_options.page_load_strategy = 'normal'
    
    service = Service(ChromeDriverManager().install())
    
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_window_size(1200, 800)
    return driver

def save_image(image_url, filename, folder_path):
    if not image_url.startswith("data:image") and image_url.startswith("//"):
        image_url = "https:" + image_url
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        file_path = os.path.join(folder_path, filename)
        if image_url.startswith("data:image"):
            match = re.match(r"data:image/(png|jpeg|jpg);base64,(.*)", image_url)
            if match:
                base64_data = match.group(2)
                image_bytes = base64.b64decode(base64_data)
                
                with open(file_path, 'wb') as out_file:
                    out_file.write(image_bytes)
                print(f"Base64 이미지 저장 완료: {filename}")

            else:
                print(f"오류: Base64 데이터 형식을 파싱할 수 없습니다: {image_url}")
        else:
            response = requests.get(image_url, stream=True, headers=headers)
            response.raise_for_status()
            with open(file_path, 'wb') as out_file:
                out_file.write(response.content)

        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                print(f"경고: {filename} 파일이 0KB로 저장되었습니다. (URL: {image_url})")
            else:
                pass
        else:
            print(f"오류: {filename} 파일이 생성되지 않았습니다.")

    except requests.exceptions.RequestException as e:
        status_code_info = f"상태 코드: {e.response.status_code}" if e.response is not None else "상태 코드 없음"
        print(f"이미지 다운로드 오류 ({image_url}): {e} ({status_code_info})")
    except Exception as e:
        print(f"이미지 처리 중 예상치 못한 오류 발생 ({image_url}): {e}")


def main(): 

    total_start_time = time.time() # --- 크롤링 시작
    print(f"[TIME LOG] 전체 크롤링 시작: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    driver = setup_driver()
    all_product_data = []
    
    # CSV 저장 관련 설정
    save_interval = 1  # 1개마다 저장
    last_save_count = 0
    
    current_page_number = 1  # ---------크롤링 시작 페이지 지정
    thumbnail_counter = (current_page_number - 1)*20 + 1

    base_url = "https://wiselux.co.kr/product/list.html?cate_no=209&page=" + str(current_page_number)
    time.sleep(1)
    
    try:
        # 사이트 접속 시간 측정
        site_access_start = time.time()
        print(f"사이트 접속 시도: {base_url}")
        driver.get(base_url)
        site_access_end = time.time()
        print(f"사이트 접속 완료 - {site_access_end - site_access_start:.2f}초")

        
        while True:

            page_start_time = time.time()  # -- 페이지별 크롤링 시작

            print(f"\n >>>>>>>>>>>>> Page {current_page_number} 크롤링 START")
            
            WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((By.XPATH, '//ul[@class="prdList grid4"]/li'))
            )
            
            print(f"현재 URL: {driver.current_url}")
            print("상품 목록 요소 찾기...")
            product_list_xpath = '//ul[@class="prdList grid4"]/li' 
            
            products_on_page = driver.find_elements(By.XPATH, product_list_xpath)
            print(f"총 {len(products_on_page)}개의 상품 발견")

            if not products_on_page:
                print(f"{current_page_number} 페이지에 상품이 없습니다. 크롤링을 종료합니다.")
                break

            product_info_list = []
            for i, product in enumerate(products_on_page):
                try:
                    thumbnail_element = product.find_element(By.CSS_SELECTOR, 'div.thumbnail img')
                    thumbnail_url = thumbnail_element.get_attribute('src')
                    
                    product_name_element = product.find_element(By.CSS_SELECTOR, 'div.description a')
                    product_name = product_name_element.text.strip()
                    
                    # 상품 상세 페이지 URL 추출
                    product_link_element = product.find_element(By.CSS_SELECTOR, 'div.description strong a')
                    product_url = product_link_element.get_attribute('href')
                    
                    product_info_list.append({
                        'index': i,
                        'name': product_name,
                        'thumbnail_url': thumbnail_url,
                        'product_url': product_url
                    })
                    
                except Exception as e:
                    print(f"상품 {i+1} 정보 수집 중 오류: {e}")
                    continue
            
            print(f"상품 정보 수집 완료: {len(product_info_list)}개")

            for product_info in product_info_list:
                i = product_info['index']
                product_name_on_list = product_info['name']
                thumbnail_url = product_info['thumbnail_url']
                product_url = product_info['product_url']
                
                product_start_time = time.time() # -- 개별 상품 크롤링 시작
                
                print(f"\n--- Page {current_page_number} : {i+1}번째 상품 처리 중 ")
                current_product_data_raw = {}
                
                try:

                    thumbnail_start = time.time()
                    current_product_data_raw['순위'] = thumbnail_counter
                    current_product_data_raw['페이지'] = current_page_number
                    current_product_data_raw['상품명'] = product_name_on_list
                    thumbnail_filename = f"{thumbnail_counter}.jpg"
                    save_image(thumbnail_url, thumbnail_filename, folder_path=THUMBNAIL_DIR)
                    current_product_data_raw['썸네일 이미지 파일명'] = thumbnail_filename
                    thumbnail_end = time.time()
                    print(f"썸네일 이미지 저장 완료 - {thumbnail_end - thumbnail_start:.2f}초")

                    product_page_start = time.time()
                    print(f"상품 페이지로 직접 이동: {product_name_on_list}")
                    driver.get(product_url)
                    time.sleep(0.1)  # 최소 대기
                    print(f"상품 페이지 이동 완료 - {time.time() - product_page_start:.2f}초")                   
                    detail_extraction_start = time.time()
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    
                    detail_product_name_element = soup.select_one('div.prd-detail-basic > h3')
                    current_product_data_raw['상품명'] = detail_product_name_element.text.strip() if detail_product_name_element else current_product_data_raw['상품명']

                    price_element = soup.select_one('strong#span_product_price_text')
                    current_product_data_raw['가격'] = price_element.text.strip() if price_element else None

                    all_p_class_0_texts = []
                    cont_div = soup.find('div', class_='cont')
                    if cont_div:
                        p_elements = cont_div.find_all('p', class_='0')
                        for p_tag in p_elements:
                            cleaned_text = p_tag.get_text(strip=True)
                            if cleaned_text:
                                all_p_class_0_texts.append(cleaned_text)
                    
                    current_product_data_raw['상품 상세'] = "\n".join(all_p_class_0_texts) if all_p_class_0_texts else None
                    detail_extraction_end = time.time()
                    print(f"상품 상세 정보 추출 완료 - 소요시간: {detail_extraction_end - detail_extraction_start:.2f}초")

                    # 상세 이미지 URL 수집 시간 측정
                    image_collection_start = time.time()
                    all_detail_images_tags = soup.find_all('img')
                    
                    # 상세 이미지 URL들을 수집
                    detail_image_urls = []
                    for img_tag in all_detail_images_tags:
                        # ec-data-src를 먼저 시도하고, 없으면 src를 시도합니다.
                        img_to_process = img_tag.get('ec-data-src') or img_tag.get('src')
                        
                        if img_to_process:
                            if img_to_process.startswith("data:image"):
                                pass
                            elif img_to_process.startswith("//"):
                                img_to_process = "https:" + img_to_process
                            elif img_to_process.startswith("http://"):
                                img_to_process = "https://" + img_to_process[7:]
                            elif not img_to_process.startswith('http'):
                                img_to_process = base_url.rstrip('/') + img_to_process
                            
                            if (('/detail/' in img_to_process) and 
                                (img_to_process.lower().endswith(('.jpg', '.jpeg', '.png'))) and 
                                ('wiselux.co.kr' in img_to_process)):
                                detail_image_urls.append(img_to_process)
                    
                    image_collection_end = time.time()
                    print(f"상세 이미지 URL 수집 완료 ({len(detail_image_urls)}개) - {image_collection_end - image_collection_start:.2f}초")
                    
                    # 병렬 이미지 다운로드 시간 측정
                    if detail_image_urls:
                        parallel_download_start = time.time()
                        def download_single_image(url_and_index):
                            url, index = url_and_index
                            detail_image_filename = f"{thumbnail_counter}_{index}.jpg"
                            save_image(url, detail_image_filename, folder_path=DETAIL_IMAGES_DIR)
                            return detail_image_filename
                        
                        # URL과 인덱스를 함께 전달
                        url_index_pairs = [(url, idx + 1) for idx, url in enumerate(detail_image_urls)]
                        
                        # ThreadPoolExecutor를 사용하여 병렬 다운로드
                        with ThreadPoolExecutor(max_workers=5) as executor:
                            futures = [executor.submit(download_single_image, pair) for pair in url_index_pairs]
                            downloaded_files = []
                            for future in as_completed(futures):
                                try:
                                    filename = future.result()
                                    downloaded_files.append(filename)
                                except Exception as e:
                                    print(f"이미지 다운로드 중 오류 발생: {e}")
                        
                        parallel_download_end = time.time()
                        print(f"상품 상세 이미지 병렬 다운로드 완료 ({len(downloaded_files)}장) - {parallel_download_end - parallel_download_start:.2f}초")
                    else:
                        print("다운로드할 상세 이미지가 없습니다.")

                    # 데이터를 메모리에 추가
                    all_product_data.append(current_product_data_raw)
                    
                    # 주기적 CSV 저장 (10개마다 또는 페이지 마지막)
                    current_count = len(all_product_data)
                    should_save = (
                        current_count - last_save_count >= save_interval or  # 10개 간격
                        i == len(products_on_page) - 1  # 페이지 마지막 상품
                    )
                    
                    if should_save:
                        csv_save_start = time.time()
                        df = pd.DataFrame(all_product_data)
                        csv_path = os.path.join(CAT_OUTPUT_DIR, "raw_data.csv")
                        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                        csv_save_end = time.time()
                        print(f"CSV 데이터 저장 완료 ({current_count}개 상품) - {csv_save_end - csv_save_start:.2f}초")
                        last_save_count = current_count

                    driver.back() 
                    product_page_end = time.time()
                    print(f"상품 페이지 처리 완료 - {product_page_end - product_page_start:.2f}초")

                    # 개별 상품 전체 처리 시간
                    product_end_time = time.time()
                    print(f"상품 {i+1} 전체 처리 완료 - 총 소요시간: {product_end_time - product_start_time:.2f}초") 

                except Exception as product_e:
                    print(f"상품 크롤링 중 오류 발생 (상품 번호 {i+1}): {product_e}")
                    try:
                        # 목록 페이지로 돌아가기
                        driver.back()
                    except:
                        pass
                    continue
                
                thumbnail_counter += 1
            
            # 페이지별 전체 처리 시간
            page_end_time = time.time()
            print(f"\n========== Page {current_page_number} 전체 처리 완료 - 총 소요시간: {page_end_time - page_start_time:.2f}초 ==========")
            current_page_number += 1

            try:
                # 페이지 이동 시간 측정
                page_navigation_start = time.time()
                print("크롤링 페이지 이동 중..")

                if current_page_number % 10 == 1:
                    next_block_button = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "xans-product-normalpaging")]/a[img[@alt="다음 페이지"]]'))
                    )
                    driver.execute_script("arguments[0].click();", next_block_button)
                    
                else:
                    if current_page_number % 10 == 0:
                        pagination_cur = 10
                    else:
                        pagination_cur = current_page_number % 10

                    next_page_button = driver.find_element(By.XPATH, f'//*[@id="contents"]/div[4]/ol/li[{pagination_cur}]/a')
                    next_page_button.click()
                
                page_navigation_end = time.time()
                print(f"Page {current_page_number}로 이동 완료 - {page_navigation_end - page_navigation_start:.2f}초")
                
            except Exception as e:
                print(f"페이지 이동 또는 로딩 중 오류 발생: {e}")
                print("페이지 링크를 찾을 수 없거나 더 이상 다음 페이지가 없습니다. 크롤링을 종료합니다.")
                break

            
    finally:
        if driver:
            driver.quit()
            print("WebDriver 종료.")
        
        # 최종 CSV 저장 (아직 저장되지 않은 데이터가 있을 경우)
        if all_product_data and len(all_product_data) > last_save_count:
            print("최종 데이터 저장 중...")
            df = pd.DataFrame(all_product_data)
            csv_path = os.path.join(CAT_OUTPUT_DIR, "raw_data.csv")
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"최종 CSV 저장 완료 ({len(all_product_data)}개 상품)")
        
        # 전체 크롤링 완료 시간
        total_end_time = time.time()
        total_duration = total_end_time - total_start_time
        print(f"\n[TIME LOG] ========== 전체 크롤링 완료 ==========")
        print(f"전체 소요시간: {total_duration:.2f}초 ({total_duration/60:.2f}분)")
        print(f"크롤링 종료: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if all_product_data:
            avg_time_per_product = total_duration / len(all_product_data)
            print(f"[TIME LOG] 상품당 평균 처리 시간: {avg_time_per_product:.2f}초")
            print(f"최종적으로 총 {len(all_product_data)}개의 상품 데이터가 수집되었습니다.")
        else:
            print("수집된 상품 데이터가 없습니다.")

if __name__ == "__main__":
    main()
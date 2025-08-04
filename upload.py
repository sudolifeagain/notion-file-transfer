import os
import math
import requests
import concurrent.futures
import shutil
import sys
import time
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

# --- Notion APIとスクリプトの動作に必要な設定値 ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")
FOLDER_PATH = os.getenv("FOLDER_PATH")
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28") # Notion APIのバージョン (LTS版を推奨)
TITLE_PROP_NAME = os.getenv("NOTION_TITLE_PROPERTY_NAME")
DESCRIPTION_PROP_NAME = os.getenv("NOTION_DESCRIPTION_PROPERTY_NAME")
TAG_PROP_NAME = os.getenv("NOTION_TAG_PROPERTY_NAME")
UPLOADED_FOLDER_PATH_ENV = os.getenv("UPLOADED_FOLDER_PATH")

# --- ファイルアップロードに関する設定 ---
PART_SIZE = 20 * 1024**2  # 1パートあたりのサイズ (Notionの推奨値: 20MB)
MAX_FILE_SIZE = 5 * 1024**3 # 1回のアップロードで扱える最大ファイルサイズ (NotionのAPI上限: 5GB)
MAX_WORKERS = 8 # 並列アップロード時の最大ワーカー（スレッド）数

# --- 必須の環境変数が設定されているかチェック ---
if not all([NOTION_TOKEN, DATABASE_ID, FOLDER_PATH, TITLE_PROP_NAME, DESCRIPTION_PROP_NAME, TAG_PROP_NAME]):
    raise ValueError("必要な環境変数が.envファイルに設定されていません。")

# --- アップロード完了後のファイル移動先フォルダを設定・作成 ---
if UPLOADED_FOLDER_PATH_ENV:
    UPLOADED_FOLDER_PATH = UPLOADED_FOLDER_PATH_ENV
else:
    UPLOADED_FOLDER_PATH = os.path.join(FOLDER_PATH, "uploaded")
os.makedirs(UPLOADED_FOLDER_PATH, exist_ok=True)

def create_file_upload(filename, part_count):
    """Notion APIに対し、マルチパートアップロードの開始を要求する."""
    url = "https://api.notion.com/v1/file_uploads"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }
    data = {
        "mode": "multi_part",
        "number_of_parts": part_count,
        "filename": filename
    }
    res = requests.post(url, headers=headers, json=data)
    if not res.ok:
        raise Exception(f"ファイルアップロード開始に失敗: {res.status_code} {res.text}")
    return res.json()

def send_file_part(upload_url, file_part_bytes, part_number):
    """署名付きURLに、ファイルの一部（パート）をアップロードする."""
    files = {'file': ('chunk', file_part_bytes)}
    data = {'part_number': str(part_number)}
    res = requests.post(upload_url, headers={
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION
    }, files=files, data=data)
    if not res.ok:
        raise Exception(f"パート{part_number}の送信に失敗: {res.status_code} {res.text}")
    return part_number

def complete_file_upload(upload_id):
    """全てのパートのアップロード完了をNotion APIに通知する."""
    url = f"https://api.notion.com/v1/file_uploads/{upload_id}/complete"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION
    }
    res = requests.post(url, headers=headers)
    if not res.ok:
        raise Exception(f"アップロード完了処理に失敗: {res.status_code} {res.text}")

def create_page_with_file(page_title, description_filename, chunk_index, total_chunks, file_upload_id):
    """アップロードしたファイルを添付したNotionページを新規作成する."""
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }
    # 説明プロパティには、指定されたファイル名を元にテキストを生成
    description_text = f"{description_filename} (chunk {chunk_index} of {total_chunks})"

    page_data = {
        "parent": {"database_id": DATABASE_ID},
        "properties": {
            TITLE_PROP_NAME: {
                # ページのタイトルには、指定されたpage_titleを使用
                "title": [{"text": {"content": page_title}}]
            },
            DESCRIPTION_PROP_NAME: {
                "rich_text": [{"text": {"content": description_text}}]
            },
            TAG_PROP_NAME: {
                "multi_select": [{"name": "Python"}]
            }
        },
        "children": [
            {
                "object": "block",
                "type": "video",
                "video": {
                    "type": "file_upload",
                    "file_upload": {"id": file_upload_id}
                }
            }
        ]
    }
    res = requests.post(url, headers=headers, json=page_data)
    if not res.ok:
        raise Exception(f"ページ作成に失敗: {res.status_code} {res.text}")
    return res.json()

# === メイン処理 ===
if __name__ == "__main__":
    for filename in os.listdir(FOLDER_PATH):
        filepath = os.path.join(FOLDER_PATH, filename)

        if not os.path.isfile(filepath):
            continue

        if not filename.lower().endswith((".mp4", ".mkv")):
            continue

        # APIに渡すためのファイル名を準備する。
        if filename.lower().endswith(".mkv"):
            notion_filename = os.path.splitext(filename)[0] + ".mp4"
        else:
            notion_filename = filename

        file_size = os.path.getsize(filepath)
        print(f"🚀 アップロード開始: '{filename}' ({file_size / 1024**2:.2f} MB)")
        if filename != notion_filename:
            print(f"  - 備考: Notion上では '{notion_filename}' として扱われます。")


        num_chunks = math.ceil(file_size / MAX_FILE_SIZE)
        for chunk_index in range(num_chunks):
            try:
                chunk_start = chunk_index * MAX_FILE_SIZE
                chunk_end = min(chunk_start + MAX_FILE_SIZE, file_size)
                chunk_size = chunk_end - chunk_start

                print(f"  - チャンク {chunk_index+1}/{num_chunks} ({chunk_size / 1024**2:.2f} MB) を処理中...")

                num_parts = math.ceil(chunk_size / PART_SIZE)

                name, ext = os.path.splitext(notion_filename)
                chunk_filename = f"{name}_chunk{chunk_index+1}{ext}"

                upload_obj = create_file_upload(chunk_filename, num_parts)
                upload_id = upload_obj["id"]
                upload_url = upload_obj["upload_url"]

                chunk_start_time = time.time()

                with open(filepath, "rb") as f:
                    f.seek(chunk_start)

                    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        futures = []
                        for part_num in range(1, num_parts + 1):
                            bytes_to_read = min(PART_SIZE, chunk_size - (part_num - 1) * PART_SIZE)
                            part_bytes = f.read(bytes_to_read)
                            future = executor.submit(send_file_part, upload_url, part_bytes, part_num)
                            futures.append(future)

                        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
                            future.result()
                            percent = (i / num_parts) * 100
                            
                            bar_len = 30
                            filled_len = int(bar_len * i // num_parts)
                            bar = '█' * filled_len + '-' * (bar_len - filled_len)
                            
                            elapsed_time = time.time() - chunk_start_time
                            if elapsed_time > 0:
                                avg_speed = ((i / num_parts) * chunk_size / 1024**2) / elapsed_time
                            else:
                                avg_speed = 0
                            
                            minutes, seconds = divmod(int(elapsed_time), 60)
                            time_str = f"{minutes:02d}:{seconds:02d}"

                            display_text = f"\r    - 送信中: [{bar}] {percent:.1f}% ({time_str}, {avg_speed:.2f} MB/s) "
                            sys.stdout.write(display_text)
                            sys.stdout.flush()
                        
                        print()

                print("  - 全パートの送信完了。アップロードを最終処理中...")
                complete_file_upload(upload_id)

                ### 変更 ###
                # ページ作成関数を呼び出す
                # page_title: Notionページのタイトル（.mp4に置換したファイル名）
                # description_filename: 説明プロパティに記録するファイル名（元の.mkvファイル名）
                page_info = create_page_with_file(
                    page_title=notion_filename,
                    description_filename=filename, # 元のファイル名を渡す
                    chunk_index=chunk_index + 1,
                    total_chunks=num_chunks,
                    file_upload_id=upload_id
                )
                page_url = page_info.get("url")

                print(f"  🎉 チャンク {chunk_index+1}/{num_chunks} アップロード完了！")
                if page_url:
                    print(f"  📄 Notionページ: {page_url}")

                if chunk_index + 1 == num_chunks:
                    dest_path = os.path.join(UPLOADED_FOLDER_PATH, filename)
                    print(f"  - ファイルを '{dest_path}' に移動します。")
                    shutil.move(filepath, dest_path)
                    print(f"  ✅ 移動完了。")

            except Exception as e:
                print()
                print(f"      ❌ エラー発生: {e}")
                print("      このファイルのアップロード処理を中断します。")
                break
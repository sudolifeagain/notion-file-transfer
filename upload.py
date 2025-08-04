import os
import math
import requests
import concurrent.futures
import shutil
import sys
import time
from tqdm import tqdm

# load_dotenv は不要なので削除

def create_file_upload(filename, part_count, notion_token, notion_version):
    """Notion APIに対し、マルチパートアップロードの開始を要求する."""
    url = "https://api.notion.com/v1/file_uploads"
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": notion_version,
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

def send_file_part(upload_url, file_part_bytes, part_number, notion_token, notion_version):
    """署名付きURLに、ファイルの一部（パート）をアップロードする."""
    files = {'file': ('chunk', file_part_bytes)}
    data = {'part_number': str(part_number)}
    res = requests.post(upload_url, headers={
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": notion_version
    }, files=files, data=data)
    if not res.ok:
        raise Exception(f"パート{part_number}の送信に失敗: {res.status_code} {res.text}")
    return part_number

def complete_file_upload(upload_id, notion_token, notion_version):
    """全てのパートのアップロード完了をNotion APIに通知する."""
    url = f"https://api.notion.com/v1/file_uploads/{upload_id}/complete"
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": notion_version
    }
    res = requests.post(url, headers=headers)
    if not res.ok:
        raise Exception(f"アップロード完了処理に失敗: {res.status_code} {res.text}")

def create_page_with_file(page_title, description_filename, chunk_index, total_chunks, file_upload_id, config, notion_token, notion_version):
    """アップロードしたファイルを添付したNotionページを新規作成する."""
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": notion_version,
        "Content-Type": "application/json"
    }
    description_text = f"{description_filename} (chunk {chunk_index} of {total_chunks})"

    page_data = {
        "parent": {"database_id": config.get("DATABASE_ID")},
        "properties": {
            config.get("NOTION_TITLE_PROPERTY_NAME", "名前"): {
                "title": [{"text": {"content": page_title}}]
            },
            config.get("NOTION_DESCRIPTION_PROPERTY_NAME", "説明"): {
                "rich_text": [{"text": {"content": description_text}}]
            },
            config.get("NOTION_TAG_PROPERTY_NAME", "タグ"): {
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

def main(config):
    """メイン処理"""
    # --- GUIから渡された設定値を読み込む ---
    NOTION_TOKEN = config.get("NOTION_TOKEN")
    DATABASE_ID = config.get("DATABASE_ID")
    FOLDER_PATH = config.get("FOLDER_PATH")
    UPLOADED_FOLDER_PATH_ENV = config.get("UPLOADED_FOLDER_PATH")
    NOTION_VERSION = config.get("NOTION_VERSION", "2022-06-28")
    TITLE_PROP_NAME = config.get("NOTION_TITLE_PROPERTY_NAME", "名前")
    DESCRIPTION_PROP_NAME = config.get("NOTION_DESCRIPTION_PROPERTY_NAME", "説明")
    TAG_PROP_NAME = config.get("NOTION_TAG_PROPERTY_NAME", "タグ")

    # --- ファイルアップロードに関する設定 ---
    PART_SIZE = 20 * 1024**2
    MAX_FILE_SIZE = 5 * 1024**3
    MAX_WORKERS = 8

    # --- 必須の環境変数が設定されているかチェック ---
    if not all([NOTION_TOKEN, DATABASE_ID, FOLDER_PATH, TITLE_PROP_NAME, DESCRIPTION_PROP_NAME, TAG_PROP_NAME]):
        print("エラー: 必須項目(Token, DB ID, 各種プロパティ名, フォルダパス)が設定されていません。")
        return

    # --- アップロード完了後のファイル移動先フォルダを設定・作成 ---
    if UPLOADED_FOLDER_PATH_ENV:
        UPLOADED_FOLDER_PATH = UPLOADED_FOLDER_PATH_ENV
    else:
        UPLOADED_FOLDER_PATH = os.path.join(FOLDER_PATH, "uploaded")
    os.makedirs(UPLOADED_FOLDER_PATH, exist_ok=True)

    for filename in os.listdir(FOLDER_PATH):
        filepath = os.path.join(FOLDER_PATH, filename)

        if not os.path.isfile(filepath):
            continue
        if not filename.lower().endswith((".mp4", ".mkv")):
            continue

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

                upload_obj = create_file_upload(chunk_filename, num_parts, NOTION_TOKEN, NOTION_VERSION)
                upload_id = upload_obj["id"]
                upload_url = upload_obj["upload_url"]

                with open(filepath, "rb") as f:
                    f.seek(chunk_start)
                    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        futures = []
                        for part_num in range(1, num_parts + 1):
                            bytes_to_read = min(PART_SIZE, chunk_size - (part_num - 1) * PART_SIZE)
                            part_bytes = f.read(bytes_to_read)
                            future = executor.submit(send_file_part, upload_url, part_bytes, part_num, NOTION_TOKEN, NOTION_VERSION)
                            futures.append(future)

                        # tqdm を使った進捗表示
                        with tqdm(total=num_parts, desc="  - 送信中", unit="part", ncols=100) as pbar:
                            for future in concurrent.futures.as_completed(futures):
                                future.result()
                                pbar.update(1)

                print("  - 全パートの送信完了。アップロードを最終処理中...")
                complete_file_upload(upload_id, NOTION_TOKEN, NOTION_VERSION)

                page_info = create_page_with_file(
                    page_title=notion_filename,
                    description_filename=filename,
                    chunk_index=chunk_index + 1,
                    total_chunks=num_chunks,
                    file_upload_id=upload_id,
                    config=config,
                    notion_token=NOTION_TOKEN,
                    notion_version=NOTION_VERSION
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
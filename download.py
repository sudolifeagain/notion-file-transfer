import os
import requests
import re
import shutil
import concurrent.futures
from collections import defaultdict
from tqdm import tqdm

# --- スクリプト設定 (調整可能なパラメータ) ---
# 同時に実行するダウンロード処理の最大数。PCやネットワーク環境に応じて調整 (8-16が一般的)
MAX_WORKERS = 12
# 1ファイルのダウンロードパーツのサイズ(MB)。安定しない場合は小さくする (5-20MBが一般的)
PART_SIZE_MB = 16


def query_database(session, database_id, headers):
    """Notionデータベースを検索し、指定タグを両方持つページのリストを取得します。"""
    print("🔍 Notionデータベースから 'Python' および 'ダウンロード待ち' タグを持つページを検索しています...")
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    query = {
        "filter": {"and": [{"property": "タグ", "multi_select": {"contains": "Python"}}, {"property": "タグ", "multi_select": {"contains": "ダウンロード待ち"}}]},
        "sorts": [{"property": "作成日時", "direction": "ascending"}]
    }
    all_results, has_more, next_cursor = [], True, None
    while has_more:
        if next_cursor: query["start_cursor"] = next_cursor
        res = session.post(url, headers=headers, json=query) # session を使用
        res.raise_for_status()
        data = res.json()
        all_results.extend(data["results"])
        has_more = data["has_more"]
        next_cursor = data["next_cursor"]
    if not all_results: print("✅ 条件に一致するページは見つかりませんでした。")
    else: print(f"✅ {len(all_results)} 件のページが見つかりました。")
    return all_results

def get_block_children(session, page_id, headers):
    """指定されたページIDに属するブロックのリストを取得します。"""
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    res = session.get(url, headers=headers) # session を使用
    return res.json()["results"] if res.ok else None

def update_page_tags(session, page_id, current_tags, downloaded_tag_name, headers):
    """ページのタグから「ダウンロード待ち」を削除し、「ダウンロード済み」を追加します。"""
    new_tags = [tag for tag in current_tags if tag.get("name") != "ダウンロード待ち"]
    if not any(tag.get("name") == downloaded_tag_name for tag in new_tags):
        new_tags.append({"name": downloaded_tag_name})

    original_tag_set = {(t.get('id'), t.get('name')) for t in current_tags}
    new_tag_set = {(t.get('id'), t.get('name')) for t in new_tags}
    if original_tag_set == new_tag_set: return

    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {"properties": {"タグ": {"multi_select": new_tags}}}
    try:
        res = session.patch(url, headers=headers, json=payload) # session を使用
        res.raise_for_status()
        print(f"  - ページ(ID: {page_id})のタグを更新しました。")
    except requests.exceptions.RequestException as e:
        print(f"  ❌ ページ(ID: {page_id})のタグ更新に失敗: {e}")

def download_part(session, url, part_path, byte_range, pbar):
    """単一のパーツをダウンロードする関数。ThreadPoolExecutorから呼ばれる。"""
    try:
        headers = {'Range': f"bytes={byte_range[0]}-{byte_range[1]}"}
        with session.get(url, headers=headers, stream=True, timeout=60) as res:
            res.raise_for_status()
            with open(part_path, 'wb') as f:
                for chunk in res.iter_content(chunk_size=8192):
                    f.write(chunk)
                    pbar.update(len(chunk))
        return (part_path, True)
    except Exception as e:
        pbar.write(f"  - パーツダウンロード失敗: {os.path.basename(part_path)} - {e}")
        return (part_path, False)

def get_task_size(session, task):
    """ダウンロードタスクのファイルサイズを取得し、タスク辞書に追加して返す"""
    try:
        with session.get(task['url'], stream=True, allow_redirects=True, timeout=15) as res:
            res.raise_for_status()
            task['size'] = int(res.headers.get('content-length', 0))
    except requests.exceptions.RequestException:
        task['size'] = 0
    return task

def main(config):
    """メイン処理"""
    # --- GUIから渡された設定値を読み込む ---
    NOTION_TOKEN = config.get("NOTION_TOKEN")
    DATABASE_ID = config.get("DATABASE_ID")
    DOWNLOAD_FOLDER_PATH = config.get("DOWNLOAD_FOLDER_PATH")
    NOTION_VERSION = config.get("NOTION_VERSION", "2022-06-28")
    DESCRIPTION_PROP_NAME = config.get("NOTION_DESCRIPTION_PROPERTY_NAME", "説明")
    NOTION_DOWNLOADED_TAG_NAME = config.get("NOTION_DOWNLOADED_TAG_NAME", "ダウンロード済み")

    if not all([NOTION_TOKEN, DATABASE_ID, DOWNLOAD_FOLDER_PATH, DESCRIPTION_PROP_NAME]):
        print("エラー: 必須項目(Token, DB ID, 説明プロパティ名, ダウンロード先フォルダ)が設定されていません。")
        return

    HEADERS = {"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": NOTION_VERSION, "Content-Type": "application/json"}
    os.makedirs(DOWNLOAD_FOLDER_PATH, exist_ok=True)
    temp_chunk_dir = os.path.join(DOWNLOAD_FOLDER_PATH, "temp_chunks")

    # requests.Session() を使用して接続を再利用
    with requests.Session() as session:
        try:
            os.makedirs(temp_chunk_dir, exist_ok=True)
            pages = query_database(session, DATABASE_ID, HEADERS)
            if not pages: return

            tasks_to_size_check, chunk_files_info = [], defaultdict(list)
            print("📋 ダウンロード対象のファイルをリストアップしています...")
            for page in pages:
                page_id, props = page["id"], page["properties"]
                desc_prop = props.get(DESCRIPTION_PROP_NAME)
                if not (desc_prop and desc_prop.get("rich_text")): continue
                description = desc_prop["rich_text"][0]["plain_text"]
                match = re.search(r"^(.*?) \(chunk (\d+) of (\d+)\)$", description)
                if not match: continue
                
                original_filename, chunk_num, total_chunks = match.groups()
                chunk_num, total_chunks = int(chunk_num), int(total_chunks)
                blocks = get_block_children(session, page_id, HEADERS)
                if not blocks: continue
                
                file_url = next((b["video"]["file"]["url"] for b in blocks if b["type"] == "video" and b.get("video", {}).get("type") == "file"), None)
                if not file_url: continue

                tags = props.get("タグ", {}).get("multi_select", [])
                task_info = {'url': file_url, 'page_id': page_id, 'tags': tags}
                if total_chunks == 1:
                    task_info['path'] = os.path.join(DOWNLOAD_FOLDER_PATH, original_filename)
                else:
                    chunk_filename = f"{original_filename}.part{chunk_num:03d}"
                    task_info['path'] = os.path.join(temp_chunk_dir, chunk_filename)
                    chunk_files_info[original_filename].append(task_info)
                tasks_to_size_check.append(task_info)

            if not tasks_to_size_check:
                print("ダウンロード対象のファイルはありませんでした。")
                return

            print(f"\n📊 {len(tasks_to_size_check)}個のファイルの合計サイズを計算しています...")
            download_tasks = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(get_task_size, session, task) for task in tasks_to_size_check]
                for future in tqdm(concurrent.futures.as_completed(futures), total=len(tasks_to_size_check), desc="サイズ計算進捗"):
                    download_tasks.append(future.result())

            total_download_size = sum(task.get('size', 0) for task in download_tasks)
            if total_download_size == 0:
                print("\nダウンロード対象のデータが見つかりませんでした。")
                return

            print(f"\n⚡ 合計 {total_download_size / 1024**3:.2f} GB のダウンロードを開始します...")
            
            # --- 並列処理の新しい構造 ---
            all_parts_to_download = []
            files_to_reconstruct = defaultdict(list)

            part_size = PART_SIZE_MB * 1024 * 1024
            for task in download_tasks:
                if task.get('size', 0) == 0: continue
                
                final_path = task['path']
                if os.path.exists(final_path):
                    print(f"  - スキップ: '{os.path.basename(final_path)}' は既に存在します。")
                    total_download_size -= task['size'] # 全体進捗から除外
                    continue

                temp_dir = final_path + ".parts"
                os.makedirs(temp_dir, exist_ok=True)
                
                num_parts = (task['size'] + part_size - 1) // part_size
                files_to_reconstruct[final_path] = {
                    'parts': [os.path.join(temp_dir, f"part_{i:04d}") for i in range(num_parts)],
                    'task_info': task
                }

                for i in range(num_parts):
                    start_byte = i * part_size
                    end_byte = min((i + 1) * part_size - 1, task['size'] - 1)
                    part_path = os.path.join(temp_dir, f"part_{i:04d}")
                    all_parts_to_download.append({
                        'url': task['url'],
                        'path': part_path,
                        'range': (start_byte, end_byte)
                    })

            if not all_parts_to_download:
                print("\n全てのファイルはダウンロード済みです。")
                return

            # 単一のThreadPoolExecutorで全パーツをダウンロード
            with tqdm(total=total_download_size, unit='iB', unit_scale=True, desc="全体進捗") as pbar:
                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    future_to_part = {executor.submit(download_part, session, part['url'], part['path'], part['range'], pbar): part for part in all_parts_to_download}
                    
                    for future in concurrent.futures.as_completed(future_to_part):
                        part_path, success = future.result()
                        # 失敗した場合は関連ファイルの再構築をキャンセル
                        if not success:
                            for final_path, data in list(files_to_reconstruct.items()):
                                if part_path in data['parts']:
                                    pbar.write(f"\nダウンロード失敗のため、'{os.path.basename(final_path)}'の処理を中断。")
                                    del files_to_reconstruct[final_path]

            print("\n--- ダウンロード完了ファイルの結合とタグ更新 ---")
            for final_path, data in files_to_reconstruct.items():
                print(f"  - 結合中: '{os.path.basename(final_path)}'")
                try:
                    with open(final_path, 'wb') as dest_file:
                        for part_path in data['parts']:
                            with open(part_path, 'rb') as src_file:
                                shutil.copyfileobj(src_file, dest_file)
                    # 結合成功後、関連ページのタグを更新
                    task_info = data['task_info']
                    update_page_tags(session, task_info['page_id'], task_info['tags'], NOTION_DOWNLOADED_TAG_NAME, HEADERS)
                finally:
                    shutil.rmtree(os.path.dirname(data['parts'][0])) # .parts フォルダを削除

        finally:
            if os.path.exists(temp_chunk_dir) and not os.listdir(temp_chunk_dir):
                print("\n🧹 空の一時フォルダ 'temp_chunks' を削除します。")
                os.rmdir(temp_chunk_dir)
            print("\n🎉 全ての処理が完了しました。")
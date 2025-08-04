import os
import requests
import re
import shutil
import concurrent.futures
from collections import defaultdict
from dotenv import load_dotenv
from tqdm import tqdm

# .envファイルから環境変数を読み込む
load_dotenv()

# --- スクリプト設定 ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")
DESCRIPTION_PROP_NAME = os.getenv("NOTION_DESCRIPTION_PROPERTY_NAME")
DOWNLOAD_FOLDER_PATH = os.getenv("DOWNLOAD_FOLDER_PATH")
# ダウンロード完了時に付与するタグの名前
NOTION_DOWNLOADED_TAG_NAME = os.getenv("NOTION_DOWNLOADED_TAG_NAME", "ダウンロード済み")
# 同時に処理するタスクの最大数
MAX_WORKERS = 8
# 1ファイルを分割する際の、1パーツあたりのサイズ(MB)
PART_SIZE_MB = 10

# --- APIリクエスト用ヘッダー ---
HEADERS = {"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": NOTION_VERSION, "Content-Type": "application/json"}

def query_database():
    """Notionデータベースを検索し、指定タグを両方持つページのリストを取得します。"""
    print("🔍 Notionデータベースから 'Python' および 'ダウンロード待ち' タグを持つページを検索しています...")
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    query = {
        "filter": {"and": [{"property": "タグ", "multi_select": {"contains": "Python"}}, {"property": "タグ", "multi_select": {"contains": "ダウンロード待ち"}}]},
        "sorts": [{"property": "作成日時", "direction": "ascending"}]
    }
    all_results = []
    has_more = True
    next_cursor = None
    while has_more:
        if next_cursor: query["start_cursor"] = next_cursor
        res = requests.post(url, headers=HEADERS, json=query)
        res.raise_for_status()
        data = res.json()
        all_results.extend(data["results"])
        has_more = data["has_more"]
        next_cursor = data["next_cursor"]
    if not all_results: print("✅ 条件に一致するページは見つかりませんでした。")
    else: print(f"✅ {len(all_results)} 件のページが見つかりました。")
    return all_results

def get_block_children(page_id):
    """指定されたページIDに属するブロックのリストを取得します。"""
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    res = requests.get(url, headers=HEADERS);
    return res.json()["results"] if res.ok else None

def update_page_tags(page_id, current_tags, downloaded_tag_name):
    """ページのタグから「ダウンロード待ち」を削除し、「ダウンロード済み」を追加します。"""
    # 「ダウンロード待ち」を除外し、既に「ダウンロード済み」がないか確認
    new_tags = [tag for tag in current_tags if tag.get("name") != "ダウンロード待ち"]
    has_downloaded_tag = any(tag.get("name") == downloaded_tag_name for tag in new_tags)

    # 「ダウンロード済み」タグがなければ追加
    if not has_downloaded_tag:
        new_tags.append({"name": downloaded_tag_name})

    # タグ構成が変更されている場合のみAPIを呼び出す
    # (元のタグリストとIDと名前のセットを比較)
    original_tag_set = { (t.get('id'), t.get('name')) for t in current_tags }
    new_tag_set = { (t.get('id'), t.get('name')) for t in new_tags }
    if original_tag_set == new_tag_set:
        return

    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = { "properties": { "タグ": { "multi_select": new_tags } } }
    try:
        res = requests.patch(url, headers=HEADERS, json=payload)
        res.raise_for_status()
        print(f"  - ページ(ID: {page_id})のタグを更新しました。")
    except requests.exceptions.RequestException as e:
        print(f"  ❌ ページ(ID: {page_id})のタグ更新に失敗: {e}")

def download_file_multipart(url, final_path, total_size, pbar):
    """単一のファイルを複数パーツに分割し、並列ダウンロードします。"""
    if os.path.exists(final_path):
        pbar.write(f"  - スキップ: '{os.path.basename(final_path)}' は既に存在します。")
        pbar.update(total_size)
        return True

    part_size = PART_SIZE_MB * 1024 * 1024
    num_parts = (total_size + part_size - 1) // part_size
    temp_dir = final_path + ".parts"
    os.makedirs(temp_dir, exist_ok=True)
    
    tasks = [{'range': (i * part_size, min((i + 1) * part_size - 1, total_size - 1)),
              'path': os.path.join(temp_dir, f"part_{i:04d}"), 'url': url}
             for i in range(num_parts)]

    download_success = True
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        def _download_part(part_task):
            try:
                headers = {'Range': f"bytes={part_task['range'][0]}-{part_task['range'][1]}"}
                res = requests.get(part_task['url'], headers=headers, stream=True, timeout=60)
                res.raise_for_status()
                with open(part_task['path'], 'wb') as f:
                    for chunk in res.iter_content(chunk_size=8192):
                        f.write(chunk)
                        pbar.update(len(chunk))
                return True
            except Exception:
                return False

        futures = [executor.submit(_download_part, task) for task in tasks]
        for future in concurrent.futures.as_completed(futures):
            if not future.result(): download_success = False

    if not download_success:
        pbar.write(f"\nダウンロード失敗のため、'{os.path.basename(final_path)}'の処理を中断。")
        shutil.rmtree(temp_dir)
        return False

    try:
        with open(final_path, 'wb') as dest_file:
            for i in range(num_parts):
                part_path = os.path.join(temp_dir, f"part_{i:04d}")
                with open(part_path, 'rb') as src_file: shutil.copyfileobj(src_file, dest_file)
    except IOError as e:
        pbar.write(f"  ❌ ファイル結合失敗: {e}")
        return False
    finally:
        shutil.rmtree(temp_dir)
    return True

def reconstruct_file_from_chunks(chunk_paths, final_path):
    """Notionの仕様で分割されたチャンクファイル群を、一つのファイルに結合して復元します。"""
    print(f"\n  - Notionチャンクを結合中... -> '{os.path.basename(final_path)}'")
    chunk_paths.sort()
    with open(final_path, 'wb') as final_file:
        for chunk_path in chunk_paths:
            with open(chunk_path, 'rb') as chunk_file: shutil.copyfileobj(chunk_file, final_file)
    for chunk_path in chunk_paths: os.remove(chunk_path)
    print(f"  ✅ ファイルの復元が完了しました: {final_path}")

def get_task_size(task):
    """ダウンロードタスクのファイルサイズを取得し、タスク辞書に追加して返す"""
    try:
        with requests.get(task['url'], stream=True, allow_redirects=True, timeout=15) as res:
            res.raise_for_status()
            task['size'] = int(res.headers.get('content-length', 0))
    except requests.exceptions.RequestException:
        task['size'] = 0
    return task

def main():
    """メイン処理"""
    os.makedirs(DOWNLOAD_FOLDER_PATH, exist_ok=True)
    temp_chunk_dir = os.path.join(DOWNLOAD_FOLDER_PATH, "temp_chunks")

    try:
        os.makedirs(temp_chunk_dir, exist_ok=True)
        pages = query_database()
        if not pages: return

        # 1. ダウンロードタスクのリストアップ
        tasks_to_size_check = []
        chunk_files_info = defaultdict(list)
        
        print("📋 ダウンロード対象のファイルをリストアップしています...")
        for page in pages:
            page_id, properties = page["id"], page["properties"]
            desc_prop = properties.get(DESCRIPTION_PROP_NAME)
            if not desc_prop or not desc_prop["rich_text"]: continue
            description = desc_prop["rich_text"][0]["plain_text"]
            match = re.search(r"^(.*?) \(chunk (\d+) of (\d+)\)$", description)
            if not match: continue
            original_filename, chunk_num, total_chunks = match.groups()
            chunk_num, total_chunks = int(chunk_num), int(total_chunks)
            blocks = get_block_children(page_id)
            if not blocks: continue
            file_url = next((b["video"]["file"]["url"] for b in blocks if b["type"] == "video" and b["video"]["type"] == "file"), None)
            if not file_url: continue

            tags = properties.get("タグ", {}).get("multi_select", [])
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

        # 2. 全タスクのファイルサイズを並列で取得
        print(f"\n📊 {len(tasks_to_size_check)}個のファイルの合計サイズを計算しています...")
        download_tasks = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(get_task_size, task) for task in tasks_to_size_check]
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(tasks_to_size_check), desc="サイズ計算進捗"):
                download_tasks.append(future.result())

        total_download_size = sum(task.get('size', 0) for task in download_tasks)
        if total_download_size == 0:
            print("\nダウンロード対象のデータが見つかりませんでした。")
            return

        # 3. 全タスクの並列ダウンロード実行
        print(f"\n⚡ 合計 {total_download_size / 1024**3:.2f} GB のダウンロードを開始します...")
        successful_tasks = []
        with tqdm(total=total_download_size, unit='iB', unit_scale=True, desc="全体進捗") as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_task = {executor.submit(download_file_multipart, task['url'], task['path'], task['size'], pbar): task
                                  for task in download_tasks if task.get('size', 0) > 0}
                
                for future in concurrent.futures.as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        if future.result():
                            successful_tasks.append(task)
                    except Exception as e:
                        pbar.write(f"タスク実行中にエラーが発生: {task['path']} - {e}")

        # 4. 完了したタスクのタグを更新
        print("\n--- ダウンロード完了ページのタグを更新します ---")
        for task in successful_tasks:
            is_chunk = any(task in chunk_list for chunk_list in chunk_files_info.values())
            if not is_chunk:
                update_page_tags(task['page_id'], task['tags'], NOTION_DOWNLOADED_TAG_NAME)

        # 5. Notionチャンクの結合とタグ更新
        if chunk_files_info:
            print("\n--- Notionチャンクファイルの結合処理を開始します ---")
            for filename, chunk_task_list in chunk_files_info.items():
                chunk_paths = [task['path'] for task in chunk_task_list]
                if all(os.path.exists(p) for p in chunk_paths):
                    print(f"📂 ファイル '{filename}' のチャンクが全て揃いました。")
                    final_path = os.path.join(DOWNLOAD_FOLDER_PATH, filename)
                    reconstruct_file_from_chunks(chunk_paths, final_path)
                    
                    print(f"  - '{filename}' に関連するページのタグを更新します...")
                    for task_info in chunk_task_list:
                        update_page_tags(task_info['page_id'], task_info['tags'], NOTION_DOWNLOADED_TAG_NAME)
                else:
                    print(f"⚠️ ファイル '{filename}' はチャンクが不足しているため結合・タグ更新できません。")
    
    finally:
        # 6. 最終クリーンアップ処理
        if os.path.exists(temp_chunk_dir) and not os.listdir(temp_chunk_dir):
            print("\n🧹 空の一時フォルダ 'temp_chunks' を削除します。")
            os.rmdir(temp_chunk_dir)
        print("\n🎉 全ての処理が完了しました。")

if __name__ == "__main__":
    main()

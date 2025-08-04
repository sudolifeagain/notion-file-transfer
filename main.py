import PySimpleGUI as sg
import json
import os
import sys
import threading
import upload   # 既存スクリプトをインポート
import download # 既存スクリプトをインポート

CONFIG_FILE = "config.json"

def load_config():
    """設定ファイルを読み込む。なければデフォルト値を返す."""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {} # 初回起動時は空

def save_config(values):
    """GUIの入力値を設定ファイルに保存する."""
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(values, f, indent=4, ensure_ascii=False)

def run_task(target_func, window):
    """GUIが固まらないように、別スレッドで処理を実行する."""
    config = load_config()
    if not config.get("NOTION_TOKEN") or not config.get("DATABASE_ID"):
        print("エラー: Notion TokenとDatabase IDは必須です。")
        window['UPLOAD'].update(disabled=False)
        window['DOWNLOAD'].update(disabled=False)
        return

    try:
        target_func(config)
        print(f"\n--- ✅ 処理が正常に完了しました ---")
    except Exception as e:
        print(f"\n--- ❌ エラーが発生しました ---\n{e}")
    finally:
        # 処理完了後、ボタンを再度有効化
        window['UPLOAD'].update(disabled=False)
        window['DOWNLOAD'].update(disabled=False)

def main():
    sg.theme_global("SystemDefault")
    config = load_config()

    # --- GUIのレイアウト定義 ---
    layout = [
        [sg.Text("Notion 大容量ファイル転送ツール", font=("Helvetica", 16, "bold"))],
        [sg.Text("Notion Token"), sg.InputText(config.get("NOTION_TOKEN", ""), key="NOTION_TOKEN", password_char='*')],
        [sg.Text("Database ID "), sg.InputText(config.get("DATABASE_ID", ""), key="DATABASE_ID")],
        [sg.Text("アップロード対象フォルダ"), sg.InputText(config.get("FOLDER_PATH", ""), key="FOLDER_PATH"), sg.FolderBrowse("選択")],
        [sg.Text("ダウンロード先フォルダ"), sg.InputText(config.get("DOWNLOAD_FOLDER_PATH", ""), key="DOWNLOAD_FOLDER_PATH"), sg.FolderBrowse("選択")],
        [sg.Button("設定を保存"), sg.Push(), sg.Button("アップロード実行", key="UPLOAD"), sg.Button("ダウンロード実行", key="DOWNLOAD")],
        [sg.Output(size=(80, 20), key='-OUTPUT-')] # 処理ログの出力エリア
    ]

    window = sg.Window("Notion Uploader/Downloader", layout)

    while True:
        event, values = window.read()
        if event == sg.WIN_CLOSED:
            break
        
        # --- ボタン操作の処理 ---
        if event == "設定を保存":
            save_config(values)
            sg.popup("設定を保存しました。")
        
        if event in ("UPLOAD", "DOWNLOAD"):
            # ログ出力をGUIのOutputウィジェットに切り替え
            sys.stdout = window['-OUTPUT-']
            sys.stderr = window['-OUTPUT-']
            window['-OUTPUT-'].update('') # 出力エリアをクリア
            
            # ボタンを無効化して多重実行を防止
            window['UPLOAD'].update(disabled=True)
            window['DOWNLOAD'].update(disabled=True)
            
            # 実行する関数を選択
            task_function = upload.main if event == "UPLOAD" else download.main
            
            # 別スレッドでタスクを実行
            threading.Thread(target=run_task, args=(task_function, window), daemon=True).start()

    window.close()

if __name__ == "__main__":
    # --- 重要: 既存スクリプトから import するため、main() の呼び出しはガードする ---
    main()
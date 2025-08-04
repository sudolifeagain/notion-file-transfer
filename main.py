import FreeSimpleGUI as sg
import json
import os
import sys
import threading
import upload   # 既存スクリプトをインポート
import download # 既存スクリプトをインポート

CONFIG_FILE = "config.json"

# --- stdout/stderrをGUIにリダイレクトするためのカスタムクラス ---
class GuiOutputStream:
    """
    print()の出力をGUIのMultiline要素にリダイレクトする。
    tqdmの進捗バーが1行で更新されるように、行頭復帰文字(\r)を処理する。
    """
    def __init__(self, multiline_element):
        self.multiline = multiline_element
        self.is_tqdm_line = False

    def write(self, text):
        # tqdmは行の更新に '\r' を使う
        is_progress_bar = text.endswith('\r')

        # 直前の行がtqdmの行で、現在の行もそうであれば、前の行を更新する
        if self.is_tqdm_line and (is_progress_bar or '\r' in text):
            # Multilineウィジェットの最後の行を削除
            current_value = self.multiline.get()
            last_newline = current_value.rfind('\n')
            if last_newline == -1:
                new_value = ''
            else:
                new_value = current_value[:last_newline + 1]
            self.multiline.update(value=new_value)

        # 新しいテキストを書き込む
        # '\r' を取り除き、末尾に改行を追加しない
        cleaned_text = text.strip('\r')
        self.multiline.print(cleaned_text, end='')

        # 現在の行がtqdmの行だったか状態を保存
        self.is_tqdm_line = is_progress_bar

    def flush(self):
        # print()関数が内部で呼び出す可能性があるため、このメソッドは必須
        pass


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
    # テーマ設定を互換性のある方法で行う
    try:
        sg.theme("SystemDefault")
    except:
        try:
            sg.theme_global("SystemDefault")
        except:
            pass  # テーマ設定に失敗した場合はデフォルトを使用
    
    config = load_config()

    # PySimpleGUIのバージョン互換性のための要素名
    try:
        Text = sg.Text
        InputText = sg.InputText
        Button = sg.Button
        FolderBrowse = sg.FolderBrowse
        Output = sg.Output
        Push = sg.Push
    except AttributeError:
        # 新しいバージョンでの代替
        Text = getattr(sg, 'T', getattr(sg, 'Text', sg.Text))
        InputText = getattr(sg, 'I', getattr(sg, 'Input', sg.InputText))
        Button = getattr(sg, 'B', getattr(sg, 'Btn', sg.Button))
        FolderBrowse = getattr(sg, 'FolderBrowse', sg.FolderBrowse)
        Output = getattr(sg, 'Output', sg.Output)
        Push = getattr(sg, 'Push', lambda: sg.Text("", expand_x=True))

    # --- GUIのレイアウト定義 ---
    layout = [
        [Text("Notion 大容量ファイル転送ツール", font=("Helvetica", 16, "bold"))],
        [Text("Notion Token"), InputText(config.get("NOTION_TOKEN", ""), key="NOTION_TOKEN", password_char='*')],
        [Text("Database ID "), InputText(config.get("DATABASE_ID", ""), key="DATABASE_ID")],
        [Text("アップロード対象フォルダ"), InputText(config.get("FOLDER_PATH", ""), key="FOLDER_PATH"), FolderBrowse("選択")],
        [Text("ダウンロード先フォルダ"), InputText(config.get("DOWNLOAD_FOLDER_PATH", ""), key="DOWNLOAD_FOLDER_PATH"), FolderBrowse("選択")],
        [Button("設定を保存"), Push(), Button("アップロード実行", key="UPLOAD"), Button("ダウンロード実行", key="DOWNLOAD")],
        [sg.Multiline(size=(80, 20), key='-OUTPUT-', disabled=True)] # autoscrollを削除
    ]

    # finalize=True にすることで、read()の前にウィジェットにアクセスできる
    window = sg.Window("Notion Uploader/Downloader", layout, finalize=True)

    # カスタム出力ストリームを作成
    gui_output_stream = GuiOutputStream(window['-OUTPUT-'])

    while True:
        event, values = window.read()
        if event == sg.WIN_CLOSED:
            break
        
        # --- ボタン操作の処理 ---
        if event == "設定を保存":
            save_config(values)
            sg.popup("設定を保存しました。")
        
        if event in ("UPLOAD", "DOWNLOAD"):
            # ログ出力をGUIのカスタムストリームに切り替え
            sys.stdout = gui_output_stream
            sys.stderr = gui_output_stream
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
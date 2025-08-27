import os
import json
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
from datetime import datetime
import sys
import re
import unicodedata
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict  # ★ 追加：結果をファイル別にまとめる用

# ================== 定数定義 ==================
CONFIG_FILE = "config.json"
MASTER_FILENAME = "規格板厚マスタ.xlsx"

# 機種定数
MACHINE_PLASMA = "41"
MACHINE_DIA_SPLICE = ["50", "51", "52", "53", "54"]
MACHINE_DIA_SPLICE_MAIN = ["50", "51", "52", "53"]
MACHINE_DECIMAL_CHECK = ["52", "53"]
MACHINE_DRILL_CHECK = "54"

# チェック閾値
MAX_DIMENSION = 6000
MAX_BOTH_DIMENSION = 500
MAX_THICKNESS_FOR_DECIMAL = 4.5  # 使っていないが将来用に残置
MAX_DRILL_SIZE = 51

# エラータイプ定数（UI表示には未使用だが拡張余地として残置）
ERROR_TYPES = {
    "MISSING_COLUMN": "列不足",
    "INVALID_THICKNESS_GRADE": "板厚材質不適合",
    "PLASMA_NAKAUKI": "プラズマ中抜きエラー",
    "DIA_DIMENSION": "ダイア寸法エラー",
    "SABI_COMMENT": "サビコメントエラー",
    "DIMENSION_OVER": "寸法超過",
    "BOTH_DIMENSION_OVER": "両寸法超過",
    "DECIMAL_NAKAUKI": "中抜き小数点エラー",
    "DRILL_MIX": "ドリル混在エラー",
    "DATE_INVALID": "日付不正",
    "DRILL_FORMAT": "ドリル形式エラー",
    "DRILL_SIZE_OVER": "ドリルサイズ超過"
}

# ================== 設定ファイルの読み書き ==================
def load_config():
    """設定ファイルを読み込み"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return get_default_config()
    return get_default_config()

def get_default_config():
    """デフォルト設定を返す"""
    return {
        "last_folder": "",
        "check_rules": {
            "thickness_grade": True,
            "plasma_nakauki": True,
            "dia_dimension": True,
            "sabi_comment": True,
            "dimension_limit": True,
            "decimal_nakauki": True,
            "drill_check": True,
            "date_check": True
        },
        "max_workers": 4
    }

def save_config(cfg):
    """設定ファイルを保存"""
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"設定保存エラー: {e}")

# ================== マスタ読み込み ==================
def load_master(master_path):
    """マスタファイルを読み込み"""
    try:
        df = pd.read_excel(master_path)
        df = df.rename(columns=lambda x: str(x).strip())
        return df[["規格", "板厚 最小", "板厚 最大"]].dropna(how="all")
    except Exception as e:
        raise Exception(f"マスタファイル読み込みエラー: {e}")

def is_valid_thickness_and_grade(row, master_df):
    """板厚と材質の組合せが有効かチェック"""
    try:
        thickness = float(row["板厚"])
        grade = str(row["材質"]).strip()
        
        matched = master_df[master_df["規格"] == grade]
        if matched.empty:
            return False
            
        for _, r in matched.iterrows():
            min_val = r["板厚 最小"]
            max_val = r["板厚 最大"]
            if pd.isna(min_val) and pd.isna(max_val):
                return True  # 無制限
            if pd.notna(min_val) and pd.notna(max_val):
                if min_val <= thickness <= max_val:
                    return True
        return False
    except (ValueError, KeyError):
        return False

# ================== 個別チェック関数 ==================
def _normalize_zen2han(s):
    """全角→半角を正規化。NaNは空文字扱い。前後の半角/全角スペースも除去。"""
    if pd.isna(s):
        return ""
    t = unicodedata.normalize("NFKC", str(s))  # 全角数字・記号・＠・．を半角へ
    return t.replace("\u3000", " ").strip()     # 全角スペースも除去

def check_plasma_nakauki(row, rowno, file_path):
    """プラズマ機種の中抜きチェック"""
    try:
        machine = str(row["機種"]).strip()
        if machine == MACHINE_PLASMA:
            nakauki_raw = row["中抜き"]
            if pd.isna(nakauki_raw):
                return None
            nakauki = str(nakauki_raw).strip()
            if nakauki == "":
                return None
            return (file_path, f"{rowno}行目: プラズマで中抜きがあります → 機種:{machine}, 中抜き:'{nakauki}'")
    except Exception as e:
        return (file_path, f"{rowno}行目: プラズマ中抜きチェックでエラー → {e}")
    return None

def check_dia_dimension(row, rowno, file_path):
    """ダイア・スプライス機種の寸法チェック"""
    try:
        machine = str(row["機種"]).strip()
        dimension = str(row["寸法・型切"]).strip()
        if machine in MACHINE_DIA_SPLICE and dimension != "1":
            return (file_path, f"{rowno}行目: ダイア・スプライスが寸法でない → 機種:{machine}, 寸法・型切:{dimension}")
    except Exception:
        pass
    return None

def check_sabi_comment(row, rowno, file_path):
    """サビとコメントの組合せチェック"""
    try:
        machine = str(row["機種"]).strip()
        sabi = str(row["サビ"]).strip()
        comment = str(row["コメント"]).strip()
        if machine in MACHINE_DIA_SPLICE_MAIN and sabi == "1" and comment != "33":
            return (file_path, f"{rowno}行目: サビありですがコメント33が入っていません → 機種:{machine}, サビ:{sabi}, コメント:{comment}")
    except Exception:
        pass
    return None

def check_dimensions(row, rowno, file_path):
    """寸法制限チェック"""
    errors = []
    try:
        width = float(row["幅"])
        length = float(row["長さ"])
        
        if width > MAX_DIMENSION or length > MAX_DIMENSION:
            errors.append((file_path, f"{rowno}行目: 幅または長さが{MAX_DIMENSION}を超過 → 幅:{width}, 長さ:{length}"))
        
        if width > MAX_BOTH_DIMENSION and length > MAX_BOTH_DIMENSION:
            errors.append((file_path, f"{rowno}行目: 幅と長さが両方{MAX_BOTH_DIMENSION}を超過 → 幅:{width}, 長さ:{length}"))
    except (ValueError, KeyError):
        pass
    return errors

def check_decimal_nakauki(row, rowno, file_path):
    """機種52・53：ドリルは空欄のみOK。板厚6.0以下は中抜き=小数必須＆小数第1位=5のみ許容（φ/@の間の数値を最優先）"""
    try:
        machine = str(row.get("機種", "")).strip()
        if machine not in MACHINE_DECIMAL_CHECK:
            return None

        # 板厚（coerceで厳密に数値化）
        thickness = pd.to_numeric(row.get("板厚", ""), errors="coerce")

        # --- ドリルは空欄のみOK（全角スペース等も除去して判定） ---
        drill_val = _normalize_zen2han(row.get("ドリル", ""))
        if drill_val != "":  # 何か入ってたら即NG
            return (file_path, f"{rowno}行目: 機種{machine}はドリル欄は空欄のみ許可 → ドリル:{drill_val}")

        # --- 中抜きチェック：板厚 <= 6.0 のときだけ ---
        if pd.notna(thickness) and thickness <= 6.0:
            nakauki_val_raw = row.get("中抜き", "")
            nakauki_val = _normalize_zen2han(nakauki_val_raw)
            
            if nakauki_val == "":
                return None  # 空欄はOK

            # 1) φ ... @ の間の数値を最優先で取得（例: φ22.5@3 → 22.5）
            m = re.search(r"φ\s*([0-9]+(?:\.[0-9]+)?)\s*@", nakauki_val)
            if m:
                number = m.group(1)
            else:
                # 2) 上記が無い場合は最初に出現する数値を拾う（例: "22.5 3個" など）
                m = re.search(r"([0-9]+(?:\.[0-9]+)?)", nakauki_val)
                if m:
                    number = m.group(1)
                else:
                    return (file_path, f"{rowno}行目: 中抜きに数値が見つかりません（小数第1位=5必須） → 中抜き:{nakauki_val_raw}")

            # 小数必須（整数や "22." のような不完全小数はNG）
            if "." not in number:
                return (file_path, f"{rowno}行目: 中抜きは小数必須（第1位=5）ですが整数です → 中抜き:{nakauki_val_raw}")

            parts = number.split(".")
            if len(parts) != 2 or parts[1] == "":
                return (file_path, f"{rowno}行目: 中抜きの小数部が不正 → 中抜き:{nakauki_val_raw}")
            
            decimal_part = parts[1]
            # 小数第1位が5でない場合はNG
            if decimal_part[0] != "5":
                return (file_path, f"{rowno}行目: 中抜きの小数第1位が5ではありません（実際: {decimal_part[0]}） → 中抜き:{nakauki_val_raw}")

        # 板厚 > 6.0 のときは中抜きチェック対象外（ただしドリルは上で既に禁止済み）
        return None

    except Exception as e:
        return (file_path, f"{rowno}行目: 機種52・53チェックでエラー → {e}")

def check_drill_format_and_size(row, rowno, file_path):
    """ドリルの形式とサイズチェック"""
    try:
        drill_raw = row["ドリル"]
        if pd.isna(drill_raw):
            return None
            
        drill_val = str(drill_raw).strip()
        if not drill_val:  # 空欄はOK
            return None
            
        if "@" not in drill_val:
            return (file_path, f"{rowno}行目: ドリルの形式が不正（@なし） → {drill_val}")
        
        parts = drill_val.split("@")
        if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
            return (file_path, f"{rowno}行目: ドリルの形式が不正（サイズまたは個数が空） → {drill_val}")
        
        try:
            size = float(parts[0])
            if size >= MAX_DRILL_SIZE:
                return (file_path, f"{rowno}行目: ドリルサイズが{MAX_DRILL_SIZE}以上 → {drill_val}")
        except ValueError:
            return (file_path, f"{rowno}行目: ドリルのサイズが数値でない → {drill_val}")
            
    except Exception as e:
        return (file_path, f"{rowno}行目: ドリルチェックで予期せぬエラー → {e}")
    return None

def check_drill_mix(row, rowno, file_path):
    """機種54のドリル混在チェック"""
    try:
        machine = str(row["機種"]).strip()
        if machine == MACHINE_DRILL_CHECK:
            drill_raw = row["ドリル"]
            if pd.isna(drill_raw):
                return None
            drill_val = str(drill_raw).strip()
            values = [v.strip() for v in drill_val.split(",") if v.strip()]
            if len(values) > 1 and len(set(values)) > 1:
                return (file_path, f"{rowno}行目: ドリルの値が混在 → 機種:{machine}, ドリル:{drill_val}")
    except Exception:
        pass
    return None

def check_dates(row, rowno, file_path):
    """日付チェック（Y≦today≦Z）"""
    try:
        y_date = pd.to_datetime(row["Y"], errors="coerce")
        z_date = pd.to_datetime(row["Z"], errors="coerce")
        today = pd.to_datetime(datetime.today().date())
        
        if pd.isna(y_date) or pd.isna(z_date) or y_date > today or z_date < today:
            return (file_path, f"{rowno}行目: 日付が不正（Y≦today≦Z） → Y:{row['Y']}, Z:{row['Z']}")
    except Exception:
        pass
    return None

# ================== メインチェック処理 ==================
def check_csv(file_path, master_df, check_rules):
    """CSVファイルをチェック"""
    try:
        # ファイル読み込み
        try:
            df = pd.read_csv(file_path, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding="cp932")

        df.columns = df.columns.str.strip()

        # 列名リネーム
        rename_map = {
            "ｷﾘ穴数": "ドリル",
            "穴詳細": "中抜き",
            "加工機種": "機種",
            "受注日": "Y",
            "納入日": "Z",
            "さび出し": "サビ",
            "寸法・型切": "寸法・型切" 
        }
        df.rename(columns=rename_map, inplace=True)

        # 必須列チェック
        required_cols = ["板厚", "材質", "幅", "長さ", "コメント", "ドリル", "中抜き", "機種", "寸法・型切", "サビ", "Y", "Z"]
        errors = []
        for col in required_cols:
            if col not in df.columns:
                errors.append((file_path, f"必須列が存在しません: '{col}'"))
                return errors

        # 行ごとのチェック
        for idx, row in df.iterrows():
            rowno = idx + 2

            # 板厚・材質チェック
            if check_rules.get("thickness_grade", True):
                if not is_valid_thickness_and_grade(row, master_df):
                    errors.append((file_path, f"{rowno}行目: 板厚と材質の組合せが無効 → 板厚:{row.get('板厚', 'N/A')}, 材質:{row.get('材質', 'N/A')}"))

            # プラズマ中抜きチェック
            if check_rules.get("plasma_nakauki", True):
                err = check_plasma_nakauki(row, rowno, file_path)
                if err:
                    errors.append(err)

            # ダイア寸法チェック
            if check_rules.get("dia_dimension", True):
                err = check_dia_dimension(row, rowno, file_path)
                if err:
                    errors.append(err)

            # サビコメントチェック
            if check_rules.get("sabi_comment", True):
                err = check_sabi_comment(row, rowno, file_path)
                if err:
                    errors.append(err)

            # 寸法制限チェック
            if check_rules.get("dimension_limit", True):
                dim_errors = check_dimensions(row, rowno, file_path)
                errors.extend(dim_errors)

            # 小数点中抜きチェック
            if check_rules.get("decimal_nakauki", True):
                err = check_decimal_nakauki(row, rowno, file_path)
                if err:
                    errors.append(err)

            # ドリルチェック
            if check_rules.get("drill_check", True):
                err = check_drill_format_and_size(row, rowno, file_path)
                if err:
                    errors.append(err)
                
                err = check_drill_mix(row, rowno, file_path)
                if err:
                    errors.append(err)

            # 日付チェック
            if check_rules.get("date_check", True):
                err = check_dates(row, rowno, file_path)
                if err:
                    errors.append(err)

        return errors

    except Exception as e:
        return [(file_path, f"ファイル処理エラー: {e}")]

def process_file(args):
    """並列処理用のファイル処理関数"""
    file_path, master_df, check_rules = args
    return check_csv(file_path, master_df, check_rules)

# ================== GUI本体 ==================
class CSVCheckerApp:
    def __init__(self, master):
        self.master = master
        self.master.title("CSVチェックツール v2.0")
        self.master.geometry("900x640")
        self.config = load_config()
        self.is_running = False

        self.folder_path = tk.StringVar(value=self.config.get("last_folder", ""))
        self.progress_var = tk.DoubleVar()
        self.status_var = tk.StringVar(value="準備完了")

        # ★ 追加：表示モード状態と結果保持用
        self.display_mode = tk.StringVar(value="全件まとめ")  # 「全件まとめ」or「ファイル別」
        self.all_errors = []           # [(file, message), ...]
        self.errors_by_file = {}       # {filename(str): [message, ...]}

        self.create_widgets()

    def create_widgets(self):
        """ウィジェット作成"""
        # フォルダ選択フレーム
        folder_frame = tk.Frame(self.master)
        folder_frame.grid(row=0, column=0, columnspan=3, sticky="ew", padx=5, pady=5)
        
        tk.Label(folder_frame, text="チェック対象フォルダ:").grid(row=0, column=0, sticky="w")
        tk.Entry(folder_frame, textvariable=self.folder_path, width=50).grid(row=0, column=1, sticky="ew", padx=5)
        tk.Button(folder_frame, text="選択", command=self.select_folder).grid(row=0, column=2)
        folder_frame.grid_columnconfigure(1, weight=1)

        # 設定フレーム
        settings_frame = tk.LabelFrame(self.master, text="チェック設定")
        settings_frame.grid(row=1, column=0, columnspan=3, sticky="ew", padx=5, pady=5)
        
        self.check_vars = {}
        rules = self.config.get("check_rules", {})
        rule_names = {
            "thickness_grade": "板厚・材質チェック",
            "plasma_nakauki": "プラズマ中抜きチェック",
            "dia_dimension": "ダイア寸法チェック",
            "sabi_comment": "サビコメントチェック",
            "dimension_limit": "寸法制限チェック",
            "decimal_nakauki": "中抜き小数点チェック",
            "drill_check": "ドリルチェック",
            "date_check": "日付チェック"
        }
        
        row, col = 0, 0
        for key, name in rule_names.items():
            var = tk.BooleanVar(value=rules.get(key, True))
            self.check_vars[key] = var
            tk.Checkbutton(settings_frame, text=name, variable=var).grid(row=row, column=col, sticky="w", padx=5)
            col += 1
            if col > 3:
                col = 0
                row += 1

        # 実行フレーム
        exec_frame = tk.Frame(self.master)
        exec_frame.grid(row=2, column=0, columnspan=3, pady=10)
        
        self.run_button = tk.Button(exec_frame, text="実行", command=self.run_check, bg="lightgreen")
        self.run_button.grid(row=0, column=0, padx=5)
        
        self.stop_button = tk.Button(exec_frame, text="停止", command=self.stop_check, bg="lightcoral", state="disabled")
        self.stop_button.grid(row=0, column=1, padx=5)

        # プログレスバー
        tk.Label(self.master, text="進捗:").grid(row=3, column=0, sticky="w", padx=5)
        self.progress_bar = ttk.Progressbar(self.master, variable=self.progress_var, maximum=100)
        self.progress_bar.grid(row=3, column=1, sticky="ew", padx=5)
        
        # ステータス
        tk.Label(self.master, textvariable=self.status_var).grid(row=4, column=0, columnspan=3, sticky="w", padx=5)

        # ★ 追加：表示モード切替（ラジオボタン）
        mode_frame = tk.Frame(self.master)
        mode_frame.grid(row=5, column=0, columnspan=3, sticky="w", padx=5)
        tk.Label(mode_frame, text="表示モード:").pack(side="left")
        tk.Radiobutton(mode_frame, text="全件まとめ", value="全件まとめ", variable=self.display_mode,
                       command=self.render_results).pack(side="left", padx=4)
        tk.Radiobutton(mode_frame, text="ファイル別", value="ファイル別", variable=self.display_mode,
                       command=self.render_results).pack(side="left", padx=4)

        # 結果表示
        tk.Label(self.master, text="チェック結果:").grid(row=6, column=0, sticky="nw", padx=5)
        
        tree_frame = tk.Frame(self.master)
        tree_frame.grid(row=7, column=0, columnspan=3, sticky="nsew", padx=5, pady=5)
        
        # Treeview 初期化（全件まとめ想定）
        self.tree = ttk.Treeview(tree_frame, columns=("ファイル", "内容"), show="headings")
        self.tree.heading("ファイル", text="ファイル名")
        self.tree.heading("内容", text="違反内容")
        self.tree.column("ファイル", width=220)
        self.tree.column("内容", width=580)
        # #0（ツリー列）は初期モードでは非表示だが、ファイル別モードで使う
        self.tree.column("#0", width=220, stretch=True)

        # スクロールバー
        v_scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        h_scrollbar = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        self.tree.grid(row=0, column=0, sticky="nsew")
        v_scrollbar.grid(row=0, column=1, sticky="ns")
        h_scrollbar.grid(row=1, column=0, sticky="ew")
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        # グリッド設定
        self.master.grid_rowconfigure(7, weight=1)
        self.master.grid_columnconfigure(1, weight=1)

    def select_folder(self):
        """フォルダ選択"""
        path = filedialog.askdirectory()
        if path:
            self.folder_path.set(path)

    def stop_check(self):
        """チェック停止"""
        self.is_running = False
        self.status_var.set("停止中...")

    def run_check(self):
        """チェック実行"""
        if self.is_running:
            return
            
        folder = self.folder_path.get()
        if not os.path.isdir(folder):
            messagebox.showerror("エラー", "フォルダが正しく選択されていません")
            return

        # 固定マスタファイルパス
        script_dir = os.path.dirname(sys.argv[0])
        master_path = os.path.join(script_dir, MASTER_FILENAME)
        if not os.path.isfile(master_path):
            messagebox.showerror("エラー", f"マスタファイルが見つかりません:\n{master_path}")
            return

        # 設定保存
        self.config["last_folder"] = folder
        self.config["check_rules"] = {key: var.get() for key, var in self.check_vars.items()}
        save_config(self.config)

        # UI状態変更
        self.run_button.config(state="disabled")
        self.stop_button.config(state="normal")
        self.is_running = True
        self.tree.delete(*self.tree.get_children())
        self.all_errors = []
        self.errors_by_file = {}

        # 別スレッドで実行
        thread = threading.Thread(target=self._run_check_thread, args=(folder, master_path))
        thread.daemon = True
        thread.start()

    def _run_check_thread(self, folder, master_path):
        """チェック処理スレッド"""
        try:
            self.status_var.set("マスタファイル読み込み中...")
            master_df = load_master(master_path)
            
            # CSVファイル一覧取得（先頭3桁が数字のものは除外）
            csv_files = []
            for fname in os.listdir(folder):
                if fname.lower().endswith(".csv") and not re.match(r"^\d{3}", fname):
                    csv_files.append(os.path.join(folder, fname))
            
            if not csv_files:
                self.status_var.set("処理対象のCSVファイルが見つかりません")
                self._reset_ui()
                return

            self.status_var.set(f"{len(csv_files)}個のファイルを処理中...")
            check_rules = {key: var.get() for key, var in self.check_vars.items()}
            
            all_errors = []
            max_workers = self.config.get("max_workers", 4)
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # タスク投入
                future_to_file = {
                    executor.submit(process_file, (file_path, master_df, check_rules)): file_path 
                    for file_path in csv_files
                }
                
                completed = 0
                for future in as_completed(future_to_file):
                    if not self.is_running:
                        break
                        
                    file_path = future_to_file[future]
                    try:
                        errors = future.result()
                        all_errors.extend(errors)
                    except Exception as e:
                        all_errors.append((file_path, f"処理エラー: {e}"))
                    
                    completed += 1
                    progress = (completed / len(csv_files)) * 100
                    self.progress_var.set(progress)
                    self.status_var.set(f"処理中... ({completed}/{len(csv_files)})")

            # 結果表示
            if self.is_running:
                self.master.after(0, self._display_results, all_errors)
            else:
                self.master.after(0, lambda: self.status_var.set("処理が停止されました"))
                
        except Exception as e:
            self.master.after(0, lambda: messagebox.showerror("エラー", f"処理中にエラーが発生しました:\n{e}"))
        finally:
            self.master.after(0, self._reset_ui)

    def _display_results(self, all_errors):
        """結果表示（内部状態に保持してから描画は render_results に委譲）"""
        if not all_errors:
            self.all_errors = []
            self.errors_by_file = {}
            self.tree.delete(*self.tree.get_children())
            self.status_var.set("すべて正常でした！")
            messagebox.showinfo("完了", "すべて正常でした！")
            return

        # 結果保持
        self.all_errors = all_errors
        by_file = defaultdict(list)
        for fp, msg in all_errors:
            by_file[os.path.basename(fp)].append(msg)
        self.errors_by_file = dict(by_file)

        # ステータス更新して描画
        self.status_var.set(f"チェック完了: {len(all_errors)}件のエラーが見つかりました（{len(self.errors_by_file)}ファイル）")
        self.render_results()

    def render_results(self):
        """表示モードに応じて Treeview を再描画"""
        self.tree.delete(*self.tree.get_children())

        if not self.all_errors:
            return

        mode = self.display_mode.get()

        if mode == "全件まとめ":
            # フラット表示：従来どおり2列テーブル
            self.tree.configure(show="headings")
            # （列設定は初期化時に済み）
            for fp, msg in self.all_errors:
                self.tree.insert("", "end", values=(os.path.basename(fp), msg))

        else:
            # ファイル別表示：親=ファイル名、子=エラー
            self.tree.configure(show="tree headings")  # #0 列を表示
            self.tree.heading("#0", text="ファイル")   # ツリー列の見出し
            # 各ファイルごとに親ノード作成
            for fname in sorted(self.errors_by_file.keys()):
                msgs = self.errors_by_file[fname]
                parent = self.tree.insert("", "end", text=f"{fname}（{len(msgs)}件）", values=("",))
                # 子ノードとして各メッセージ
                for m in msgs:
                    self.tree.insert(parent, "end", text="", values=("", m))
                # 展開して見やすく
                self.tree.item(parent, open=True)

    def _reset_ui(self):
        """UI状態をリセット"""
        self.run_button.config(state="normal")
        self.stop_button.config(state="disabled")
        self.is_running = False        # 念のためOFF
        self.progress_var.set(0)

# ================== 実行 ==================
if __name__ == "__main__":
    root = tk.Tk()
    app = CSVCheckerApp(root)
    root.mainloop()

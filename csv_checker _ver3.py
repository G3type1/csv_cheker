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
EPS = 1e-9

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
            "date_check": True,
            "shot_dimension": True,
            "machine_thickness": True,
            "file_mix": True,
            "shot_mix_15_18": True,
            "shot_mix_18_22": True,
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
    """在庫マスタ読込（A列=規格、C列以降=在庫板厚）
    - B列は無視（備考など自由に）
    - C以降で数値化できたセルだけ在庫として採用
    - DataFrameに _inventory_map を属性として持たせて返す
    """
    try:
        df = pd.read_excel(master_path)
        df = df.rename(columns=lambda x: str(x).strip())

        if "規格" not in df.columns:
            raise Exception("マスタに『規格』列がありません（A列に配置してください）。")

        from collections import defaultdict
        inv_map = defaultdict(set)

        # 行ごとに「規格」とC列以降の数値を収集（B列は無視）
        for _, r in df.iterrows():
            grade = str(r.get("規格", "")).strip()
            if not grade or grade.lower() == "nan":
                continue

            # C列（index=2）以降を数値化
            values = pd.to_numeric(r.iloc[2:], errors="coerce")
            for v in values.dropna():
                inv_map[grade].add(round(float(v), 3))  # 小数誤差対策で3桁丸め

        if not inv_map:
            raise Exception("在庫板厚が1件も読み取れませんでした。C列以降に数値を入力してください。")

        # 参照しやすいよう DataFrame に属性で付与
        df._inventory_map = {k: sorted(v) for k, v in inv_map.items()}
        return df

    except Exception as e:
        raise Exception(f"マスタファイル読み込みエラー: {e}")

def is_valid_thickness_and_grade(row, master_df):
    """在庫マスタ（_inventory_map）に '材質' があり、
    その在庫リストに '板厚' が一致（±0.001mm 許容）すればOK。
    それ以外はNG。
    """
    try:
        thickness = pd.to_numeric(row["板厚"], errors="coerce")
        if pd.isna(thickness):
            return False
        thickness = float(thickness)

        grade = str(row["材質"]).strip()

        inv_map = getattr(master_df, "_inventory_map", None)
        if inv_map is None:
            # 旧方式は廃止したので、ここに来たらマスタの読み方が間違い
            return False

        allowed = inv_map.get(grade, [])
        if not allowed:
            return False

        for t in allowed:
            if abs(thickness - float(t)) <= 1e-3:
                return True

        return False

    except Exception:
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
            return (file_path, f"{rowno}行目: 機種フィラーがドリルになっています → ドリル:{drill_val}")

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
                return (file_path, f"{rowno}行目: フィラーの中抜きUPしていません（実際: {decimal_part[0]}） → 中抜き:{nakauki_val_raw}")

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

def check_shot_dimension(row, rowno, file_path):
    """ショット寸法チェック：コメント=33のとき、幅・長さの両方が500を超えたらNG。
       例）600x400 → OK、495x700 → OK、510x510 → NG
    """
    try:
        comment = str(row["コメント"]).strip()
        if comment != "33":
            return None  # コメント33以外は対象外

        width = float(row["幅"])
        length = float(row["長さ"])

        if width > MAX_BOTH_DIMENSION and length > MAX_BOTH_DIMENSION:
            return (file_path,
                    f"{rowno}行目: ショット品の寸法が{MAX_BOTH_DIMENSION}×{MAX_BOTH_DIMENSION}を超過 → 幅:{width}, 長さ:{length}（コメント:{comment}）")
    except (ValueError, KeyError):
        # 幅/長さが数値でない・列が無い等はスルー（他の必須列チェックで拾われる）
        pass
    return None

def check_machine_thickness(row, rowno, file_path):
    """機種と板厚の組み合わせ制約をチェック（≧／≦は「含む」）
       40: 厚さ16以上
       41: 6以上かつ40まで
       42,44,45: 12まで
       50〜53: 4.5まで
       54: 16以上
    """
    try:
        machine = str(row.get("機種", "")).strip()
        t = pd.to_numeric(row.get("板厚", ""), errors="coerce")
        if pd.isna(t):
            return None
        thickness = float(t)

        # 40: >=16
        if machine == "40":
            if not (thickness >= 16 - EPS):
                return (file_path, f"{rowno}行目: 機種ガスは板厚16以上が条件 → 板厚:{thickness}")

        # 41: 6〜40（両端含む）
        elif machine == "41":
            if not (6 - EPS <= thickness <= 40 + EPS):
                return (file_path, f"{rowno}行目: 機種プラズマは板厚6以上40まで → 板厚:{thickness}")

        # 42,44,45: 〜12
        elif machine in ("42", "44", "45"):
            if not (thickness <= 12 + EPS):
                return (file_path, f"{rowno}行目: 機種レーザーは板厚12まで → 板厚:{thickness}")

        # 50〜53: 〜4.5
        elif machine in ("50", "51", "52", "53"):
            if not (thickness <= 4.5 + EPS):
                return (file_path, f"{rowno}行目: 機種フィラーは板厚4.5まで → 板厚:{thickness}")

        # 54: >=16
        elif machine == "54":
            if not (thickness >= 16 - EPS):
                return (file_path, f"{rowno}行目: 機種ガスは板厚16以上が条件 → 板厚:{thickness}")

        # その他の機種は対象外
        return None
    except Exception:
        return None
    
def check_file_mix(df: pd.DataFrame, file_path: str):
    """1つのCSV内での中抜き/ドリルの混在を検出する（ファイル単位のNG）。
       NG条件：
         - 中抜き：あり と なし が混在
         - ドリル：あり と なし が混在
         - ドリルあり かつ 中抜きあり が両方存在（機械種に関係なくファイル内混在をNG）
    """
    try:
        # 正規化して空欄判定を頑丈に（全角スペース等も除去）
        nak_series = df["中抜き"].apply(_normalize_zen2han) if "中抜き" in df.columns else pd.Series([], dtype=str)
        drill_series = df["ドリル"].apply(_normalize_zen2han) if "ドリル" in df.columns else pd.Series([], dtype=str)

        has_nak_present = (nak_series != "").any()
        has_nak_absent  = (nak_series == "").any()

        has_drill_present = (drill_series != "").any()
        has_drill_absent  = (drill_series == "").any()

        errors = []

        # 1) 中抜き あり/なし混在
        if has_nak_present and has_nak_absent:
            errors.append((file_path, "ファイル内で『中抜きの有無』が混在しています（あり/なし）。ファイルを分けてください。"))

        # 2) ドリル あり/なし混在
        if has_drill_present and has_drill_absent:
            errors.append((file_path, "ファイル内で『ドリルの有無』が混在しています（あり/なし）。ファイルを分けてください。"))

        # 3) ドリル と 中抜き の混在
        if has_drill_present and has_nak_present:
            errors.append((file_path, "ファイル内で『ドリル』と『中抜き』が混在しています。ファイルを分けてください。"))

        return errors

    except Exception as e:
        # 列欠損など予期せぬ事態はエラーメッセージとして返す（デバッグ用）
        return [(file_path, f"ファイル混在チェックでエラー → {e}")]   

def _extract_sizes_from_row_for_shot_mix(row):
    """機種41〜45向け：ドリル/中抜きからサイズ(mm)集合を抽出。
       - ドリル: 'size@count' をカンマ対応で複数抽出
       - 中抜き: 'φ size @' を優先的に全部抽出。無ければ行内の数値から抽出
       - 誤検知回避のため、10未満の数値は捨てる（個数など）
    """
    sizes = set()

    machine = str(row.get("機種", "")).strip()
    if machine not in {"41", "42", "43", "44", "45"}:
        return sizes  # 対象機種以外は空

    # --- ドリル ---
    drill_val = _normalize_zen2han(row.get("ドリル", ""))
    if drill_val:
        # 全角カンマや読点も区切りとする
        drill_val = drill_val.replace("，", ",").replace("、", ",")
        for token in [t.strip() for t in drill_val.split(",") if t.strip()]:
            left = token.split("@")[0].strip()
            num = pd.to_numeric(left, errors="coerce")
            if pd.notna(num) and float(num) >= 10:
                sizes.add(round(float(num), 3))

    # --- 中抜き ---
    nak_val_raw = row.get("中抜き", "")
    nak_val = _normalize_zen2han(nak_val_raw)
    if nak_val:
        # 1) φ ... @ をすべて拾う
        vals = re.findall(r"φ\s*([0-9]+(?:\.[0-9]+)?)\s*@", nak_val)
        if vals:
            for v in vals:
                f = pd.to_numeric(v, errors="coerce")
                if pd.notna(f) and float(f) >= 10:
                    sizes.add(round(float(f), 3))
        else:
            # 2) バックアップ：行内の数値を全部拾う（個数等の小さい数は除外）
            vals = re.findall(r"([0-9]+(?:\.[0-9]+)?)", nak_val)
            for v in vals:
                f = pd.to_numeric(v, errors="coerce")
                if pd.notna(f) and float(f) >= 10:
                    sizes.add(round(float(f), 3))

    return sizes

def check_shot_mix_15_18(df: pd.DataFrame, file_path: str):
    """機種41〜45：同一CSVに φ15 と φ18以上 が混載していればエラー"""
    try:
        sizes_in_file = set()
        if not df.empty:
            for _, row in df.iterrows():
                sizes_in_file |= _extract_sizes_from_row_for_shot_mix(row)

        has_15    = any(abs(s - 15.0) <= 1e-3 for s in sizes_in_file)
        has_ge_18 = any(s >= 18.0 - EPS      for s in sizes_in_file)

        return [(file_path, "ショット有無15-18チェック：φ15 と φ18以上があります。ショット確認してください。")] \
               if (has_15 and has_ge_18) else []
    except Exception as e:
        return [(file_path, f"ショット有無15-18チェックでエラー → {e}")]


def check_shot_mix_18_22(df: pd.DataFrame, file_path: str):
    """機種41〜45：同一CSVに φ18 と φ22以上 が混載していればエラー"""
    try:
        sizes_in_file = set()
        if not df.empty:
            for _, row in df.iterrows():
                sizes_in_file |= _extract_sizes_from_row_for_shot_mix(row)

        has_18    = any(abs(s - 18.0) <= 1e-3 for s in sizes_in_file)
        has_ge_22 = any(s >= 22.0 - EPS      for s in sizes_in_file)

        return [(file_path, "ショット有無15-18チェック：φ15 と φ18以上があります。ショット確認してください。")] \
                       if (has_18 and has_ge_22) else []
    except Exception as e:
        return [(file_path, f"ショット有無18-22チェックでエラー → {e}")]

# ================== メインチェック処理 ==================
def check_csv(file_path, master_df, check_rules):
    try:
        # --- 読み込み（BOM対策つきにしておくと安全） ---
        try:
            df = pd.read_csv(file_path, encoding="utf-8-sig")
        except UnicodeDecodeError:
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
                    
            # ショット寸法チェック（コメント=33のときの 500x500 超過）
            if check_rules.get("shot_dimension", True):
                err = check_shot_dimension(row, rowno, file_path)
                if err:
                    errors.append(err)
                    
            # ★ 機種×板厚チェック
            if check_rules.get("machine_thickness", True):
                err = check_machine_thickness(row, rowno, file_path)
                if err:
                    errors.append(err)
                                        
            if check_rules.get("file_mix", True):
                errors.extend(check_file_mix(df, file_path))
                
            # ★ ファイル単位の混載チェック（行ループの前に1回だけ）
            if check_rules.get("file_mix", True):
                errors.extend(check_file_mix(df, file_path))

            # ★ 追加：ショット有無の混載チェック（機種41〜45）
            if check_rules.get("shot_mix_15_18", True):
                errors.extend(check_shot_mix_15_18(df, file_path))
            if check_rules.get("shot_mix_18_22", True):
                errors.extend(check_shot_mix_18_22(df, file_path))

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
            "thickness_grade": "板厚x材質チェック",
            "plasma_nakauki": "プラズマ中抜きチェック",
            "dia_dimension": "ダイアSPL寸法切りチェック",
            "sabi_comment": "サビコメントチェック",
            "dimension_limit": "寸法制限チェック",
            "decimal_nakauki": "フィラー中抜きUPチェック",
            "drill_check": "ドリル径チェック",
            "date_check": "日付チェック",
            "shot_dimension": "ショット寸法チェック",
            "machine_thickness": "機種×板厚チェック",
            "file_mix": "ファイル内の中抜き/ドリル混在チェック",
            "shot_mix_15_18": "ショット有無15-18チェック（機種41〜45）",
            "shot_mix_18_22": "ショット有無18-22チェック（機種41〜45）",
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

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
MIN_DRILL_SIZE = 5.0  # ドリル最小径(φ)。これ以下はNG
EPS = 1e-9

# エラータイプ定数（UI表示には未使用だが拡張余地として残置）
ERROR_TYPES = {
    "MISSING_COLUMN": "列不足",
    "INVALID_THICKNESS_GRADE": "板厚材質不適合",
    "PLASMA_NAKAUKI": "プラズマ中抜きエラー",
    "DIA_DIMENSION": "ダイヤ寸法エラー",
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
            "drill_check": True,
            "date_check": True,
            "shot_dimension": True,
            "machine_thickness": True,
            "file_mix": True,
            "drill_work_size": True, 
            "round_drill": True,
            "kakizaki_consistency": True,
        },
        "ignore_shot_when_nak_over38": True,
        "max_workers": 4,
       
        # ウィンドウ関係
        "window_geometry": "",     # 例 "1024x700+200+100"
        "start_maximized": False,  # True なら起動時に最大化
        "min_width": 400,          # 最小幅
        "min_height": 400          # 最小高さ
    }


def save_config(cfg):
    """設定ファイルを保存"""
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"設定保存エラー: {e}")
        
def _parse_sizes_counts_from_row(row):
    """
    その行に含まれる (径mm, 孔数int) のリストを返す。
    - ドリル: 'size@count' をカンマ/スラッシュ区切りで複数可
    - 中抜き: 'φ size @ count' を複数拾う（@count 必須）。区切りはカンマ/スラッシュ可
    全角→半角化と空白除去をした上で数値を読む。
    """
    results = []

    # --- ドリル ---
    drill = _normalize_zen2han(row.get("ドリル", ""))
    if drill:
        # 区切りをカンマに統一（，、／/）
        for ch in ("，", "、", "／", "/"):
            drill = drill.replace(ch, ",")
        for token in [t.strip() for t in drill.split(",") if t.strip()]:
            if "@" not in token:
                continue
            left, right = token.split("@", 1)
            size = pd.to_numeric(left.strip(), errors="coerce")
            cnt  = pd.to_numeric(right.strip(), errors="coerce")
            if pd.notna(size) and pd.notna(cnt):
                results.append((float(size), int(cnt)))

    # --- 中抜き（穴詳細） ---
    nak = _normalize_zen2han(row.get("中抜き", ""))
    if nak:
        # 区切りをカンマに統一（，、／/）
        for ch in ("，", "、", "／", "/"):
            nak = nak.replace(ch, ",")
        # 例: "φ18 @ 2, φ15@1" 等を想定（半角全角正規化済み）
        # ※ 'φ' の直後や '@' 周りのスペースは可
        for token in [t.strip() for t in nak.split(",") if t.strip()]:
            m = re.search(r"φ\s*([0-9]+(?:\.[0-9]+)?)\s*@\s*([0-9]+)", token)
            if not m:
                continue
            size = pd.to_numeric(m.group(1), errors="coerce")
            cnt  = pd.to_numeric(m.group(2), errors="coerce")
            if pd.notna(size) and pd.notna(cnt):
                results.append((float(size), int(cnt)))

    return results

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
    """ダイヤ・スプライス機種の寸法チェック"""
    try:
        machine = str(row["機種"]).strip()
        dimension = str(row["寸法・型切"]).strip()
        if machine in MACHINE_DIA_SPLICE and dimension != "1":
            return (file_path, f"{rowno}行目: ダイヤ・スプライスが寸法でない → 機種:{machine}, 寸法・型切:{dimension}")
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
    """寸法制限チェック
    - 幅または長さが6000を超えたらNG
    - 板厚が4.5以下の場合は板の標準サイズ制限あり
        ・1200x2400を超える → 警告「製品寸法が4x8より大きいです。5x10はありますか？」
        ・1500x3000を超える → 警告「製品寸法が5x10を超えています。要確認」
    """
    errors = []
    try:
        width = float(row.get("幅", 0))
        length = float(row.get("長さ", 0))
        thickness = float(row.get("板厚", 0))
    except Exception:
        return errors  # ← 一貫してリストを返す

    # --- 既存ルール: 6000超え ---
    if width > 6000 or length > 6000:
        errors.append((file_path, f"{rowno}行目: 幅{width}または長さ{length}が6000を超えています。"))

    # --- 新ルール: 板厚4.5以下の制限 ---
    if thickness <= 4.5:
        # まず 5x10 超え（1500x3000）を優先して判定
        if width > 1500 or length > 3000:
            errors.append((file_path, f"{rowno}行目: 製品寸法が5x10を超えています。要確認"))
        # 次に 4x8 超え（1200x2400）
        elif width > 1200 or length > 2400:
            errors.append((file_path, f"{rowno}行目: 製品寸法が4x8より大きいです。5x10はありますか？"))

    return errors

def check_drill_format_and_size(row, rowno, file_path):
    """ドリルの形式とサイズチェック（カンマ/スラッシュ区切り対応）
       - フォーマット: size@count 必須
       - φ5 以下も NG（MIN_DRILL_SIZE で閾値調整可）
       - MAX_DRILL_SIZE 以上も NG
    """
    try:
        drill_raw = row.get("ドリル", "")
        if pd.isna(drill_raw):
            return None

        drill_val = _normalize_zen2han(drill_raw)
        if not drill_val:
            return None  # 空欄はOK

        # 区切り統一
        for ch in ("，", "、", "／", "/"):
            drill_val = drill_val.replace(ch, ",")

        tokens = [t.strip() for t in drill_val.split(",") if t.strip()]
        if not tokens:
            return None

        for token in tokens:
            if "@" not in token:
                return (file_path, f"{rowno}行目: ドリルの形式が不正（@なし） → {token}")

            left, right = token.split("@", 1)
            if not left.strip() or not right.strip():
                return (file_path, f"{rowno}行目: ドリルの形式が不正（サイズまたは個数が空） → {token}")

            # サイズ/個数の数値性チェック
            try:
                size = float(left.strip())
            except ValueError:
                return (file_path, f"{rowno}行目: ドリルのサイズが数値でない → {token}")

            try:
                cnt = int(float(right.strip()))  # "1.0" も許容
            except ValueError:
                return (file_path, f"{rowno}行目: ドリルの個数が数値でない → {token}")

            # ★ 追加: φ5 以下はNG（==5も含む）
            if size <= MIN_DRILL_SIZE:
                return (file_path, f"{rowno}行目: ドリルサイズがφ{MIN_DRILL_SIZE:g}以下 → {token}")

            # 既存：上限チェック
            if size >= MAX_DRILL_SIZE:
                return (file_path, f"{rowno}行目: ドリルサイズが{MAX_DRILL_SIZE}以上 → {token}")

    except Exception as e:
        return (file_path, f"{rowno}行目: ドリルチェックで予期せぬエラー → {e}")
    return None

def check_drill_mix(row, rowno, file_path):
    """機種54のドリル混在チェック（カンマ/スラッシュ区切り対応）"""
    try:
        machine = str(row.get("機種", "")).strip()
        if machine == MACHINE_DRILL_CHECK:
            drill_raw = row.get("ドリル", "")
            if pd.isna(drill_raw):
                return None
            drill_val = _normalize_zen2han(drill_raw)
            for ch in ("，", "、", "／", "/"):
                drill_val = drill_val.replace(ch, ",")
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
    """ショット寸法チェック：
       コメント=33 かつ 板厚>=16 のとき、幅・長さの両方が500を超えたらNG。
       例）600x400 → OK、495x700 → OK、510x510 → NG
    """
    try:
        comment = str(row["コメント"]).strip()
        if comment != "33":
            return None  # コメント33以外は対象外

        # 板厚 >= 16（誤差EPS考慮）でなければ対象外
        t = pd.to_numeric(row.get("板厚", ""), errors="coerce")
        if pd.isna(t):
            return None  # 板厚不明はここではスルー（別チェックで拾う）
        thickness = float(t)
        if thickness < 16 - EPS:
            return None

        width = float(row["幅"])
        length = float(row["長さ"])

        if width > MAX_BOTH_DIMENSION and length > MAX_BOTH_DIMENSION:
            return (file_path,
                    f"{rowno}行目: ショット品（板厚:{thickness}）の寸法が{MAX_BOTH_DIMENSION}×{MAX_BOTH_DIMENSION}を超過 → 幅:{width}, 長さ:{length}（コメント:{comment}）")
    except (ValueError, KeyError):
        # 幅/長さ/板厚が数値でない・列が無い等はスルー（他の必須列チェックで拾われる）
        pass
    return None

def check_machine_thickness(row, rowno, file_path):
    """機種と板厚の組み合わせ制約をチェック（≧／≦は「含む」）
       40: 厚さ16以上
       41: 6以上かつ40まで
       42,44,45: 12まで
       52,53: 4.5まで
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

        # 52,53: 〜4.5
        elif machine in ("52", "53"):
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
    
def check_shot_comment_rule(row, rowno, file_path, abc_rule):
    """
    A/B/C 入力に基づく行別チェック（機種 40/41/42/43/44/45 のみ判定）。
    ・C判定は「穴個数の合計」で評価（径は問わない）
    ・合計がC未満の場合のみ、A(ショット無し想定径)での33不要判定を行う
    ・設定で『穴詳細がφ38以上の行はショット判定しない』を適用可能
    """
    # 対象機種のみ
    try:
        machine = str(row.get("機種", "")).strip()
    except Exception:
        machine = ""
    if machine not in {"40", "41", "42", "43", "44", "45"}:
        return None

    # A,B,C 取得（Bは互換のため受け取るが合計判定では未使用）
    try:
        a = float(abc_rule.get("a"))
        _b_unused = float(abc_rule.get("b"))
        c = int(abc_rule.get("c"))
    except Exception:
        return None

    # φ38以上が含まれる行はスキップ（オプション）
    if abc_rule.get("ignore_nak_over_38", False):
        if _nak_has_size_over_equal(row, threshold=38.0):
            return None

    comment33 = str(row.get("コメント", "")).strip() == "33"
    pairs = _parse_sizes_counts_from_row(row)  # [(径, 孔数), ...]
    if not pairs:
        return None

    # ★ ここを変更：C判定は穴個数の「合計」で評価
    total_cnt = sum(int(cnt) for dia, cnt in pairs)

    msgs = []
    if total_cnt >= c:
        # 合計がC以上なら 33 必須
        if not comment33:
            msgs.append(f"孔数合計{total_cnt}が{c}以上です。コメント33を入れてください。")
    else:
        # 合計がC未満なら 33 不要
        if comment33:
            msgs.append(f"孔数合計{total_cnt}が{c}未満のためコメント33は不要です。")

        # さらに A(ショット無し想定径) が含まれる場合の補足（任意だが従来ロジックを踏襲）
        if any(abs(dia - a) <= 1e-3 for dia, _ in pairs):
            if comment33:
                # すでに「33不要」メッセージが入っている可能性が高いが、より具体的に補足
                msgs.append(f"φ{a:g} はショット無し想定のためコメント33は不要です。")

    if msgs:
        return (file_path, f"{rowno}行目: " + " / ".join(msgs))
    return None

    
def _nak_has_size_over_equal(row, threshold=38.0):
    """穴詳細（中抜き）に threshold 以上の径が含まれるか判定"""
    try:
        nak = _normalize_zen2han(row.get("中抜き", ""))
        if not nak:
            return False
        for ch in ("，", "、", "／", "/"):
            nak = nak.replace(ch, ",")
        # 例: "φ18 @ 2, φ40@1"
        for token in [t.strip() for t in nak.split(",") if t.strip()]:
            m = re.search(r"φ\s*([0-9]+(?:\.[0-9]+)?)\s*@\s*([0-9]+)", token)
            if not m:
                continue
            size = pd.to_numeric(m.group(1), errors="coerce")
            if pd.notna(size) and float(size) >= threshold - EPS:
                return True
        return False
    except Exception:
        return False
    
def check_drill_work_size(row, rowno, file_path):
    """
    製品寸法が1501×1501以上 かつ ドリル孔がある場合はNG
    - ドリルが空欄（空文字/空白/NaN）ならOK（判定しない）
    """
    # ドリル欄の空判定（全角→半角・空白除去で頑丈に）
    drill_raw = row.get("ドリル", "")
    if pd.isna(drill_raw):
        return None
    drill = _normalize_zen2han(drill_raw)
    if drill == "":
        return None  # 空欄ならOK

    # 寸法チェック
    try:
        width = float(row.get("幅", 0) or 0)
        length = float(row.get("長さ", 0) or 0)
    except Exception:
        return None

    if width >= 1501 and length >= 1501:
        return (file_path, f"{rowno}行目: 製品寸法 {width:g}×{length:g} でドリル孔あり → NG")
    return None

def check_round_drill(row, rowno, file_path):
    TOL = 1.0
    PI  = 3.14

    try:
        width  = float(row.get("幅", 0) or 0)
        length = float(row.get("長さ", 0) or 0)
        cutlen = float(row.get("切断長", 0) or 0)
    except Exception:
        return None
    if width <= 0 or length <= 0 or cutlen <= 0:
        return None

    # --- 穴詳細（径@個数）を処理 ---
    nak_raw = _normalize_zen2han(row.get("中抜き", ""))
    if nak_raw:
        total_sub = 0.0
        for token in [t.strip() for t in nak_raw.split("/") if t.strip()]:
            m = re.match(r"(?:φ)?([0-9]+(?:\.[0-9]+)?)@([0-9]+)", token)
            if m:
                dia = float(m.group(1))
                cnt = int(m.group(2))
                total_sub += dia * PI * cnt
        cutlen -= total_sub
        if cutlen <= 0:
            return None

    # --- 丸切り判定 ---
    dia_est = cutlen / PI
    if not (abs(width - length) <= TOL and abs(width - dia_est) <= TOL and abs(length - dia_est) <= TOL):
        return None

    # --- ドリル有無 ---
    drill = _normalize_zen2han(row.get("ドリル", ""))
    if drill == "":
        return None

    # --- ケガキ詳細・穴詳細のチェック ---
    kegaki = _normalize_zen2han(row.get("ケガキ詳細", ""))
    nak    = _normalize_zen2han(row.get("中抜き", ""))

    if kegaki != "" or nak != "":
        return (
            file_path,
            f"{rowno}行目: 丸切りでドリルあり。ケガキor中抜きがあるようなので確認してください "
        )
    return None

def check_kakizaki_consistency(df: pd.DataFrame, file_path: str):
    """
    開先K詳細 / 開先V詳細 の有無がファイル内で統一されているかをチェック。
    許容パターン（ファイル内が1種類のみ）:
      - 全行 Kのみ
      - 全行 Vのみ
      - 全行 KもVもある
      - 全行 KもVもない
    上記以外＝行によって混在 → NG
    """
    try:
        if ("開先K詳細" not in df.columns) and ("開先V詳細" not in df.columns):
            return []  # 対象列が無ければスキップ

        # 正規化して空判定
        k_series = df["開先K詳細"].apply(_normalize_zen2han) if "開先K詳細" in df.columns else pd.Series([""] * len(df))
        v_series = df["開先V詳細"].apply(_normalize_zen2han) if "開先V詳細" in df.columns else pd.Series([""] * len(df))

        states = []
        for k_val, v_val in zip(k_series, v_series):
            k_on = (k_val != "")
            v_on = (v_val != "")
            states.append((k_on, v_on))

        unique_states = set(states)
        if len(unique_states) <= 1:
            return []  # 統一されている → OK

        # NG：混在しているので内訳をわかるようにメッセージ化
        from collections import Counter
        cnt = Counter(states)
        label_map = {
            (True,  False): "Kのみ",
            (False, True):  "Vのみ",
            (True,  True):  "K+Vあり",
            (False, False): "K/Vなし",
        }
        detail = " / ".join(f"{label_map[s]}:{n}行" for s, n in cnt.items())
        return [(file_path, f"開先K/Vがファイル内で混在しています → {detail}。ファイルを分けるか、入力を統一してください。")]

    except Exception as e:
        return [(file_path, f"開先整合性チェックでエラー → {e}")]


# ================== メインチェック処理 ==================
def check_csv(file_path, master_df, check_rules, abc_rule):
    try:
        # --- 読み込み（BOM対策つき） ---
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
            "寸法・型切": "寸法・型切",
        }
        df.rename(columns=rename_map, inplace=True)

        # 必須列チェック
        required_cols = ["板厚", "材質", "幅", "長さ", "コメント",
                         "ドリル", "中抜き", "機種", "寸法・型切",
                         "サビ", "Y", "Z"]
        errors = []
        for col in required_cols:
            if col not in df.columns:
                errors.append((file_path, f"必須列が存在しません: '{col}'"))
                return errors

        # 行ごとのチェック
        for idx, row in df.iterrows():
            rowno = idx + 2  # 見出し1行の次が2行目

            # 板厚×材質（在庫）チェック
            if check_rules.get("thickness_grade", True):
                if not is_valid_thickness_and_grade(row, master_df):
                    errors.append((file_path,
                                   f"{rowno}行目: 板厚と材質の組合せが無効 → 板厚:{row.get('板厚', 'N/A')}, 材質:{row.get('材質', 'N/A')}"))

            # プラズマ中抜き
            if check_rules.get("plasma_nakauki", True):
                err = check_plasma_nakauki(row, rowno, file_path)
                if err:
                    errors.append(err)

            # ダイヤ・スプライスの寸法判定
            if check_rules.get("dia_dimension", True):
                err = check_dia_dimension(row, rowno, file_path)
                if err:
                    errors.append(err)

            # サビ×コメント
            if check_rules.get("sabi_comment", True):
                err = check_sabi_comment(row, rowno, file_path)
                if err:
                    errors.append(err)

            # 寸法制限
            if check_rules.get("dimension_limit", True):
                dim_errors = check_dimensions(row, rowno, file_path)
                errors.extend(dim_errors)

            # ドリル形式・サイズ／混在（54）
            if check_rules.get("drill_check", True):
                err = check_drill_format_and_size(row, rowno, file_path)
                if err:
                    errors.append(err)
                err = check_drill_mix(row, rowno, file_path)
                if err:
                    errors.append(err)

            # 日付（Y≦today≦Z）
            if check_rules.get("date_check", True):
                err = check_dates(row, rowno, file_path)
                if err:
                    errors.append(err)

            # ショット寸法（コメント=33 かつ t>=16 の 500×500超過）
            if check_rules.get("shot_dimension", True):
                err = check_shot_dimension(row, rowno, file_path)
                if err:
                    errors.append(err)

            # 機種×板厚（52/53 は 4.5まで 等）
            if check_rules.get("machine_thickness", True):
                err = check_machine_thickness(row, rowno, file_path)
                if err:
                    errors.append(err)

            # ★ A/B/C 入力に基づく コメント33 ルール
            if check_rules.get("enable_shot_rule", True): 
                err = check_shot_comment_rule(row, rowno, file_path, abc_rule)
                if err:
                    errors.append(err)
                    
            # ドリルワークサイズチェック（1501x1501以上でドリル孔あり）
            if check_rules.get("drill_work_size", True):
                err = check_drill_work_size(row, rowno, file_path)
                if err:
                    errors.append(err)

            # 丸切りドリル判定
            if check_rules.get("round_drill", True):
                err = check_round_drill(row, rowno, file_path)
                if err:
                    errors.append(err)

        # ファイル単位の混在（中抜き/ドリル）
        if check_rules.get("file_mix", True):
            errors.extend(check_file_mix(df, file_path))
            
        # ファイル単位の混在（中抜き/ドリル）
        if check_rules.get("file_mix", True):
            errors.extend(check_file_mix(df, file_path))

        # ★ 開先K/Vの整合性（ファイル単位）
        if check_rules.get("kakizaki_consistency", True):
            errors.extend(check_kakizaki_consistency(df, file_path))


        return errors

    except Exception as e:
        return [(file_path, f"ファイル処理エラー: {e}")]
   
def process_file(args):
    file_path, master_df, check_rules, abc_rule = args
    return check_csv(file_path, master_df, check_rules, abc_rule)

# ================== GUI本体 ==================
class CSVCheckerApp:
    def __init__(self, master):
        self.master = master
        self.master.title("CSVチェックツール ver4.7")
        self.config = load_config()
        
        # ★ 追加：ウィンドウ復元（最大化／前回位置／中央寄せ）
        self._restore_window()

        # ★ 追加：閉じるときにウィンドウ状態を保存
        self.master.protocol("WM_DELETE_WINDOW", self._on_close)
        self.is_running = False

        self.folder_path = tk.StringVar(value=self.config.get("last_folder", ""))
        self.progress_var = tk.DoubleVar()
        self.status_var = tk.StringVar(value="準備完了")

        # ★ 追加：表示モード状態と結果保持用
        self.display_mode = tk.StringVar(value="全件まとめ")  # 「全件まとめ」or「ファイル別」
        self.all_errors = []           # [(file, message), ...]
        self.errors_by_file = {}       # {filename(str): [message, ...]}

        self.create_widgets()
        
    def _center_window(self, w, h):
        """与えられたサイズで画面中央に配置"""
        self.master.update_idletasks()
        sw = self.master.winfo_screenwidth()
        sh = self.master.winfo_screenheight()
        x = max((sw - w) // 2, 0)
        y = max((sh - h) // 2, 0)
        self.master.geometry(f"{w}x{h}+{x}+{y}")

    def _restore_window(self):
        """最大化設定 or 前回ジオメトリ復元。無ければ中央寄せ"""
        try:
            if self.config.get("start_maximized", False):
                self.master.state('zoomed')  # Windows で最大化
            else:
                geom = self.config.get("window_geometry", "")
                if geom:
                    self.master.geometry(geom)
                else:
                    w = self.config.get("min_width", 0)
                    h = self.config.get("min_height", 0)
                    self._center_window(w, h)
        except Exception:
            # 何かあっても安全側で中央寄せ
            self._center_window(self.config.get("min_width", 0),
                                self.config.get("min_height", 0))

    def _on_close(self):
        """閉じる前にウィンドウ状態を保存"""
        try:
            state = self.master.state()
            self.config["start_maximized"] = (state == "zoomed")
            if not self.config["start_maximized"]:
                self.config["window_geometry"] = self.master.geometry()
            save_config(self.config)
        finally:
            self.master.destroy()

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
        # ← ここから：5列レイアウト。shot_mix_patterns はここには出さない
        rule_names = {
            "thickness_grade": "板厚x材質チェック",
            "plasma_nakauki": "プラズマ中抜きチェック",
            "dia_dimension": "ダイヤSPL寸法切りチェック",
            "sabi_comment": "サビコメントチェック",
            "dimension_limit": "寸法制限チェック",
            "drill_check": "ドリル径チェック",
            "date_check": "日付チェック",
            "shot_dimension": "ショット寸法チェック",
            "machine_thickness": "機種×板厚チェック",
            "file_mix": "ファイル内の中抜き/ドリル混在チェック",
            "drill_work_size": "ドリルワークサイズ",
            "round_drill": "丸切りドリル判定",
            "kakizaki_consistency": "開先整合性（K/V）チェック",
        }
        
        row, col = 0, 0
        for key, name in rule_names.items():
            var = tk.BooleanVar(value=rules.get(key, True))
            self.check_vars[key] = var
            tk.Checkbutton(settings_frame, text=name, variable=var).grid(row=row, column=col, sticky="w", padx=5, pady=2)
            col += 1
            if col > 4:  # ★ 5列
                col = 0
                row += 1

        # === ショット判定（A/B/C） 入力欄：実行ボタンの「上」 ===
        abc_frame = tk.LabelFrame(self.master, text="ショット判定（A/B/C）")
        abc_frame.grid(row=3, column=0, columnspan=3, sticky="ew", padx=5, pady=(4, 2))
        
        # ON/OFF チェックボックス（Aの前）
        self.enable_shot_rule = tk.BooleanVar(value=self.config.get("enable_shot_rule", True))
        cb = tk.Checkbutton(abc_frame, variable=self.enable_shot_rule)  # command は後で設定
        cb.grid(row=0, column=0, sticky="w", padx=(2, 5))

        # A/B/C の変数（Entryより先に作成）
        self.shot_no_dia_var = tk.StringVar(value=str(self.config.get("shot_abc_rule", {}).get("a", "15")))
        self.shot_yes_dia_var = tk.StringVar(value=str(self.config.get("shot_abc_rule", {}).get("b", "18")))
        self.shot_count_var  = tk.StringVar(value=str(self.config.get("shot_abc_rule", {}).get("c", "2")))

        # A
        tk.Label(abc_frame, text="ショット無し (A) φ").grid(row=0, column=1, sticky="e")
        a_entry = tk.Entry(abc_frame, textvariable=self.shot_no_dia_var, width=8)
        a_entry.grid(row=0, column=2, padx=(2, 80), sticky="w")

        # B
        tk.Label(abc_frame, text="ショット有り (B) φ").grid(row=0, column=3, sticky="e")
        b_entry = tk.Entry(abc_frame, textvariable=self.shot_yes_dia_var, width=8)
        b_entry.grid(row=0, column=4, padx=(2, 12), sticky="w")

        # C
        tk.Label(abc_frame, text="孔個数以上 (C)").grid(row=0, column=5, sticky="e")
        c_entry = tk.Entry(abc_frame, textvariable=self.shot_count_var, width=8)
        c_entry.grid(row=0, column=6, padx=(2, 12), sticky="w")
        
        # ★ 追加：穴詳細38φ以上はショット判定しないチェック
        self.ignore_nak_over38_var = tk.BooleanVar(
            value=self.config.get("ignore_shot_when_nak_over38", True)
        )

        chk_ignore_nak38 = tk.Checkbutton(
            abc_frame,
            text="中抜きショットなし(38φ以上)",
            variable=self.ignore_nak_over38_var
        )
        chk_ignore_nak38.grid(row=0, column=7, sticky="w", padx=(16, 2))


        # OFF時にA/B/Cをグレーアウト
        def _toggle_shot_fields():
            state = ("normal" if self.enable_shot_rule.get() else "disabled")
            for w in (a_entry, b_entry, c_entry, chk_ignore_nak38):  # ← ここに追加
                w.config(state=state)


        cb.config(command=_toggle_shot_fields)
        _toggle_shot_fields()  # 初期反映

        # 実行フレーム（重なり防止のため row を 4 に）
        exec_frame = tk.Frame(self.master)
        exec_frame.grid(row=4, column=0, columnspan=3, pady=10, padx=5)
        exec_frame.grid_columnconfigure(0, weight=1)
        exec_frame.grid_columnconfigure(1, weight=0)
        exec_frame.grid_columnconfigure(2, weight=0)
        exec_frame.grid_columnconfigure(3, weight=1)
        
        self.run_button = tk.Button(exec_frame, text="実行", command=self.run_check, bg="lightgreen", width=10)
        self.run_button.grid(row=0, column=1, padx=5)
        
        self.stop_button = tk.Button(exec_frame, text="停止", command=self.stop_check, bg="lightcoral", state="disabled", width=10)
        self.stop_button.grid(row=0, column=2, padx=5)

        # プログレスバー（行を 5 にずらす）
        tk.Label(self.master, text="進捗:").grid(row=5, column=0, sticky="w", padx=5)
        self.progress_bar = ttk.Progressbar(self.master, variable=self.progress_var, maximum=100)
        self.progress_bar.grid(row=5, column=1, columnspan=2, sticky="ew", padx=5)

        # ステータス（行を 6 にずらす）
        tk.Label(self.master, textvariable=self.status_var).grid(row=6, column=0, columnspan=3, sticky="w", padx=5)

        # 表示モード切替（行を 7 にずらす）
        mode_frame = tk.Frame(self.master)
        mode_frame.grid(row=7, column=0, columnspan=3, sticky="w", padx=5, pady=(4,0))
        tk.Label(mode_frame, text="表示モード:").pack(side="left")
        tk.Radiobutton(mode_frame, text="全件まとめ", value="全件まとめ", variable=self.display_mode,
                    command=self.render_results).pack(side="left", padx=4)
        tk.Radiobutton(mode_frame, text="ファイル別", value="ファイル別", variable=self.display_mode,
                    command=self.render_results).pack(side="left", padx=4)

        # 結果表示（行を 8 に）
        tk.Label(self.master, text="チェック結果:").grid(row=8, column=0, sticky="nw", padx=5)

        tree_frame = tk.Frame(self.master)
        tree_frame.grid(row=9, column=0, columnspan=3, sticky="nsew", padx=5, pady=5)
        
        self.tree = ttk.Treeview(tree_frame, columns=("ファイル", "内容"), show="headings")
        self.tree.heading("ファイル", text="ファイル名")
        self.tree.heading("内容", text="違反内容")
        self.tree.column("ファイル", width=220)
        self.tree.column("内容", width=580)
        self.tree.column("#0", width=220, stretch=True)

        v_scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        h_scrollbar = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        self.tree.grid(row=0, column=0, sticky="nsew")
        v_scrollbar.grid(row=0, column=1, sticky="ns")
        h_scrollbar.grid(row=1, column=0, sticky="ew")
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        # グリッド設定
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        self.master.grid_columnconfigure(0, weight=0)  # ← ラベル列は伸ばさない
        self.master.grid_columnconfigure(1, weight=1)  # ← ここが主に伸びる
        self.master.grid_columnconfigure(2, weight=1)  # ← ここも伸びる
        self.master.grid_rowconfigure(9, weight=1)

        
    def _read_abc_rule(self):
        """UIのA/B/C入力を読み取り・検証して返す"""
        try:
            a = float(self.shot_no_dia_var.get())
            b = float(self.shot_yes_dia_var.get())
            c = int(float(self.shot_count_var.get()))  # "2.0" 等も許容してint化
        except Exception:
            raise ValueError("A,Bは数値、Cは整数で入力してください。")
        if a <= 0 or b <= 0 or c <= 0:
            raise ValueError("A/B/Cはいずれも正の値にしてください。")
        if not (a < b):
            raise ValueError("A < B を満たしてください。")
        return {"a": a, "b": b, "c": c}


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
        if self.is_running:
            return

        folder = self.folder_path.get()
        if not os.path.isdir(folder):
            messagebox.showerror("エラー", "フォルダが正しく選択されていません")
            return

        # A/B/C を読み取り（バリデーション）
        try:
            abc_rule = self._read_abc_rule()
        except Exception as e:
            messagebox.showerror("入力エラー", f"ショット判定（A/B/C）の入力が不正です:\n{e}")
            return
        
        abc_rule["ignore_nak_over_38"] = self.ignore_nak_over38_var.get()

        # マスタ確認
        script_dir = os.path.dirname(sys.argv[0])
        master_path = os.path.join(script_dir, MASTER_FILENAME)
        if not os.path.isfile(master_path):
            messagebox.showerror("エラー", f"マスタファイルが見つかりません:\n{master_path}")
            return

        # 設定保存
        self.config["last_folder"] = folder
        self.config["check_rules"] = {key: var.get() for key, var in self.check_vars.items()}
        self.config["shot_abc_rule"] = abc_rule
        self.config["enable_shot_rule"] = self.enable_shot_rule.get()   # ← 追加
        self.config["ignore_shot_when_nak_over38"] = self.ignore_nak_over38_var.get()
        save_config(self.config)

        # 実行準備
        self.run_button.config(state="disabled")
        self.stop_button.config(state="normal")
        self.is_running = True
        self.tree.delete(*self.tree.get_children())
        self.all_errors = []
        self.errors_by_file = {}

        thread = threading.Thread(target=self._run_check_thread, args=(folder, master_path, abc_rule))
        thread.daemon = True
        thread.start()



    def _run_check_thread(self, folder, master_path, abc_rule):
        try:
            self.status_var.set("マスタファイル読み込み中...")
            master_df = load_master(master_path)

            csv_files = [os.path.join(folder, f) for f in os.listdir(folder)
                        if f.lower().endswith(".csv") and not re.match(r"^\d{3}", f)]
            if not csv_files:
                self.status_var.set("処理対象のCSVファイルが見つかりません")
                self._reset_ui()
                return

            self.status_var.set(f"{len(csv_files)}個のファイルを処理中...")
            check_rules = {key: var.get() for key, var in self.check_vars.items()}
            check_rules["enable_shot_rule"] = self.enable_shot_rule.get()

            all_errors = []
            with ThreadPoolExecutor(max_workers=self.config.get("max_workers", 4)) as executor:
                future_to_file = {
                    executor.submit(process_file, (fp, master_df, check_rules, abc_rule)): fp
                    for fp in csv_files
                }
                completed = 0
                for future in as_completed(future_to_file):
                    if not self.is_running:
                        break
                    fp = future_to_file[future]
                    try:
                        all_errors.extend(future.result())
                    except Exception as e:
                        all_errors.append((fp, f"処理エラー: {e}"))
                    completed += 1
                    self.progress_var.set((completed/len(csv_files))*100)
                    self.status_var.set(f"処理中... ({completed}/{len(csv_files)})")

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
        # いったんクリア
        for i in self.tree.get_children():
            self.tree.delete(i)

        if not self.all_errors:
            return

        mode = self.display_mode.get()

        if mode == "全件まとめ":
            # 列を2本に再構成（ファイル名 / 違反内容）
            self.tree.configure(show="headings", columns=("ファイル", "内容"))
            self.tree.heading("ファイル", text="ファイル名")
            self.tree.heading("内容", text="違反内容")
            self.tree.column("ファイル", width=220, stretch=False)
            self.tree.column("内容", width=580, stretch=True)

            # データ投入
            for fp, msg in self.all_errors:
                self.tree.insert("", "end", values=(os.path.basename(fp), msg))

        else:  # ファイル別
            # ツリー＋見出し、列は「内容」1本のみ
            self.tree.configure(show="tree headings", columns=("内容",))
            self.tree.heading("#0", text="ファイル")   # ツリー列の見出し
            self.tree.heading("内容", text="違反内容")
            self.tree.column("#0", width=280, stretch=False)
            self.tree.column("内容", width=600, stretch=True)

            # ファイルごとに親ノード作成 → 子に違反内容
            for fname in sorted(self.errors_by_file.keys()):
                msgs = self.errors_by_file[fname]
                parent = self.tree.insert("", "end", text=f"{fname}（{len(msgs)}件）", values=("",))
                for m in msgs:
                    self.tree.insert(parent, "end", text="", values=(m,))
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

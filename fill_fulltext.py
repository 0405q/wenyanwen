import json
import sqlite3
from pathlib import Path
from typing import Optional

DB_PATH = "resource_library.sqlite3"          # 你的数据库文件名
DATASET_ROOT = Path("./chinese-poetry")       # 数据集所在目录（请确认路径正确）

def find_full_text(title: str, author: str, category: str) -> Optional[str]:
    if category == "tang_poems":
        search_dirs = ["唐诗", "御定全唐詩"]
    elif category == "song_ci":
        search_dirs = ["宋词"]
    else:
        return None

    for dir_name in search_dirs:
        search_dir = DATASET_ROOT / dir_name
        if not search_dir.exists():
            continue
        for json_file in search_dir.glob("*.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, list):
                    continue
                for item in data:
                    item_title = item.get("title", "") or item.get("rhythmic", "")
                    item_author = item.get("author", "")
                    if item_title == title and item_author == author:
                        paras = item.get("paragraphs", [])
                        if paras:
                            return "\n".join(paras)
            except Exception:
                continue
    return None

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT id, category_slug, title, author FROM library_items "
        "WHERE (full_text IS NULL OR full_text = '') "
        "AND category_slug IN ('tang_poems', 'song_ci')"
    )
    rows = cur.fetchall()
    print(f"找到 {len(rows)} 条缺少全文的记录")
    updated = 0
    not_found = 0
    for row in rows:
        full = find_full_text(row["title"], row["author"], row["category_slug"])
        if full:
            conn.execute("UPDATE library_items SET full_text = ? WHERE id = ?", (full, row["id"]))
            updated += 1
            print(f"✅ 已更新：{row['title']} - {row['author']}")
        else:
            not_found += 1
            print(f"❌ 未找到：{row['title']} - {row['author']}")
    conn.commit()
    conn.close()
    print(f"\n完成：成功更新 {updated} 条，未找到 {not_found} 条")

if __name__ == "__main__":
    main()
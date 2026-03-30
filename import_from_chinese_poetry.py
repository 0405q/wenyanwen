import argparse
import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from resource_library_db import DEFAULT_DB_PATH, create_tables


# 宋词风格（可继续扩充）
SONG_CI_STYLE_BY_AUTHOR: Dict[str, str] = {
    "苏轼": "豪放派",
    "辛弃疾": "豪放派",
    "陆游": "豪放派",
    "岳飞": "豪放派",
    "李清照": "婉约派",
    "柳永": "婉约派",
    "周邦彦": "婉约派",
    "秦观": "婉约派",
    "晏殊": "婉约派",
    "欧阳修": "婉约派",
    "姜夔": "婉约派",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从 chinese-poetry JSON 导入资源库数据（SQLite）。"
    )
    parser.add_argument(
        "--dataset-root",
        default="./data/chinese-poetry",
        help="chinese-poetry 数据集根目录（包含 唐诗/宋词 等子目录）。",
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help=f"SQLite 路径，默认：{DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--mode",
        choices=["append", "replace"],
        default="append",
        help="append=补充导入，replace=先清理指定分类后重建。",
    )
    parser.add_argument(
        "--include-others",
        action="store_true",
        help="兼容保留参数：当前版本仅导入唐诗/宋词，其他体裁会被忽略。",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="分批提交条数，默认 500。",
    )
    return parser.parse_args()


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path,timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAl;")
    return conn


def ensure_categories(conn: sqlite3.Connection) -> None:
    categories = [
        ("tang_poems", "唐诗三百首", "来自 chinese-poetry 数据集的唐诗导入数据。", 10),
        ("song_ci", "宋词精选", "来自 chinese-poetry 数据集的宋词导入数据。", 20),
    ]
    for slug, name, desc, order in categories:
        conn.execute(
            """
            INSERT INTO library_categories (slug, name, description, sort_order)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(slug) DO UPDATE SET
                name=excluded.name,
                description=excluded.description,
                sort_order=excluded.sort_order;
            """,
            (slug, name, desc, order),
        )


def reset_target_data(conn: sqlite3.Connection, include_others: bool) -> None:
    categories = ["tang_poems", "song_ci"]

    placeholders = ",".join(["?"] * len(categories))
    # 先删关联，再删条目，最后删仅属于这些分类的标签
    conn.execute(
        f"""
        DELETE FROM library_item_tags
        WHERE item_id IN (
            SELECT id FROM library_items WHERE category_slug IN ({placeholders})
        );
        """,
        categories,
    )
    conn.execute(
        f"DELETE FROM library_items WHERE category_slug IN ({placeholders});",
        categories,
    )
    conn.execute(
        f"DELETE FROM library_tags WHERE category_slug IN ({placeholders});",
        categories,
    )


def discover_json_files(dataset_root: Path, include_others: bool) -> List[Path]:
    all_files = list(dataset_root.rglob("*.json"))
    selected: List[Path] = []
    for fp in all_files:
        p = str(fp).replace("\\", "/")
        p_lower = p.lower()
        file_name = fp.name.lower()
        if (
            "/唐诗/" in p
            or "/宋词/" in p
            or "/御定全唐詩/" in p
            or "poet.tang" in file_name
            or "poet.tang" in p_lower
            or "quantangshi" in p_lower
            or "ci.song" in file_name
            or "ci.song" in p_lower
        ):
            selected.append(fp)
            continue
        # 其他体裁当前版本不导入
    return sorted(selected)


def infer_bucket(path: Path) -> Tuple[str, str, str]:
    """
    返回 (category_slug, dynasty, item_type)
    """
    p = str(path).replace("\\", "/")
    p_lower = p.lower()
    file_name = path.name.lower()

    # 唐诗：目录命中 + 常见文件命名 + 全唐诗目录
    if (
        "/唐诗/" in p
        or "/御定全唐詩/" in p
        or "poet.tang" in file_name
        or "poet.tang" in p_lower
        or "quantangshi" in p_lower
    ):
        return ("tang_poems", "唐", "poem")

    # 宋词：目录命中 + 常见词集文件命名
    if (
        "/宋词/" in p
        or "ci.song" in file_name
        or "ci.song" in p_lower
    ):
        return ("song_ci", "宋", "ci")

    # 其他体裁当前不导入
    return ("", "", "")


def normalize_lines(paragraphs: object) -> List[str]:
    if paragraphs is None:
        return []
    if isinstance(paragraphs, list):
        return [str(x).strip() for x in paragraphs if str(x).strip()]
    if isinstance(paragraphs, str):
        lines = [x.strip() for x in paragraphs.split("\n")]
        return [x for x in lines if x]
    return []


def build_title(obj: Dict, item_type: str) -> str:
    title = str(obj.get("title", "")).strip()
    if title:
        return title
    rhythmic = str(obj.get("rhythmic", "")).strip()
    # 宋词常见字段：rhythmic
    if item_type == "ci" and rhythmic:
        return rhythmic
    return rhythmic or "无题"


def extract_highlight(lines: Sequence[str]) -> str:
    # 从正文中挑一句较短且语义完整的句子作为名句
    candidates: List[str] = []
    for line in lines:
        for piece in re.split(r"[，。！？；：、]", line):
            s = piece.strip()
            if 4 <= len(s) <= 18:
                candidates.append(s + "。")
    if candidates:
        return candidates[0]
    return lines[0] if lines else ""


def build_excerpt(lines: Sequence[str], max_lines: int = 2) -> str:
    if not lines:
        return ""
    return "\n".join(lines[:max_lines])


def derive_tags(
    obj: Dict,
    category_slug: str,
    dynasty: str,
    item_type: str,
    author: str,
) -> List[Tuple[str, str]]:
    tags: List[Tuple[str, str]] = []
    if author:
        tags.append(("author", author))
    if dynasty:
        tags.append(("dynasty", dynasty))

    # 主题标签：优先取原始 tags
    raw_tags = obj.get("tags")
    if isinstance(raw_tags, list):
        for t in raw_tags:
            name = str(t).strip()
            if name:
                tags.append(("theme", name))

    # 词风标签：按常见词人映射
    if category_slug == "song_ci":
        style = SONG_CI_STYLE_BY_AUTHOR.get(author)
        if style:
            tags.append(("style", style))

    # 体裁标签
    genre_name = {"poem": "诗", "ci": "词", "prose": "文"}.get(item_type, item_type)
    tags.append(("genre", genre_name))

    # 去重保持顺序
    seen: Set[Tuple[str, str]] = set()
    uniq: List[Tuple[str, str]] = []
    for t in tags:
        if t not in seen and t[1]:
            seen.add(t)
            uniq.append(t)
    return uniq


def fetch_existing_keys(conn: sqlite3.Connection) -> Set[Tuple[str, str, str]]:
    rows = conn.execute(
        """
        SELECT category_slug, title, author
        FROM library_items
        WHERE category_slug IN ('tang_poems', 'song_ci');
        """
    ).fetchall()
    return {(r["category_slug"], r["title"], r["author"]) for r in rows}


def get_or_create_tag_id(
    conn: sqlite3.Connection,
    category_slug: str,
    tag_type: str,
    name: str,
    cache: Dict[Tuple[str, str, str], int],
) -> int:
    key = (category_slug, tag_type, name)
    tag_id = cache.get(key)
    if tag_id:
        return tag_id

    conn.execute(
        """
        INSERT OR IGNORE INTO library_tags (category_slug, tag_type, name)
        VALUES (?, ?, ?);
        """,
        (category_slug, tag_type, name),
    )
    row = conn.execute(
        """
        SELECT id FROM library_tags
        WHERE category_slug = ? AND tag_type = ? AND name = ?;
        """,
        (category_slug, tag_type, name),
    ).fetchone()
    if not row:
        raise RuntimeError(f"无法获取 tag_id: {key}")
    tag_id = int(row["id"])
    cache[key] = tag_id
    return tag_id


def read_json_records(fp: Path) -> Iterable[Dict]:
    # chinese-poetry 绝大多数文件是 JSON 数组
    with fp.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        for obj in data:
            if isinstance(obj, dict):
                yield obj


def import_data(args: argparse.Namespace) -> None:
    dataset_root = Path(args.dataset_root)
    if not dataset_root.exists():
        raise FileNotFoundError(f"dataset_root 不存在: {dataset_root}")

    conn = connect(args.db_path)
    try:
        create_tables(conn)
        ensure_categories(conn)

        if args.mode == "replace":
            print("[INFO] replace 模式：清理历史导入数据...")
            reset_target_data(conn, include_others=args.include_others)

        existing = fetch_existing_keys(conn)
        tag_cache: Dict[Tuple[str, str, str], int] = {}

        files = discover_json_files(dataset_root, include_others=args.include_others)
        if not files:
            print("[WARN] 未找到可导入 JSON 文件，请检查目录结构。")
            return

        inserted = 0
        skipped_dup = 0
        parsed = 0
        failed = 0
        linked_tags = 0
        sort_order = 10000

        print(f"[INFO] 将处理文件数: {len(files)}")

        for idx, fp in enumerate(files, 1):
            category_slug, dynasty, item_type = infer_bucket(fp)
            source = "《全唐诗》" if category_slug == "tang_poems" else (
                "《全宋词》" if category_slug == "song_ci" else "chinese-poetry"
            )

            if not category_slug:
                continue

            try:
                for obj in read_json_records(fp):
                    parsed += 1
                    title = build_title(obj, item_type=item_type)
                    author = str(obj.get("author", "")).strip()
                    lines = normalize_lines(obj.get("paragraphs"))
                    highlight = extract_highlight(lines)
                    excerpt = build_excerpt(lines)
                    full_text = "\n".join(lines)

                    # sub_type / sub_group 处理
                    sub_group = author or ""
                    sub_type = ""
                    if item_type == "ci":
                        sub_type = SONG_CI_STYLE_BY_AUTHOR.get(author, "")

                    key = (category_slug, title, author)
                    if key in existing:
                        # 已存在时，尽量补齐全文，避免旧库只有摘要无法“查看全文”
                        if full_text:
                            conn.execute(
                                """
                                UPDATE library_items
                                SET full_text = CASE
                                    WHEN full_text IS NULL OR full_text = '' THEN ?
                                    ELSE full_text
                                END
                                WHERE category_slug = ? AND title = ? AND author = ?;
                                """,
                                (full_text, category_slug, title, author),
                            )
                        skipped_dup += 1
                        continue

                    cur = conn.execute(
                        """
                        INSERT INTO library_items (
                            category_slug, title, author, dynasty, item_type, sub_type, sub_group,
                            highlight, excerpt, full_text, source, sort_order
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            category_slug,
                            title,
                            author,
                            dynasty,
                            item_type,
                            sub_type,
                            sub_group,
                            highlight,
                            excerpt,
                            full_text,
                            source,
                            sort_order,
                        ),
                    )
                    item_id = int(cur.lastrowid)
                    existing.add(key)
                    inserted += 1
                    sort_order += 1

                    # 标签导入与关联
                    for tag_type, name in derive_tags(
                        obj=obj,
                        category_slug=category_slug,
                        dynasty=dynasty,
                        item_type=item_type,
                        author=author,
                    ):
                        tag_id = get_or_create_tag_id(
                            conn, category_slug, tag_type, name, tag_cache
                        )
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO library_item_tags (item_id, tag_id)
                            VALUES (?, ?);
                            """,
                            (item_id, tag_id),
                        )
                        linked_tags += 1

                    # 分批提交，避免长事务与内存增长
                    if inserted % max(1, args.batch_size) == 0:
                        conn.commit()
                        print(
                            f"[INFO] 已提交 {inserted} 条（去重跳过 {skipped_dup}），当前文件: {idx}/{len(files)} {fp.name}"
                        )

            except Exception as e:
                failed += 1
                print(f"[ERROR] 文件处理失败: {fp} -> {e}")

        conn.commit()
        print("========== 导入完成 ==========")
        print(f"解析记录数: {parsed}")
        print(f"成功插入: {inserted}")
        print(f"去重跳过: {skipped_dup}")
        print(f"标签关联写入: {linked_tags}")
        print(f"失败文件数: {failed}")
        print(f"数据库路径: {args.db_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    cli_args = parse_args()
    import_data(cli_args)

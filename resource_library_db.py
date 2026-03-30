import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
import time
from functools import wraps

DEFAULT_DB_PATH = os.getenv("RESOURCE_LIBRARY_DB_PATH", "resource_library.sqlite3")


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path,timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAl;")
    return conn


def create_tables(conn: sqlite3.Connection) -> None:
    # 资源库：分类
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS library_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            sort_order INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    # 资源库：作品/篇目（诗/词/古文/典籍章节/名篇）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS library_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_slug TEXT NOT NULL,
            title TEXT NOT NULL,
            author TEXT NOT NULL DEFAULT '',
            dynasty TEXT NOT NULL DEFAULT '',
            item_type TEXT NOT NULL DEFAULT '',   -- poem/ci/prose/classic_chapter/famous_prose
            sub_type TEXT NOT NULL DEFAULT '',    -- 拓展：题材/文体/流派/部类等
            sub_group TEXT NOT NULL DEFAULT '',   -- 拓展：作者分类/朝代分类/典籍卷类等
            highlight TEXT NOT NULL DEFAULT '',   -- 核心名句/核心价值说明
            excerpt TEXT NOT NULL DEFAULT '',     -- 核心段落（古文/名篇）或思想摘要
            full_text TEXT NOT NULL DEFAULT '',   -- 全文内容（用于资源库详情页展示）
            source TEXT NOT NULL DEFAULT '',      -- 出处：如《论语·学而》
            sort_order INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (category_slug) REFERENCES library_categories(slug) ON DELETE CASCADE
        );
        """
    )

    # 兼容历史数据库：若缺少 full_text 列则补齐
    existing_cols = {
        r["name"] for r in conn.execute("PRAGMA table_info(library_items);").fetchall()
    }
    if "full_text" not in existing_cols:
        conn.execute(
            "ALTER TABLE library_items ADD COLUMN full_text TEXT NOT NULL DEFAULT '';"
        )

    # 资源库：标签（用于多维度筛选）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS library_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_slug TEXT NOT NULL,
            tag_type TEXT NOT NULL,              -- author/theme/style/dynasty/genre/book/section
            name TEXT NOT NULL,
            UNIQUE(category_slug, tag_type, name),
            FOREIGN KEY (category_slug) REFERENCES library_categories(slug) ON DELETE CASCADE
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS library_item_tags (
            item_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            PRIMARY KEY (item_id, tag_id),
            FOREIGN KEY (item_id) REFERENCES library_items(id) ON DELETE CASCADE,
            FOREIGN KEY (tag_id) REFERENCES library_tags(id) ON DELETE CASCADE
        );
        """
    )


def _seed_categories() -> List[Dict[str, Any]]:
    return [
        {
            "slug": "tang_poems",
            "name": "唐诗三百首",
            "description": "按作者与题材梳理唐诗名作，配核心名句，便于检索与赏读。",
            "sort_order": 10,
        },
        {
            "slug": "song_ci",
            "name": "宋词精选",
            "description": "按流派与词人汇总宋词名篇，配核心名句，便于学习与背诵。",
            "sort_order": 20,
        },
        {
            "slug": "guwen_guanzhi",
            "name": "古文观止",
            "description": "按文体与朝代梳理名篇，配核心段落，便于理解文章结构与思想。",
            "sort_order": 30,
        },
        {
            "slug": "sishu_wujing",
            "name": "四书五经",
            "description": "提炼典籍核心篇章与思想脉络，配关键名句与要点摘要。",
            "sort_order": 40,
        },
    ]


def _seed_tags() -> Dict[str, List[Tuple[str, str]]]:
    # (tag_type, name)
    return {
        # 唐诗三百首：作者分类 + 题材分类
        "tang_poems": [
            ("author", "李白"),
            ("author", "杜甫"),
            ("author", "王维"),
            ("author", "白居易"),
            ("author", "孟浩然"),
            ("author", "岑参"),
            ("author", "高适"),
            ("author", "李商隐"),
            ("author", "杜牧"),
            ("author", "刘禹锡"),
            ("theme", "边塞诗"),
            ("theme", "田园山水"),
            ("theme", "咏物诗"),
            ("theme", "送别诗"),
            ("theme", "怀古咏史"),
            ("theme", "闺怨"),
            ("theme", "哲理"),
        ],
        # 宋词精选：流派分类 + 代表词人
        "song_ci": [
            ("style", "豪放派"),
            ("style", "婉约派"),
            ("author", "苏轼"),
            ("author", "辛弃疾"),
            ("author", "李清照"),
            ("author", "柳永"),
            ("author", "周邦彦"),
            ("author", "秦观"),
            ("author", "姜夔"),
            ("author", "晏殊"),
            ("author", "欧阳修"),
            ("author", "陆游"),
        ],
        # 古文观止：文体分类 + 朝代分类
        "guwen_guanzhi": [
            ("genre", "史传"),
            ("genre", "论说文"),
            ("genre", "杂记"),
            ("genre", "序"),
            ("genre", "表"),
            ("dynasty", "先秦"),
            ("dynasty", "两汉"),
            ("dynasty", "魏晋"),
            ("dynasty", "唐"),
            ("dynasty", "宋"),
        ],
        # 四书五经：典籍 + 核心部类/篇章（作为标签用于聚合）
        "sishu_wujing": [
            ("book", "论语"),
            ("book", "孟子"),
            ("book", "大学"),
            ("book", "中庸"),
            ("book", "诗经"),
            ("book", "尚书"),
            ("book", "礼记"),
            ("book", "周易"),
            ("book", "春秋"),
        ],
    }


def _seed_items() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    # =========================
    # 拓展：唐诗三百首-经典篇目（≥20）
    # =========================
    items.extend(
        [
            {
                "category_slug": "tang_poems",
                "title": "静夜思",
                "author": "李白",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "思乡",
                "sub_group": "李白",
                "highlight": "举头望明月，低头思故乡。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "李白"), ("theme", "哲理")],
                "sort_order": 10,
            },
            {
                "category_slug": "tang_poems",
                "title": "将进酒",
                "author": "李白",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "乐府/豪放",
                "sub_group": "李白",
                "highlight": "天生我材必有用，千金散尽还复来。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "李白")],
                "sort_order": 11,
            },
            {
                "category_slug": "tang_poems",
                "title": "望庐山瀑布",
                "author": "李白",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "山水",
                "sub_group": "李白",
                "highlight": "飞流直下三千尺，疑是银河落九天。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "李白"), ("theme", "田园山水")],
                "sort_order": 12,
            },
            {
                "category_slug": "tang_poems",
                "title": "黄鹤楼送孟浩然之广陵",
                "author": "李白",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "送别",
                "sub_group": "李白",
                "highlight": "孤帆远影碧空尽，唯见长江天际流。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "李白"), ("theme", "送别诗")],
                "sort_order": 13,
            },
            {
                "category_slug": "tang_poems",
                "title": "蜀道难",
                "author": "李白",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "乐府",
                "sub_group": "李白",
                "highlight": "蜀道之难，难于上青天！",
                "excerpt": "",
                "source": "",
                "tags": [("author", "李白")],
                "sort_order": 14,
            },
            {
                "category_slug": "tang_poems",
                "title": "登高",
                "author": "杜甫",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "律诗/感怀",
                "sub_group": "杜甫",
                "highlight": "无边落木萧萧下，不尽长江滚滚来。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "杜甫")],
                "sort_order": 20,
            },
            {
                "category_slug": "tang_poems",
                "title": "望岳",
                "author": "杜甫",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "咏物/山水",
                "sub_group": "杜甫",
                "highlight": "会当凌绝顶，一览众山小。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "杜甫"), ("theme", "咏物诗")],
                "sort_order": 21,
            },
            {
                "category_slug": "tang_poems",
                "title": "春望",
                "author": "杜甫",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "时事/忧国",
                "sub_group": "杜甫",
                "highlight": "国破山河在，城春草木深。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "杜甫")],
                "sort_order": 22,
            },
            {
                "category_slug": "tang_poems",
                "title": "闻官军收河南河北",
                "author": "杜甫",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "叙事/喜悦",
                "sub_group": "杜甫",
                "highlight": "即从巴峡穿巫峡，便下襄阳向洛阳。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "杜甫")],
                "sort_order": 23,
            },
            {
                "category_slug": "tang_poems",
                "title": "绝句（两个黄鹂鸣翠柳）",
                "author": "杜甫",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "田园山水",
                "sub_group": "杜甫",
                "highlight": "两个黄鹂鸣翠柳，一行白鹭上青天。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "杜甫"), ("theme", "田园山水")],
                "sort_order": 24,
            },
            {
                "category_slug": "tang_poems",
                "title": "山居秋暝",
                "author": "王维",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "田园山水",
                "sub_group": "王维",
                "highlight": "明月松间照，清泉石上流。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "王维"), ("theme", "田园山水")],
                "sort_order": 30,
            },
            {
                "category_slug": "tang_poems",
                "title": "鹿柴",
                "author": "王维",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "山水",
                "sub_group": "王维",
                "highlight": "空山不见人，但闻人语响。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "王维"), ("theme", "田园山水")],
                "sort_order": 31,
            },
            {
                "category_slug": "tang_poems",
                "title": "送元二使安西",
                "author": "王维",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "送别",
                "sub_group": "王维",
                "highlight": "劝君更尽一杯酒，西出阳关无故人。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "王维"), ("theme", "送别诗")],
                "sort_order": 32,
            },
            {
                "category_slug": "tang_poems",
                "title": "使至塞上",
                "author": "王维",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "边塞",
                "sub_group": "王维",
                "highlight": "大漠孤烟直，长河落日圆。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "王维"), ("theme", "边塞诗")],
                "sort_order": 33,
            },
            {
                "category_slug": "tang_poems",
                "title": "春晓",
                "author": "孟浩然",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "田园/春景",
                "sub_group": "孟浩然",
                "highlight": "春眠不觉晓，处处闻啼鸟。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "孟浩然"), ("theme", "田园山水")],
                "sort_order": 40,
            },
            {
                "category_slug": "tang_poems",
                "title": "望洞庭湖赠张丞相",
                "author": "孟浩然",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "山水/干谒",
                "sub_group": "孟浩然",
                "highlight": "气蒸云梦泽，波撼岳阳城。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "孟浩然"), ("theme", "田园山水")],
                "sort_order": 41,
            },
            {
                "category_slug": "tang_poems",
                "title": "白雪歌送武判官归京",
                "author": "岑参",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "边塞",
                "sub_group": "岑参",
                "highlight": "忽如一夜春风来，千树万树梨花开。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "岑参"), ("theme", "边塞诗")],
                "sort_order": 50,
            },
            {
                "category_slug": "tang_poems",
                "title": "燕歌行（节选）",
                "author": "高适",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "边塞",
                "sub_group": "高适",
                "highlight": "战士军前半死生，美人帐下犹歌舞。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "高适"), ("theme", "边塞诗")],
                "sort_order": 51,
            },
            {
                "category_slug": "tang_poems",
                "title": "钱塘湖春行",
                "author": "白居易",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "山水",
                "sub_group": "白居易",
                "highlight": "乱花渐欲迷人眼，浅草才能没马蹄。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "白居易"), ("theme", "田园山水")],
                "sort_order": 60,
            },
            {
                "category_slug": "tang_poems",
                "title": "赋得古原草送别",
                "author": "白居易",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "咏物/送别",
                "sub_group": "白居易",
                "highlight": "野火烧不尽，春风吹又生。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "白居易"), ("theme", "咏物诗"), ("theme", "送别诗")],
                "sort_order": 61,
            },
            {
                "category_slug": "tang_poems",
                "title": "陋室铭（节选）",
                "author": "刘禹锡",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "铭",
                "sub_group": "刘禹锡",
                "highlight": "山不在高，有仙则名。水不在深，有龙则灵。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "刘禹锡")],
                "sort_order": 70,
            },
            {
                "category_slug": "tang_poems",
                "title": "夜雨寄北",
                "author": "李商隐",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "抒情",
                "sub_group": "李商隐",
                "highlight": "何当共剪西窗烛，却话巴山夜雨时。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "李商隐")],
                "sort_order": 80,
            },
            {
                "category_slug": "tang_poems",
                "title": "登乐游原",
                "author": "李商隐",
                "dynasty": "唐",
                "item_type": "poem",
                "sub_type": "哲理",
                "sub_group": "李商隐",
                "highlight": "夕阳无限好，只是近黄昏。",
                "excerpt": "",
                "source": "",
                "tags": [("author", "李商隐"), ("theme", "哲理")],
                "sort_order": 81,
            },
        ]
    )

    # =========================
    # 拓展：宋词精选-经典篇目（≥20）
    # =========================
    items.extend(
        [
            {
                "category_slug": "song_ci",
                "title": "念奴娇·赤壁怀古",
                "author": "苏轼",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "豪放派",
                "sub_group": "苏轼",
                "highlight": "大江东去，浪淘尽，千古风流人物。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "豪放派"), ("author", "苏轼")],
                "sort_order": 10,
            },
            {
                "category_slug": "song_ci",
                "title": "水调歌头·明月几时有",
                "author": "苏轼",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "豪放派",
                "sub_group": "苏轼",
                "highlight": "但愿人长久，千里共婵娟。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "豪放派"), ("author", "苏轼")],
                "sort_order": 11,
            },
            {
                "category_slug": "song_ci",
                "title": "江城子·密州出猎",
                "author": "苏轼",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "豪放派",
                "sub_group": "苏轼",
                "highlight": "会挽雕弓如满月，西北望，射天狼。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "豪放派"), ("author", "苏轼")],
                "sort_order": 12,
            },
            {
                "category_slug": "song_ci",
                "title": "破阵子·为陈同甫赋壮词以寄之",
                "author": "辛弃疾",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "豪放派",
                "sub_group": "辛弃疾",
                "highlight": "醉里挑灯看剑，梦回吹角连营。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "豪放派"), ("author", "辛弃疾")],
                "sort_order": 20,
            },
            {
                "category_slug": "song_ci",
                "title": "青玉案·元夕",
                "author": "辛弃疾",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "豪放派",
                "sub_group": "辛弃疾",
                "highlight": "众里寻他千百度，蓦然回首，那人却在，灯火阑珊处。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "豪放派"), ("author", "辛弃疾")],
                "sort_order": 21,
            },
            {
                "category_slug": "song_ci",
                "title": "永遇乐·京口北固亭怀古",
                "author": "辛弃疾",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "豪放派",
                "sub_group": "辛弃疾",
                "highlight": "想当年，金戈铁马，气吞万里如虎。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "豪放派"), ("author", "辛弃疾")],
                "sort_order": 22,
            },
            {
                "category_slug": "song_ci",
                "title": "声声慢·寻寻觅觅",
                "author": "李清照",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "婉约派",
                "sub_group": "李清照",
                "highlight": "寻寻觅觅，冷冷清清，凄凄惨惨戚戚。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "婉约派"), ("author", "李清照")],
                "sort_order": 30,
            },
            {
                "category_slug": "song_ci",
                "title": "如梦令·常记溪亭日暮",
                "author": "李清照",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "婉约派",
                "sub_group": "李清照",
                "highlight": "争渡，争渡，惊起一滩鸥鹭。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "婉约派"), ("author", "李清照")],
                "sort_order": 31,
            },
            {
                "category_slug": "song_ci",
                "title": "一剪梅·红藕香残玉簟秋",
                "author": "李清照",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "婉约派",
                "sub_group": "李清照",
                "highlight": "此情无计可消除，才下眉头，却上心头。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "婉约派"), ("author", "李清照")],
                "sort_order": 32,
            },
            {
                "category_slug": "song_ci",
                "title": "雨霖铃·寒蝉凄切",
                "author": "柳永",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "婉约派",
                "sub_group": "柳永",
                "highlight": "今宵酒醒何处？杨柳岸，晓风残月。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "婉约派"), ("author", "柳永")],
                "sort_order": 40,
            },
            {
                "category_slug": "song_ci",
                "title": "蝶恋花·伫倚危楼风细细",
                "author": "柳永",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "婉约派",
                "sub_group": "柳永",
                "highlight": "衣带渐宽终不悔，为伊消得人憔悴。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "婉约派"), ("author", "柳永")],
                "sort_order": 41,
            },
            {
                "category_slug": "song_ci",
                "title": "卜算子·我住长江头",
                "author": "李之仪",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "婉约派",
                "sub_group": "李之仪",
                "highlight": "只愿君心似我心，定不负相思意。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "婉约派")],
                "sort_order": 42,
            },
            {
                "category_slug": "song_ci",
                "title": "鹊桥仙·纤云弄巧",
                "author": "秦观",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "婉约派",
                "sub_group": "秦观",
                "highlight": "两情若是久长时，又岂在朝朝暮暮。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "婉约派"), ("author", "秦观")],
                "sort_order": 50,
            },
            {
                "category_slug": "song_ci",
                "title": "虞美人·春花秋月何时了",
                "author": "李煜",
                "dynasty": "南唐",
                "item_type": "ci",
                "sub_type": "婉约派",
                "sub_group": "李煜",
                "highlight": "问君能有几多愁？恰似一江春水向东流。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "婉约派")],
                "sort_order": 51,
            },
            {
                "category_slug": "song_ci",
                "title": "相见欢·无言独上西楼",
                "author": "李煜",
                "dynasty": "南唐",
                "item_type": "ci",
                "sub_type": "婉约派",
                "sub_group": "李煜",
                "highlight": "剪不断，理还乱，是离愁。别是一般滋味在心头。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "婉约派")],
                "sort_order": 52,
            },
            {
                "category_slug": "song_ci",
                "title": "浣溪沙·一曲新词酒一杯",
                "author": "晏殊",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "婉约派",
                "sub_group": "晏殊",
                "highlight": "无可奈何花落去，似曾相识燕归来。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "婉约派"), ("author", "晏殊")],
                "sort_order": 60,
            },
            {
                "category_slug": "song_ci",
                "title": "蝶恋花·庭院深深深几许",
                "author": "欧阳修",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "婉约派",
                "sub_group": "欧阳修",
                "highlight": "泪眼问花花不语，乱红飞过秋千去。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "婉约派"), ("author", "欧阳修")],
                "sort_order": 61,
            },
            {
                "category_slug": "song_ci",
                "title": "苏幕遮·碧云天",
                "author": "范仲淹",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "婉约派",
                "sub_group": "范仲淹",
                "highlight": "黯乡魂，追旅思，夜夜除非，好梦留人睡。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "婉约派")],
                "sort_order": 62,
            },
            {
                "category_slug": "song_ci",
                "title": "满江红·怒发冲冠",
                "author": "岳飞",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "豪放派",
                "sub_group": "岳飞",
                "highlight": "三十功名尘与土，八千里路云和月。",
                "excerpt": "",
                "source": "",
                "tags": [("style", "豪放派")],
                "sort_order": 70,
            },
            {
                "category_slug": "song_ci",
                "title": "钗头凤·红酥手",
                "author": "陆游",
                "dynasty": "宋",
                "item_type": "ci",
                "sub_type": "婉约派",
                "sub_group": "陆游",
                "highlight": "错，错，错！",
                "excerpt": "",
                "source": "",
                "tags": [("style", "婉约派"), ("author", "陆游")],
                "sort_order": 71,
            },
        ]
    )

    # =========================
    # 拓展：古文观止-经典篇目（≥15，含核心段落）
    # =========================
    items.extend(
        [
            {
                "category_slug": "guwen_guanzhi",
                "title": "曹刿论战",
                "author": "《左传》",
                "dynasty": "先秦",
                "item_type": "prose",
                "sub_type": "史传",
                "sub_group": "先秦",
                "highlight": "一鼓作气，再而衰，三而竭。",
                "excerpt": "夫战，勇气也。一鼓作气，再而衰，三而竭。彼竭我盈，故克之。",
                "source": "《左传·庄公十年》",
                "tags": [("genre", "史传"), ("dynasty", "先秦")],
                "sort_order": 10,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "郑伯克段于鄢",
                "author": "《左传》",
                "dynasty": "先秦",
                "item_type": "prose",
                "sub_type": "史传",
                "sub_group": "先秦",
                "highlight": "多行不义必自毙。",
                "excerpt": "多行不义必自毙，子姑待之。",
                "source": "《左传·隐公元年》",
                "tags": [("genre", "史传"), ("dynasty", "先秦")],
                "sort_order": 11,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "出师表",
                "author": "诸葛亮",
                "dynasty": "三国",
                "item_type": "prose",
                "sub_type": "表",
                "sub_group": "魏晋",
                "highlight": "亲贤臣，远小人，此先汉所以兴隆也。",
                "excerpt": "亲贤臣，远小人，此先汉所以兴隆也；亲小人，远贤臣，此后汉所以倾颓也。",
                "source": "",
                "tags": [("genre", "表"), ("dynasty", "魏晋")],
                "sort_order": 12,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "陈情表",
                "author": "李密",
                "dynasty": "西晋",
                "item_type": "prose",
                "sub_type": "表",
                "sub_group": "魏晋",
                "highlight": "臣无祖母，无以至今日；祖母无臣，无以终余年。",
                "excerpt": "臣无祖母，无以至今日；祖母无臣，无以终余年。母孙二人，更相为命。",
                "source": "",
                "tags": [("genre", "表"), ("dynasty", "魏晋")],
                "sort_order": 13,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "桃花源记",
                "author": "陶渊明",
                "dynasty": "东晋",
                "item_type": "prose",
                "sub_type": "杂记",
                "sub_group": "魏晋",
                "highlight": "土地平旷，屋舍俨然，有良田美池桑竹之属。",
                "excerpt": "土地平旷，屋舍俨然，有良田美池桑竹之属。阡陌交通，鸡犬相闻。",
                "source": "",
                "tags": [("genre", "杂记"), ("dynasty", "魏晋")],
                "sort_order": 14,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "兰亭集序",
                "author": "王羲之",
                "dynasty": "东晋",
                "item_type": "prose",
                "sub_type": "序",
                "sub_group": "魏晋",
                "highlight": "后之视今，亦犹今之视昔。",
                "excerpt": "夫人之相与，俯仰一世，或取诸怀抱，悟言一室之内；或因寄所托，放浪形骸之外。",
                "source": "",
                "tags": [("genre", "序"), ("dynasty", "魏晋")],
                "sort_order": 15,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "师说",
                "author": "韩愈",
                "dynasty": "唐",
                "item_type": "prose",
                "sub_type": "论说文",
                "sub_group": "唐",
                "highlight": "师者，所以传道受业解惑也。",
                "excerpt": "师者，所以传道受业解惑也。人非生而知之者，孰能无惑？",
                "source": "",
                "tags": [("genre", "论说文"), ("dynasty", "唐")],
                "sort_order": 20,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "陋室铭",
                "author": "刘禹锡",
                "dynasty": "唐",
                "item_type": "prose",
                "sub_type": "论说文",
                "sub_group": "唐",
                "highlight": "斯是陋室，惟吾德馨。",
                "excerpt": "斯是陋室，惟吾德馨。苔痕上阶绿，草色入帘青。",
                "source": "",
                "tags": [("genre", "论说文"), ("dynasty", "唐")],
                "sort_order": 21,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "滕王阁序",
                "author": "王勃",
                "dynasty": "唐",
                "item_type": "prose",
                "sub_type": "序",
                "sub_group": "唐",
                "highlight": "落霞与孤鹜齐飞，秋水共长天一色。",
                "excerpt": "落霞与孤鹜齐飞，秋水共长天一色。渔舟唱晚，响穷彭蠡之滨。",
                "source": "",
                "tags": [("genre", "序"), ("dynasty", "唐")],
                "sort_order": 22,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "岳阳楼记",
                "author": "范仲淹",
                "dynasty": "宋",
                "item_type": "prose",
                "sub_type": "杂记",
                "sub_group": "宋",
                "highlight": "先天下之忧而忧，后天下之乐而乐。",
                "excerpt": "不以物喜，不以己悲。居庙堂之高则忧其民；处江湖之远则忧其君。",
                "source": "",
                "tags": [("genre", "杂记"), ("dynasty", "宋")],
                "sort_order": 30,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "醉翁亭记",
                "author": "欧阳修",
                "dynasty": "宋",
                "item_type": "prose",
                "sub_type": "杂记",
                "sub_group": "宋",
                "highlight": "醉翁之意不在酒，在乎山水之间也。",
                "excerpt": "醉翁之意不在酒，在乎山水之间也。山水之乐，得之心而寓之酒也。",
                "source": "",
                "tags": [("genre", "杂记"), ("dynasty", "宋")],
                "sort_order": 31,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "爱莲说",
                "author": "周敦颐",
                "dynasty": "宋",
                "item_type": "prose",
                "sub_type": "论说文",
                "sub_group": "宋",
                "highlight": "出淤泥而不染，濯清涟而不妖。",
                "excerpt": "予独爱莲之出淤泥而不染，濯清涟而不妖，中通外直，不蔓不枝。",
                "source": "",
                "tags": [("genre", "论说文"), ("dynasty", "宋")],
                "sort_order": 32,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "前赤壁赋",
                "author": "苏轼",
                "dynasty": "宋",
                "item_type": "prose",
                "sub_type": "杂记",
                "sub_group": "宋",
                "highlight": "寄蜉蝣于天地，渺沧海之一粟。",
                "excerpt": "寄蜉蝣于天地，渺沧海之一粟。哀吾生之须臾，羡长江之无穷。",
                "source": "",
                "tags": [("genre", "杂记"), ("dynasty", "宋")],
                "sort_order": 33,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "六国论",
                "author": "苏洵",
                "dynasty": "宋",
                "item_type": "prose",
                "sub_type": "论说文",
                "sub_group": "宋",
                "highlight": "苟以天下之大，而从六国破亡之故事，是又在六国下矣。",
                "excerpt": "以地事秦，犹抱薪救火，薪不尽，火不灭。",
                "source": "",
                "tags": [("genre", "论说文"), ("dynasty", "宋")],
                "sort_order": 34,
            },
            {
                "category_slug": "guwen_guanzhi",
                "title": "阿房宫赋",
                "author": "杜牧",
                "dynasty": "唐",
                "item_type": "prose",
                "sub_type": "论说文",
                "sub_group": "唐",
                "highlight": "后人哀之而不鉴之，亦使后人而复哀后人也。",
                "excerpt": "后人哀之而不鉴之，亦使后人而复哀后人也。",
                "source": "",
                "tags": [("genre", "论说文"), ("dynasty", "唐")],
                "sort_order": 35,
            },
        ]
    )

    # =========================
    # 拓展：四书五经-核心章节/名句/思想摘要
    # 用 item_type=classic_chapter
    # =========================
    items.extend(
        [
            # 论语
            {
                "category_slug": "sishu_wujing",
                "title": "学而篇",
                "author": "",
                "dynasty": "先秦",
                "item_type": "classic_chapter",
                "sub_type": "核心篇章",
                "sub_group": "论语",
                "highlight": "学而时习之，不亦说乎？",
                "excerpt": "要点：以“学习-温习-实践”为路径，强调修身与为人处世的日常功夫。",
                "source": "《论语·学而》",
                "tags": [("book", "论语")],
                "sort_order": 10,
            },
            {
                "category_slug": "sishu_wujing",
                "title": "为政篇",
                "author": "",
                "dynasty": "先秦",
                "item_type": "classic_chapter",
                "sub_type": "核心篇章",
                "sub_group": "论语",
                "highlight": "为政以德，譬如北辰，居其所而众星共之。",
                "excerpt": "要点：以德化民、以礼成俗，强调政治的道德基础与示范效应。",
                "source": "《论语·为政》",
                "tags": [("book", "论语")],
                "sort_order": 11,
            },
            # 孟子
            {
                "category_slug": "sishu_wujing",
                "title": "梁惠王上",
                "author": "",
                "dynasty": "先秦",
                "item_type": "classic_chapter",
                "sub_type": "核心篇章",
                "sub_group": "孟子",
                "highlight": "民为贵，社稷次之，君为轻。",
                "excerpt": "要点：仁政与民本思想，强调施政应以百姓福祉为先。",
                "source": "《孟子·梁惠王上》",
                "tags": [("book", "孟子")],
                "sort_order": 20,
            },
            {
                "category_slug": "sishu_wujing",
                "title": "告子上（性善论要点）",
                "author": "",
                "dynasty": "先秦",
                "item_type": "classic_chapter",
                "sub_type": "思想要点",
                "sub_group": "孟子",
                "highlight": "恻隐之心，人皆有之。",
                "excerpt": "要点：以“四端”说明人性向善的内在根基（恻隐、羞恶、辞让、是非）。",
                "source": "《孟子·告子上》",
                "tags": [("book", "孟子")],
                "sort_order": 21,
            },
            # 大学
            {
                "category_slug": "sishu_wujing",
                "title": "三纲领八条目",
                "author": "",
                "dynasty": "先秦",
                "item_type": "classic_chapter",
                "sub_type": "核心框架",
                "sub_group": "大学",
                "highlight": "大学之道，在明明德，在亲民，在止于至善。",
                "excerpt": "要点：以修身为本，层层外推到齐家、治国、平天下，构成儒家实践路径。",
                "source": "《大学》",
                "tags": [("book", "大学")],
                "sort_order": 30,
            },
            # 中庸
            {
                "category_slug": "sishu_wujing",
                "title": "中和之道",
                "author": "",
                "dynasty": "先秦",
                "item_type": "classic_chapter",
                "sub_type": "核心思想",
                "sub_group": "中庸",
                "highlight": "喜怒哀乐之未发，谓之中；发而皆中节，谓之和。",
                "excerpt": "要点：强调情感与行为的节制与合度，以“中和”达成个人与社会的秩序。",
                "source": "《中庸》",
                "tags": [("book", "中庸")],
                "sort_order": 40,
            },
            # 诗经：国风/雅/颂
            {
                "category_slug": "sishu_wujing",
                "title": "国风（部类）",
                "author": "",
                "dynasty": "先秦",
                "item_type": "classic_chapter",
                "sub_type": "部类",
                "sub_group": "诗经",
                "highlight": "关关雎鸠，在河之洲。",
                "excerpt": "要点：多为各地民歌，反映民情风俗，是理解先秦社会生活的重要窗口。",
                "source": "《诗经》",
                "tags": [("book", "诗经")],
                "sort_order": 50,
            },
            {
                "category_slug": "sishu_wujing",
                "title": "大雅（部类）",
                "author": "",
                "dynasty": "先秦",
                "item_type": "classic_chapter",
                "sub_type": "部类",
                "sub_group": "诗经",
                "highlight": "周虽旧邦，其命维新。",
                "excerpt": "要点：多与王政、礼乐相关，兼具史诗与政治教化功能。",
                "source": "《诗经·大雅》",
                "tags": [("book", "诗经")],
                "sort_order": 51,
            },
            # 周易
            {
                "category_slug": "sishu_wujing",
                "title": "乾卦（自强不息）",
                "author": "",
                "dynasty": "先秦",
                "item_type": "classic_chapter",
                "sub_type": "卦义要点",
                "sub_group": "周易",
                "highlight": "天行健，君子以自强不息。",
                "excerpt": "要点：以“健”释天道，推及君子修为，强调持续进取与自我完善。",
                "source": "《周易·乾》",
                "tags": [("book", "周易")],
                "sort_order": 60,
            },
            {
                "category_slug": "sishu_wujing",
                "title": "坤卦（厚德载物）",
                "author": "",
                "dynasty": "先秦",
                "item_type": "classic_chapter",
                "sub_type": "卦义要点",
                "sub_group": "周易",
                "highlight": "地势坤，君子以厚德载物。",
                "excerpt": "要点：以“顺”释地道，强调包容、承载与持守。",
                "source": "《周易·坤》",
                "tags": [("book", "周易")],
                "sort_order": 61,
            },
        ]
    )

    # =========================
    # 拓展：历代名篇（≥10，尽量不与上面重复）
    # =========================
    items.extend(
        [
            {
                "category_slug": "famous_essays",
                "title": "过秦论（上）",
                "author": "贾谊",
                "dynasty": "西汉",
                "item_type": "famous_prose",
                "sub_type": "政论",
                "sub_group": "汉",
                "highlight": "仁义不施而攻守之势异也。",
                "excerpt": "核心价值：以秦之兴亡论“德政/仁义”与制度之要，善用铺陈对比的论证方式。",
                "source": "",
                "tags": [("dynasty", "汉")],
                "sort_order": 10,
            },
            {
                "category_slug": "famous_essays",
                "title": "报任安书（节选）",
                "author": "司马迁",
                "dynasty": "西汉",
                "item_type": "famous_prose",
                "sub_type": "书信",
                "sub_group": "汉",
                "highlight": "人固有一死，或重于泰山，或轻于鸿毛。",
                "excerpt": "核心价值：以个人遭际论生命价值与志业担当，文字沉郁顿挫，情理交融。",
                "source": "",
                "tags": [("dynasty", "汉")],
                "sort_order": 11,
            },
            {
                "category_slug": "famous_essays",
                "title": "祭十二郎文（节选）",
                "author": "韩愈",
                "dynasty": "唐",
                "item_type": "famous_prose",
                "sub_type": "祭文",
                "sub_group": "唐",
                "highlight": "呜呼哀哉！",
                "excerpt": "核心价值：以真挚至痛的叙述与排比抒情，成为古代祭文中的情感典范。",
                "source": "",
                "tags": [("dynasty", "唐")],
                "sort_order": 20,
            },
            {
                "category_slug": "famous_essays",
                "title": "捕蛇者说",
                "author": "柳宗元",
                "dynasty": "唐",
                "item_type": "famous_prose",
                "sub_type": "寓言/政论",
                "sub_group": "唐",
                "highlight": "苛政猛于虎也。",
                "excerpt": "核心价值：借捕蛇者之口揭示苛政之害，以小见大，批判深刻。",
                "source": "",
                "tags": [("dynasty", "唐")],
                "sort_order": 21,
            },
            {
                "category_slug": "famous_essays",
                "title": "小石潭记",
                "author": "柳宗元",
                "dynasty": "唐",
                "item_type": "famous_prose",
                "sub_type": "山水游记",
                "sub_group": "唐",
                "highlight": "凄神寒骨，悄怆幽邃。",
                "excerpt": "核心价值：景与情互渗，以清幽景致映照孤峭心境，是唐代山水小品名作。",
                "source": "",
                "tags": [("dynasty", "唐")],
                "sort_order": 22,
            },
            {
                "category_slug": "famous_essays",
                "title": "范进中举（节选）",
                "author": "吴敬梓",
                "dynasty": "清",
                "item_type": "famous_prose",
                "sub_type": "小说片段",
                "sub_group": "清",
                "highlight": "喜极而狂（讽刺科举心态）。",
                "excerpt": "核心价值：以夸张笔法写科举对人性的扭曲，讽刺锋利，极具社会批判意义。",
                "source": "《儒林外史》",
                "tags": [("dynasty", "清")],
                "sort_order": 30,
            },
            {
                "category_slug": "famous_essays",
                "title": "项脊轩志",
                "author": "归有光",
                "dynasty": "明",
                "item_type": "famous_prose",
                "sub_type": "记",
                "sub_group": "明",
                "highlight": "庭有枇杷树，吾妻死之年所手植也。",
                "excerpt": "核心价值：以小轩记家事与人生无常，笔致清淡而情深，世称“明文第一”。",
                "source": "",
                "tags": [("dynasty", "明")],
                "sort_order": 31,
            },
            {
                "category_slug": "famous_essays",
                "title": "岳阳楼记（价值视角）",
                "author": "范仲淹",
                "dynasty": "宋",
                "item_type": "famous_prose",
                "sub_type": "记",
                "sub_group": "宋",
                "highlight": "先天下之忧而忧，后天下之乐而乐。",
                "excerpt": "核心价值：把个人情怀上升到家国责任，成为士大夫精神的典型表达。",
                "source": "",
                "tags": [("dynasty", "宋")],
                "sort_order": 32,
            },
            {
                "category_slug": "famous_essays",
                "title": "兰亭集序（价值视角）",
                "author": "王羲之",
                "dynasty": "东晋",
                "item_type": "famous_prose",
                "sub_type": "序",
                "sub_group": "魏晋",
                "highlight": "后之视今，亦犹今之视昔。",
                "excerpt": "核心价值：以自然景事引出人生无常与情感真切，兼具哲思与审美典范。",
                "source": "",
                "tags": [("dynasty", "魏晋")],
                "sort_order": 33,
            },
            {
                "category_slug": "famous_essays",
                "title": "出师表（价值视角）",
                "author": "诸葛亮",
                "dynasty": "三国",
                "item_type": "famous_prose",
                "sub_type": "表",
                "sub_group": "魏晋",
                "highlight": "鞠躬尽瘁，死而后已。",
                "excerpt": "核心价值：以忠诚与责任为核心的政治伦理表达，语言恳切，情理兼备。",
                "source": "",
                "tags": [("dynasty", "魏晋")],
                "sort_order": 34,
            },
        ]
    )

    return items

def retry_on_lock(max_retries=3, delay=0.5):
    """装饰器：当遇到数据库锁错误时自动重试"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    if 'database is locked' in str(e) and attempt < max_retries - 1:
                        time.sleep(delay)
                        continue
                    raise
            return None
        return wrapper
    return decorator

@retry_on_lock(max_retries=3, delay=0.5)

def ensure_initialized(db_path: str = DEFAULT_DB_PATH) -> None:
    """
    初始化资源库数据库（建表+种子数据）。
    若已存在数据，则不重复插入。
    """
    conn = _connect(db_path)
    try:
        create_tables(conn)

        # 若已有分类数据，视为已初始化
        row = conn.execute("SELECT COUNT(1) AS c FROM library_categories;").fetchone()
        if row and int(row["c"]) > 0:
            conn.commit()
            return

        # 插入分类
        for cat in _seed_categories():
            conn.execute(
                """
                INSERT INTO library_categories (slug, name, description, sort_order)
                VALUES (?, ?, ?, ?);
                """,
                (cat["slug"], cat["name"], cat["description"], cat["sort_order"]),
            )
        valid_categories = {cat["slug"] for cat in _seed_categories()}

        # 插入标签
        tags_map = _seed_tags()
        for category_slug, tags in tags_map.items():
            for tag_type, name in tags:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO library_tags (category_slug, tag_type, name)
                    VALUES (?, ?, ?);
                    """,
                    (category_slug, tag_type, name),
                )

        # 便于后续关联：预取 tag_id
        tag_id_index: Dict[Tuple[str, str, str], int] = {}
        for r in conn.execute(
            "SELECT id, category_slug, tag_type, name FROM library_tags;"
        ).fetchall():
            tag_id_index[(r["category_slug"], r["tag_type"], r["name"])] = int(r["id"])

        # 插入条目并关联标签
        for item in _seed_items():
            # 历史版本可能存在已下线分类（如 famous_essays），这里直接跳过
            if item["category_slug"] not in valid_categories:
                continue
            cur = conn.execute(
                """
                INSERT INTO library_items (
                    category_slug, title, author, dynasty, item_type, sub_type, sub_group,
                    highlight, excerpt, full_text, source, sort_order
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    item["category_slug"],
                    item["title"],
                    item.get("author", ""),
                    item.get("dynasty", ""),
                    item.get("item_type", ""),
                    item.get("sub_type", ""),
                    item.get("sub_group", ""),
                    item.get("highlight", ""),
                    item.get("excerpt", ""),
                    item.get("full_text", ""),
                    item.get("source", ""),
                    item.get("sort_order", 0),
                ),
            )
            item_id = int(cur.lastrowid)

            for tag_type, name in item.get("tags", []):
                tag_id = tag_id_index.get((item["category_slug"], tag_type, name))
                if tag_id:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO library_item_tags (item_id, tag_id)
                        VALUES (?, ?);
                        """,
                        (item_id, tag_id),
                    )

        conn.commit()
    finally:
        conn.close()


def list_categories(db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT slug, name, description, sort_order
            FROM library_categories
            ORDER BY sort_order ASC, id ASC;
            """
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def list_facets(db_path: str, category_slug: str) -> Dict[str, List[str]]:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT tag_type, name
            FROM library_tags
            WHERE category_slug = ?
            ORDER BY tag_type ASC, name ASC;
            """,
            (category_slug,),
        ).fetchall()
        facets: Dict[str, List[str]] = {}
        for r in rows:
            facets.setdefault(r["tag_type"], []).append(r["name"])
        return facets
    finally:
        conn.close()


def list_items(
    db_path: str,
    category_slug: str,
    tag_type: Optional[str] = None,
    tag_name: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    conn = _connect(db_path)
    try:
        params: List[Any] = [category_slug]
        where = ["i.category_slug = ?"]

        join = ""
        if tag_type and tag_name:
            join = """
            JOIN library_item_tags it ON it.item_id = i.id
            JOIN library_tags t ON t.id = it.tag_id
            """
            where.append("t.tag_type = ? AND t.name = ?")
            params.extend([tag_type, tag_name])

        if q:
            where.append(
                "(i.title LIKE ? OR i.author LIKE ? OR i.highlight LIKE ? OR i.excerpt LIKE ? OR i.full_text LIKE ? OR i.source LIKE ?)"
            )
            like = f"%{q}%"
            params.extend([like, like, like, like, like, like])

        params.append(limit)

        rows = conn.execute(
            f"""
            SELECT
                i.id, i.category_slug, i.title, i.author, i.dynasty, i.item_type,
                i.sub_type, i.sub_group, i.highlight, i.excerpt, i.source, i.sort_order
            FROM library_items i
            {join}
            WHERE {' AND '.join(where)}
            ORDER BY i.sort_order ASC, i.id ASC
            LIMIT ?;
            """,
            tuple(params),
        ).fetchall()

        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_item(db_path: str, item_id: int) -> Optional[Dict[str, Any]]:
    conn = _connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT
                id, category_slug, title, author, dynasty, item_type,
                sub_type, sub_group, highlight, excerpt, full_text, source, sort_order
            FROM library_items
            WHERE id = ?;
            """,
            (item_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

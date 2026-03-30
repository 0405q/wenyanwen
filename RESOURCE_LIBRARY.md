# 资源库内容说明文档

本项目的“资源库”模块用于对古典文籍与经典篇目进行**结构化归档**与**可检索展示**，与主功能（文言文翻译/断句/虚词/句式分析）并行，提供“可持续扩充”的内容底座。

## 一、数据落地方式

- **存储**：SQLite（默认文件 `resource_library.sqlite3`）
- **初始化**：应用首次运行会自动建表并写入种子数据
  - 触发点：访问首页 `/` 或任意 ` /api/library/* ` 接口时
- **实现文件**：`resource_library_db.py`
  - **建表**：`create_tables`
  - **初始化**：`ensure_initialized`
  - **查询**：`list_categories` / `list_facets` / `list_items` / `get_item`

> 如需自定义数据库路径，可设置环境变量 `RESOURCE_LIBRARY_DB_PATH`。

## 二、分类结构与拓展内容

资源库目前内置 4 个核心分类（与你的界面分类一致）：

1. **唐诗三百首**（`tang_poems`）
   - **作者分类（标签 tag_type=author）**：李白、杜甫、王维、白居易、孟浩然、岑参、高适、李商隐、刘禹锡等
   - **题材分类（标签 tag_type=theme）**：边塞诗、田园山水、咏物诗、送别诗、怀古咏史、哲理等
   - **经典篇目明细**：已内置 ≥ 20 首（含 标题 + 作者 + 核心名句）

2. **宋词精选**（`song_ci`）
   - **流派分类（tag_type=style）**：豪放派、婉约派
   - **代表词人（tag_type=author）**：苏轼、辛弃疾、李清照、柳永、秦观、晏殊、欧阳修、陆游等
   - **经典篇目明细**：已内置 ≥ 20 首（含 标题 + 作者 + 核心名句）

3. **古文观止**（`guwen_guanzhi`）
   - **文体分类（tag_type=genre）**：史传、论说文、杂记、序、表等
   - **朝代分类（tag_type=dynasty）**：先秦、两汉、魏晋、唐、宋等
   - **经典篇目明细**：已内置 ≥ 15 篇（含 标题 + 作者/出处 + 核心段落）

4. **四书五经**（`sishu_wujing`）
   - **典籍分类（tag_type=book）**：论语、孟子、大学、中庸、诗经、尚书、礼记、周易、春秋
   - **每部典籍的核心章节/要点**：以“条目”形式内置（如《论语·学而》《为政》《大学》三纲领八条目等）
   - **核心名句/思想摘要**：字段 `highlight`（名句）+ `excerpt`（要点摘要）+ `source`（出处）

## 三、数据库表结构（摘要）

- `library_categories`：资源库分类
- `library_items`：篇目/作品条目
- `library_tags`：可筛选维度（作者/题材/流派/朝代/文体/典籍等）
- `library_item_tags`：条目与标签的多对多关系

## 四、接口说明（Flask）

后端文件：`api.py`

- `GET /api/library/categories`
  - 返回分类列表（slug/name/description）

- `GET /api/library/<category_slug>/facets`
  - 返回该分类下可用筛选维度（作者/题材/流派/朝代/文体/典籍等）

- `GET /api/library/<category_slug>/items?tag_type=&tag_name=&q=&limit=`
  - 返回条目列表
  - `tag_type + tag_name` 同时提供时按标签筛选
  - `q` 支持在 标题/作者/名句/段落/出处 里模糊检索

- `GET /api/library/item/<item_id>`
  - 返回单条条目详情

## 五、前端展示说明

模板：`templates/second.html`

- 右侧新增“资源库”卡片（选择分类 → 选择筛选维度 → 输入关键词 → 列表展示名句/段落/出处）。
- 数据完全来自后端 `/api/library/*`，页面无需手动改静态 HTML 来扩展内容。

## 六、运行方式

1. 安装依赖（如你原项目已能运行可跳过）
2. 确保已配置 `DASHSCOPE_API_KEY`
3. 启动：

```bash
python api.py
```

4. 访问：
   - 首页：`/`
   - 资源库接口示例：`/api/library/categories`

## 七、如何继续扩充资源库内容

在 `resource_library_db.py` 中扩展：

- 新增分类：修改 `_seed_categories`
- 新增筛选维度：修改 `_seed_tags`
- 新增条目：修改 `_seed_items`

条目字段建议：

- **诗/词**：`highlight` 放核心名句
- **古文**：`excerpt` 放核心段落（可 1~3 句）
- **四书五经**：`source` 写明“典籍·篇章”，`excerpt` 写思想摘要


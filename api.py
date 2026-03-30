import dashscope
from dashscope import Generation
import re
import os
from typing import Dict, List
from dotenv import load_dotenv
from flask import Flask, request, jsonify, render_template
import json

load_dotenv()

app = Flask(__name__, template_folder='templates')

# =========================
# 拓展：资源库（SQLite）初始化与接口
# =========================
from resource_library_db import (
    DEFAULT_DB_PATH,
    ensure_initialized,
    get_item as library_get_item,
    list_categories as library_list_categories,
    list_facets as library_list_facets,
    list_items as library_list_items,
)


class ClassicalChineseAnalyzer:
    """
    文言文语法分析器 - 安全版本
    """

    def __init__(self, model: str = 'qwen-max'):
        """
        初始化分析器 - 从环境变量获取API Key
        """
        # 从环境变量获取API Key
        api_key = os.getenv('DASHSCOPE_API_KEY')

        if not api_key:
            print("错误：未找到API Key")
            raise ValueError("请设置DASHSCOPE_API_KEY环境变量")
        # 检查API Key格式
        if not api_key.startswith('sk-'):
            print("警告：API Key格式可能不正确")

        dashscope.api_key = api_key
        self.model = model

        # 系统提示（已优化）
        self.system_prompt = """
您是一位精通古汉语的资深语言学家，专精于文言文断句、虚词解析、特殊句式识别与现代汉语翻译。您的分析必须基于严谨的学术规范，输出逻辑清晰、格式统一。

# 核心任务处理流程
请严格遵循以下步骤处理用户输入的文言文句子：

## 步骤一：断句与标点处理
1.  **标点状态判定**：首先检查输入文本。如果文本已包含任何现代标点符号（如，。！？；：），则判定为"原文已标点，无需断句"，直接跳过断句步骤。
2.  **无标点文本断句**：若文本为纯汉字、无任何标点，则需对其进行合理断句。断句依据应为古汉语的语法结构（如主谓宾关系）、虚词标志（如"者"、"也"、"乎"、"盖"、"夫"常位于句首或句尾）以及韵律节奏。断句后需添加规范的文言标点（如逗号、句号、分号）。
3.  **断句输出格式**：必须严格按以下格式输出：
    - 标注规范文言标点的完整文句：[添加了标点的完整句子]
    - 断句依据：[简要说明关键断句点与依据]

## 步骤二：虚词穷尽式分析
1.  **识别与列举**：逐一识别句中出现的所有虚词。常见文言虚词包括但不限于：之、乎、者、也、以、于、而、其、则、乃、且、焉、哉、矣、耳、乎、耶、欤等。
2.  **精细化解析**：对识别出的**每一个虚词**，按照以下三个维度进行精准解析：
    - **词性**：精确界定其词性（如：代词、副词、介词、连词、助词、语气词、叹词）。
    - **语法功能**：说明其在句中的具体语法作用（如：作定语、表示转折、引出对象、表判断、表疑问语气、提顿话题等）。
    - **现代对应**：提供其在当前语境下最贴切的现代汉语释义（如："的"、"他"、"比"、"对于"、"却"、"吗"、"啊"），如果无直接对应词，可解释其语法功能。

## 步骤三：句式结构诊断
1.  **句式类型判定**：根据文言语法规范，判断句子是否为特殊句式，包括：
    - 判断句（……者……也、乃、即、则、为、是、非等）
    - 被动句（见、于、为、为…所、被）
    - 省略句（省略主语、宾语、介词、谓语）
    - 倒装句：必须细分为以下三类：
        * 宾语前置（疑问代词作宾语前置、否定句代词宾语前置、之/是提宾）
        * 定语后置（中心词+之+定语+者、中心词+定语+者）
        * **状语后置**（也叫介词结构后置）：标志为"动 + 于/乎/以 + 宾"，如"于长沙"修饰"屈"，"于海曲"修饰"窜"。
    - 固定句式（如奈……何、如……何、无乃……乎、不亦……乎、得无……乎等）

2.  **结构解析**：若判定为特殊句式，需拆解句子成分，清晰解释其特殊结构的构成逻辑。例如状语后置句，应明确指出"介宾短语在动词后作状语，现代汉语应前置"。若不属于任何特殊句式，则标注为"一般陈述句"。

**特别注意**：
- 当看到"动词 + 于 + 名词"结构时，应优先判定为状语后置，除非上下文明确为被动句（"见……于"）或另有解释。
- 对偶、排比等修辞不影响句式类型判定，需根据语法结构独立判断。

## 步骤四：语序还原与翻译
1.  **现代语序**：此步骤为**纯字面调整**。仅对原句的词语顺序进行调整，以符合现代汉语的语法习惯（如将倒装的宾语、后置的状语或定语复位），**绝对不替换、不增删任何原文词语**。
2.  **现代翻译**：此步骤为**意译**。基于现代语序，在忠实于原文意思的前提下，通过替换词汇（如将古义换为今义）、补充省略成分等方式，将句子转化为流畅、自然、符合现代语境的表达。

# 输出格式与规范
您的最终输出必须严格遵循以下结构化格式，各部分清晰分离，**所有标题后的冒号均为中文全角符号（：）**，**禁止使用Markdown加粗标记（**）**：

【句子原文】
[用户输入的原始句子，不做任何修改]

【断句结果】
- 状态：[原文已标点，无需断句 / 已完成断句]
- 标注规范文言标点的完整文句：[若状态为"已完成断句"，则输出添加了现代标点后的完整句子；若为"原文已标点"，则此栏无需输出或直接复制原文]
- 断句依据：[若状态为"已完成断句"，在此简要说明关键断句点及理由；若为"原文已标点"，此栏可省略]

【虚词分析】
（请严格按照以下列表格式输出，每个虚词项独立一行，禁止使用项目符号或星号，每个虚词项前必须使用阿拉伯数字加点号如"1."）
1. [虚词]：词性为[精确词性]，功能是[语法功能]，此处可译为"[现代释义]"
2. [虚词]：词性为[精确词性]，功能是[语法功能]，此处可译为"[现代释义]"
...

【句式分析】
- 句式类型：[判断句/被动句/省略句/宾语前置/定语后置/状语后置/固定句式/一般陈述句，必须选择其一，不可空缺]
- 结构解析：[简要说明句子结构，如果是特殊句式，需拆解其构成]

【现代语序】
[仅调整词序后的句子，保留原文字词，不得增删或替换]

【现代翻译】
[意译后的通顺现代汉语]

# 关键执行原则
1.  **严谨区分**：严格区分"现代语序"与"现代翻译"两个步骤，前者是字面调序，不得增删字词；后者是语义翻译，可适当补充替换。
2.  **虚词穷尽**：必须分析句中出现的每一个虚词，不得遗漏。对于多义词，需结合上下文选择最准确的解释。
3.  **格式统一**：输出格式必须与上述模板完全一致，所有标题均为中文全角括号【】，冒号使用中文全角符号（：），虚词分析部分必须使用阿拉伯数字加点号编号，句式分析部分必须使用短横线（-）作为项目符号。
4.  **禁止加粗**：全文禁止使用Markdown加粗语法（**），保持纯文本格式。
5.  **断句明确**：断句结果部分必须包含"标注规范文言标点的完整文句"和"断句依据"两个子项，每个子项前使用短横线（-）标记。
6.  **翻译忠实**：忠于原意，不增删主观内容，不曲解语境。"""

        # 文言虚词数据库（用于验证和补充）
        self.function_words_db = {
            '之': {'pos': ['助词', '代词', '动词'],
                   'functions': ['定语标志', '取消句子独立性', '宾语前置标志', '代词']},
            '乎': {'pos': ['语气词', '介词'], 'functions': ['疑问语气', '感叹语气', '介词']},
            '者': {'pos': ['助词', '代词'], 'functions': ['提顿语气', '定语后置标志', '代词']},
            '也': {'pos': ['语气词'], 'functions': ['判断语气', '肯定语气', '句中停顿']},
            '矣': {'pos': ['语气词'], 'functions': ['陈述语气', '完成语气']},
            '焉': {'pos': ['兼词', '语气词'], 'functions': ['兼有"于之"', '句末语气']},
            '哉': {'pos': ['语气词'], 'functions': ['感叹语气', '疑问语气']},
            '邪': {'pos': ['语气词'], 'functions': ['疑问语气']},
            '耶': {'pos': ['语气词'], 'functions': ['疑问语气']},
            '与': {'pos': ['连词', '语气词', '介词'], 'functions': ['并列连接', '疑问语气', '介词']},
            '欤': {'pos': ['语气词'], 'functions': ['疑问语气']},
            '夫': {'pos': ['语气词', '代词'], 'functions': ['发语词', '远指代词']},
            '盖': {'pos': ['语气词'], 'functions': ['发语词']},
            '唯': {'pos': ['语气词'], 'functions': ['希望语气']},
            '其': {'pos': ['代词', '语气词', '助词'], 'functions': ['第三人称代词', '推测语气', '祈使语气']},
            '所': {'pos': ['助词'], 'functions': ['构成所字结构']},
            '以': {'pos': ['介词', '连词'], 'functions': ['表示工具/方式', '表示原因', '表示时间', '连接目的']},
            '而': {'pos': ['连词'], 'functions': ['顺承连接', '转折连接', '修饰连接', '假设连接']},
            '则': {'pos': ['连词'], 'functions': ['顺承连接', '条件连接', '对比连接']},
            '然': {'pos': ['连词', '代词'], 'functions': ['转折连接', '指示代词']},
            '虽': {'pos': ['连词'], 'functions': ['让步连接']},
            '故': {'pos': ['连词'], 'functions': ['因果连接']},
            '苟': {'pos': ['连词'], 'functions': ['假设连接']},
            '若': {'pos': ['连词', '代词'], 'functions': ['假设连接', '第二人称代词']},
            '乃': {'pos': ['副词', '代词'], 'functions': ['顺承副词', '判断副词', '第二人称代词']},
            '即': {'pos': ['副词', '连词'], 'functions': ['时间副词', '假设连词']},
            '既': {'pos': ['副词', '连词'], 'functions': ['时间副词', '并列连词']},
            '将': {'pos': ['副词', '助词'], 'functions': ['将来副词', '选择连词']},
            '且': {'pos': ['连词', '副词'], 'functions': ['并列连词', '让步连词', '时间副词']},
            '或': {'pos': ['代词', '副词'], 'functions': ['无定代词', '或许副词']},
            '莫': {'pos': ['代词', '副词'], 'functions': ['无定代词', '否定副词']},
            '毋': {'pos': ['副词'], 'functions': ['否定副词']},
            '勿': {'pos': ['副词'], 'functions': ['否定副词']},
            '弗': {'pos': ['副词'], 'functions': ['否定副词']},
            '非': {'pos': ['副词'], 'functions': ['否定副词']},
            '微': {'pos': ['副词', '连词'], 'functions': ['否定副词', '假设连词']},
        }

        # 特殊句式数据库
        self.sentence_patterns = {
            '判断句': ['者...也', '...者...', '...也', '乃', '即', '则', '为', '是', '非'],
            '被动句': ['见', '于', '为', '为...所', '被'],
            '宾语前置': ['何+动词', '疑问代词+动词', '否定句+代词宾语', '之/是+宾语提前'],
            '定语后置': ['中心词+之+定语+者', '中心词+定语+者'],
            '状语后置': ['动+于+宾', '动+以+宾', '动+乎+宾'],
            '省略句': ['省略主语', '省略宾语', '省略介词', '省略谓语'],
        }

    def analyze_sentence(self, sentence: str) -> str:
        """
        分析句子并返回格式化结果
        """
        prompt = f"请分析以下文言文句子：\n句子原文：{sentence}"

        try:
            response = Generation.call(
                model=self.model,
                messages=[
                    {'role': 'system', 'content': self.system_prompt},
                    {'role': 'user', 'content': prompt}
                ],
                temperature=0.1,
                max_tokens=3000,  # 增加token限制以适应更详细的分析
                result_format='text'
            )

            if response.status_code == 200:
                return response.output.text
            else:
                return f"请求失败: {response.code} - {response.message}"

        except Exception as e:
            return f"发生异常: {str(e)}"

    def parse_analysis_result(self, result: str) -> Dict:
        """
        解析分析结果，拆分出断句结果、虚词分析、句式分析、现代翻译
        """
        parsed_data = {
            'original_sentence': '',
            'punctuation_result': {
                'has_punctuation': False,
                'annotated_sentence': '',
                'punctuation_basis': ''
            },
            'function_words': '',
            'sentence_pattern': {
                'type': '',
                'analysis': '',
                'modern_order': ''
            },
            'modern_translation': ''
        }

        # ---------- 句子原文 ----------
        sentence_match = re.search(r'[【\[]?\s*句子原文\s*[】\]]?\s*\n?(.*?)(?=\n[【\[]|$)', result,
                                   re.DOTALL | re.IGNORECASE)
        if sentence_match:
            parsed_data['original_sentence'] = sentence_match.group(1).strip()
        else:
            lines = result.strip().split('\n')
            if lines:
                parsed_data['original_sentence'] = lines[0].strip()

        # ---------- 断句结果 ----------
        punctuation_section = re.search(r'[【\[]?\s*断句结果\s*[】\]]?\s*\n(.*?)(?=\n[【\[]|$)', result,
                                        re.DOTALL | re.IGNORECASE)
        if punctuation_section:
            punctuation_content = punctuation_section.group(1).strip()
            if "原文已标点" in punctuation_content or "无需断句" in punctuation_content:
                parsed_data['punctuation_result']['has_punctuation'] = True
                parsed_data['punctuation_result']['annotated_sentence'] = "原文已标点，无需断句"
                parsed_data['punctuation_result']['punctuation_basis'] = ""
            else:
                parsed_data['punctuation_result']['has_punctuation'] = False
                # 提取标注文句
                annotated_match = None
                patterns = [
                    r'[＊*]*\s*标注规范文言标点的完整文句\s*[：:]\s*(.*?)(?=\n[＊*]?\s*断句依据|$)',
                    r'[＊*]*\s*标注文言标点的完整文句\s*[：:]\s*(.*?)(?=\n[＊*]?\s*断句依据|$)',
                    r'[＊*]*\s*文句\s*[：:]\s*(.*?)(?=\n[＊*]?\s*断句依据|$)',
                    r'[＊*]*\s*完整文句\s*[：:]\s*(.*?)(?=\n|$)',
                ]
                for pattern in patterns:
                    annotated_match = re.search(pattern, punctuation_content, re.DOTALL)
                    if annotated_match:
                        parsed_data['punctuation_result']['annotated_sentence'] = annotated_match.group(1).strip()
                        break
                if not parsed_data['punctuation_result']['annotated_sentence']:
                    basis_split = re.split(r'[＊*]?\s*断句依据\s*[：:]', punctuation_content)
                    if len(basis_split) > 1:
                        annotated_sentence = basis_split[0].strip()
                        annotated_sentence = re.sub(r'[（(].*?[）)]', '', annotated_sentence).strip()
                        parsed_data['punctuation_result']['annotated_sentence'] = annotated_sentence
                # 提取断句依据
                basis_match = re.search(r'[＊*]?\s*断句依据\s*[：:]\s*(.*?)(?=\n[＊*]?\s*|$)', punctuation_content,
                                        re.DOTALL)
                if basis_match:
                    parsed_data['punctuation_result']['punctuation_basis'] = basis_match.group(1).strip()

        # ---------- 虚词分析 ----------
        func_word_match = re.search(r'[【\[]?\s*虚词分析\s*[】\]]?\s*\n?(.*?)(?=\n[【\[]|$)', result,
                                    re.DOTALL | re.IGNORECASE)
        if func_word_match:
            parsed_data['function_words'] = func_word_match.group(1).strip()
        else:
            func_word_match2 = re.search(r'虚词分析[：:]\s*\n?(.*?)(?=\n[【\[]|\n句式分析|$)', result,
                                         re.DOTALL | re.IGNORECASE)
            if func_word_match2:
                parsed_data['function_words'] = func_word_match2.group(1).strip()

        # ---------- 句式分析提取（直接全局匹配，兼容性更强）----------
        # 提取【句式分析】区块内容
        section_match = re.search(r'【句式分析】(.*?)(?=【|$)', result, re.DOTALL | re.IGNORECASE)
        if section_match:
            section_content = section_match.group(1).strip()
            # 在区块内提取句式类型
            type_match = re.search(r'句式类型\s*[：:]\s*(.*?)(?=\n|$)', section_content, re.IGNORECASE)
            if type_match:
                parsed_data['sentence_pattern']['type'] = type_match.group(1).strip()
            # 在区块内提取结构解析
            analysis_match = re.search(r'结构解析\s*[：:]\s*(.*?)(?=\n|$)', section_content, re.IGNORECASE)
            if analysis_match:
                parsed_data['sentence_pattern']['analysis'] = analysis_match.group(1).strip()
        else:
            # 兜底：全局搜索（兼容没有【】的情况）
            type_global = re.search(r'句式类型\s*[：:]\s*(.*?)(?=\n|$)', result, re.IGNORECASE)
            if type_global:
                parsed_data['sentence_pattern']['type'] = type_global.group(1).strip()
            analysis_global = re.search(r'结构解析\s*[：:]\s*(.*?)(?=\n|$)', result, re.IGNORECASE)
            if analysis_global:
                parsed_data['sentence_pattern']['analysis'] = analysis_global.group(1).strip()

        # 提取【现代语序】区块
        order_section = re.search(r'【现代语序】(.*?)(?=【|$)', result, re.DOTALL | re.IGNORECASE)
        if order_section:
            parsed_data['sentence_pattern']['modern_order'] = order_section.group(1).strip()
        else:
            order_global = re.search(r'现代语序\s*[：:]\s*(.*?)(?=\n|$)', result, re.DOTALL | re.IGNORECASE)
            if order_global:
                parsed_data['sentence_pattern']['modern_order'] = order_global.group(1).strip()

        # ---------- 现代翻译 ----------
        translation_match = re.search(r'[【\[]?\s*现代翻译\s*[】\]]?\s*[：:]\s*(.*?)(?=\n[【\[]|$)', result,
                                      re.DOTALL | re.IGNORECASE)
        if translation_match:
            parsed_data['modern_translation'] = translation_match.group(1).strip()
        else:
            translation_patterns = [
                r'现代翻译[：:]\s*(.+?)(?=\n[【\[]|\n\n|$)',
                r'现代翻译[：:]\s*(.+?)$',
                r'[【\[]现代翻译[】\]]\s*(.+?)$',
            ]
            for pattern in translation_patterns:
                translation_match = re.search(pattern, result, re.DOTALL | re.IGNORECASE)
                if translation_match:
                    parsed_data['modern_translation'] = translation_match.group(1).strip()
                    break

        # 后备：在段落中查找包含“翻译”的内容
        if not parsed_data['modern_translation']:
            paragraphs = result.split('\n\n')
            for para in paragraphs:
                if '翻译' in para and len(para) < 200:
                    parts = re.split(r'[：:]', para)
                    if len(parts) > 1:
                        candidate = parts[-1].strip()
                        if candidate and len(candidate) < 100:
                            parsed_data['modern_translation'] = candidate
                            break

        return parsed_data

# 初始化分析器
try:
    analyzer = ClassicalChineseAnalyzer()
except ValueError as e:
    print(f"分析器初始化失败: {e}")
    analyzer = None

# =========================
# 拓展：资源库初始化（只执行一次）
# 在 debug 模式下，Werkzeug 会启动两个进程，这里只在主进程中执行初始化，避免锁冲突
# =========================
if not os.environ.get('WERKZEUG_RUN_MAIN') or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
    # 仅在主进程（非 reloader 子进程）中执行，或非 debug 模式直接执行
    try:
        ensure_initialized(DEFAULT_DB_PATH)
        print("资源库数据库初始化完成")
    except Exception as e:
        print(f"资源库数据库初始化失败: {e}")


# 前端页面路由
@app.route('/')
def index():
    return render_template('second.html')


@app.route('/resource-library')
def resource_library_page():
    return render_template('resource-library.html')


# 分析接口
@app.route('/analyze', methods=['POST'])
def analyze():
    if not analyzer:
        return jsonify({'error': '分析器初始化失败，请检查API Key'}), 500

    data = request.get_json()
    sentence = data.get('sentence', '').strip()
    if not sentence:
        return jsonify({'error': '请输入需要分析的文言文句子'}), 400

    # 调用分析方法
    raw_result = analyzer.analyze_sentence(sentence)
    if '请求失败' in raw_result or '发生异常' in raw_result:
        return jsonify({'error': raw_result}), 500

    # 解析结果
    parsed_result = analyzer.parse_analysis_result(raw_result)
    return jsonify({
        'success': True,
        'raw_result': raw_result,  # 保留原始完整结果用于调试
        'data': parsed_result
    })


# 新增：返回原始完整结果的接口（用于调试）
@app.route('/raw_analyze', methods=['POST'])
def raw_analyze():
    if not analyzer:
        return jsonify({'error': '分析器初始化失败，请检查API Key'}), 500

    data = request.get_json()
    sentence = data.get('sentence', '').strip()
    if not sentence:
        return jsonify({'error': '请输入需要分析的文言文句子'}), 400

    # 调用分析方法
    raw_result = analyzer.analyze_sentence(sentence)
    return jsonify({
        'success': True,
        'raw_result': raw_result
    })


# =========================
# 拓展：资源库API（分类/明细/检索）
# =========================
@app.route('/api/library/categories', methods=['GET'])
def library_categories():
    return jsonify({'success': True, 'data': library_list_categories(DEFAULT_DB_PATH)})


@app.route('/api/library/<category_slug>/facets', methods=['GET'])
def library_facets(category_slug: str):
    return jsonify({'success': True, 'data': library_list_facets(DEFAULT_DB_PATH, category_slug)})


@app.route('/api/library/<category_slug>/items', methods=['GET'])
def library_items(category_slug: str):
    tag_type = request.args.get('tag_type')
    tag_name = request.args.get('tag_name')
    q = request.args.get('q')
    try:
        limit = int(request.args.get('limit', '200'))
    except ValueError:
        limit = 200

    data = library_list_items(
        DEFAULT_DB_PATH,
        category_slug=category_slug,
        tag_type=tag_type,
        tag_name=tag_name,
        q=q,
        limit=max(1, min(limit, 500)),
    )
    return jsonify({'success': True, 'data': data})


@app.route('/api/library/item/<int:item_id>', methods=['GET'])
def library_item(item_id: int):
    item = library_get_item(DEFAULT_DB_PATH, item_id)
    if not item:
        return jsonify({'success': False, 'error': '未找到该条目'}), 404
    return jsonify({'success': True, 'data': item})


if __name__ == "__main__":
    # 创建templates文件夹（如果不存在）
    if not os.path.exists('templates'):
        os.makedirs('templates')

    app.run(debug=True, host='0.0.0.0', port=5000)

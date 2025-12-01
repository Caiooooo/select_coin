#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
币种筛选脚本
筛选市值30亿以下的优秀币种
"""

import requests
import json
import os
import time
from typing import List, Dict, Tuple, Optional
from datetime import datetime


class CoinSelector:
    """币种筛选器"""

    # 市值上限（美元）：1-100亿
    MARKET_CAP_MAX = 10_000_000_000
    MARKET_CAP_MIN = 100_000_000

    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'CoinSelector/1.0'
        })
        # 加载名称/符号黑名单关键字
        self.blacklist_keywords = self._load_blacklist_keywords()

    def _load_blacklist_keywords(self) -> List[str]:
        """
        从本地文件加载黑名单关键字（名称或符号的一部分）
        每行一个关键字，大小写不敏感，支持注释行（以#开头）
        """
        keywords: List[str] = []
        try:
            with open("blacklist.txt", "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    keywords.append(line.upper())
        except FileNotFoundError:
            # 没有黑名单文件时忽略
            pass
        return keywords

    def _load_coins_from_local_pages(self, limit: int) -> List[Dict]:
        """
        从本地分页文件 coin_pages/page_*.json 汇总币种数据
        :param limit: 返回数量限制
        """
        pages_dir = "coin_pages"
        if not os.path.isdir(pages_dir):
            print(f"本地分页目录不存在：{pages_dir}，无法从本地读取币种数据")
            return []

        # 收集所有 page_*.json 文件，并按页码升序排序
        page_files = []
        for name in os.listdir(pages_dir):
            if not name.startswith("page_") or not name.endswith(".json"):
                continue
            try:
                page_num = int(name[len("page_"):-len(".json")])
            except ValueError:
                continue
            page_files.append((page_num, os.path.join(pages_dir, name)))

        if not page_files:
            print(f"目录 {pages_dir} 中没有找到任何 page_*.json 文件")
            return []

        page_files.sort(key=lambda x: x[0])

        coins: List[Dict] = []
        for page_num, file_path in page_files:
            if len(coins) >= limit:
                break
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # fetch_coin_infos.py 中的结构为 {"page": x, "coins": [...]}
                page_coins = data.get("coins", [])
                if not isinstance(page_coins, list):
                    print(f"文件 {file_path} 中的 coins 字段不是列表，跳过该文件")
                    continue
                coins.extend(page_coins)
                print(
                    f"已从本地文件 {file_path} 读取 {len(page_coins)} 条记录，累计 {len(coins)} 条")
            except Exception as e:
                print(f"读取本地分页文件失败：{file_path}，错误：{e}")

        return coins[:limit]

    def get_all_coins(self, limit: int = 250000) -> List[Dict]:
        """
        获取所有币种数据（只从本地读取，不再访问网络）
        优先使用 all_coins_cache.json，本地缓存不存在或读取失败时，
        再从 coin_pages/page_*.json 汇总生成，并写入缓存。
        :param limit: 返回数量限制
        :return: 币种列表
        """
        cache_file = "all_coins_cache.json"

        # 1. 优先从本地缓存读取
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cache_data = json.load(f)
                coins = cache_data.get("coins", [])
                if coins:
                    print(f"从本地缓存读取币种数据，共 {len(coins)} 个")
                    return coins[:limit]
                else:
                    print(f"本地缓存文件 {cache_file} 中没有有效的 coins 数据，尝试从分页文件重新生成")
            except Exception as e:
                print(f"读取本地缓存失败，将从分页文件重新生成: {e}")

        # 2. 缓存无效或不存在时，从本地分页文件汇总
        print("开始从本地分页文件 coin_pages/page_*.json 汇总币种数据（不访问网络）...")
        coins = self._load_coins_from_local_pages(limit=limit)

        if not coins:
            print("获取币种数据失败：未能从本地缓存或分页文件中获取任何数据")
            return []

        # 3. 成功汇总后，写入缓存文件，方便下次快速读取
        try:
            cache_data = {
                "fetch_time": datetime.now().isoformat(),
                "total_count": len(coins),
                "coins": coins,
            }
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            print(f"币种数据已从本地分页汇总并缓存到文件: {cache_file}（共 {len(coins)} 个）")
        except Exception as e:
            print(f"保存本地缓存失败（但不影响继续使用内存中的数据）: {e}")

        return coins[:limit]

    def calculate_score(self, coin: Dict) -> float:
        """
        计算币种评分
        评分标准：
        1. 交易量/市值比率（流动性指标）
        2. 24小时价格变化（正分）
        3. 7天价格变化（正分）
        4. 市值排名（排名越靠前分数越高）
        """
        score = 0.0

        market_cap = coin.get('market_cap', 0) or 0
        total_volume = coin.get('total_volume', 0) or 0
        price_change_24h = coin.get('price_change_percentage_24h', 0) or 0
        price_change_7d = coin.get('price_change_percentage_7d', 0) or 0
        market_cap_rank = coin.get('market_cap_rank', 999) or 999

        # 1. 交易量/市值比率（流动性指标，权重40%）
        if market_cap > 0:
            volume_ratio = (total_volume / market_cap) * 100
            # 比率越高越好，最高10分
            volume_score = min(volume_ratio / 5, 10) * 0.4
            score += volume_score

        # 2. 24小时价格变化（权重30%）
        if price_change_24h > 0:
            # 正增长加分，最高10分
            change_24h_score = min(price_change_24h / 10, 10) * 0.3
            score += change_24h_score

        # 3. 7天价格变化（权重20%）
        if price_change_7d > 0:
            # 正增长加分，最高10分
            change_7d_score = min(price_change_7d / 20, 10) * 0.2
            score += change_7d_score

        # 4. 市值排名（权重10%）
        # 排名越靠前分数越高
        if market_cap_rank <= 100:
            rank_score = (101 - market_cap_rank) / 10 * 0.1
            score += rank_score

        return score

    def is_excellent_coin(self, coin: Dict) -> Tuple[bool, Optional[str]]:
        """
        判断是否为优秀币种
        :param coin: 币种数据
        :return: (是否为优秀币种, 过滤原因) 如果通过则返回 (True, None)，否则返回 (False, 原因)
        """
        market_cap = coin.get('market_cap', 0) or 0
        total_volume = coin.get('total_volume', 0) or 0
        price_change_percentage_24h = coin.get(
            'price_change_percentage_24h', 0) or 0
        # 获取币种名称和符号
        name = coin.get('name', '').upper()
        symbol = coin.get('symbol', '').upper()

        # 黑名单：名称或符号命中任一关键字则直接排除
        for kw in self.blacklist_keywords:
            if kw == name or symbol == kw:
                return (False, f"命中黑名单关键字: {kw}")

        # 额外强规则：排除名称或符号中包含 USD 的币种（稳定币等）
        if 'USD' in name or 'USD' in symbol or 'USDT' in name or 'STAKED' in name or 'STAKED' in symbol:
            return (False, "名称或符号包含 USD/USDT/STAKED（稳定币等）")

        if 'STAKED' in name or 'STAKED' in symbol:
            return (False, "名称或符号包含 STAKED（质押）")

        # 基本条件：
        # 1. 市值在30亿以下
        if market_cap > self.MARKET_CAP_MAX:
            return (False, f"市值超过上限: ${market_cap/1e9:.2f}B > ${self.MARKET_CAP_MAX/1e9:.2f}B")

        # 2. 市值必须大于30M
        if market_cap < self.MARKET_CAP_MIN:
            return (False, f"市值低于下限: ${market_cap/1e6:.2f}M < ${self.MARKET_CAP_MIN/1e6:.2f}M")

        # 2. 市值必须大于0
        if market_cap <= 0:
            return (False, "市值 <= 0")

        # 3. 有足够交易量（流动性要求：至少 1M）
        if total_volume < 600_000:
            return (False, f"交易量不足: ${total_volume/1e6:.2f}M < $0.6M")

        # 4. 交易量/市值比率 >= 2%（确保有一定流动性）
        # volume_ratio = (total_volume / market_cap) * \
        #     100 if market_cap > 0 else 0
        # if volume_ratio < 2:
        #     return False

        # 5. 24小时跌幅不超过20%（避免暴跌币）
        if price_change_percentage_24h < -20:
            return (False, f"24小时跌幅过大: {price_change_percentage_24h:.2f}% < -20%")

        # 6. ATH筛选：ATH时间在2023-01-01之前，或者ath_change_percentage小于-98%的币种要筛选掉
        ath_date_str = coin.get('ath_date')
        ath_change_percentage = coin.get('ath_change_percentage')

        if ath_date_str:
            try:
                # 解析ATH日期（ISO 8601格式）
                ath_date = datetime.fromisoformat(
                    ath_date_str.replace('Z', '+00:00'))
                cutoff_date = datetime.fromisoformat(
                    '2023-01-01T00:00:00+00:00')
                # ATH时间在2023-01-01之前, 且暴跌了95%以上，筛选掉
                if ath_date < cutoff_date and (ath_change_percentage is None or ath_change_percentage < -95):
                    return (False, f"ATH在2023-01-01之前且跌幅>95%: ATH日期={ath_date_str}, 跌幅={ath_change_percentage}%")
            except (ValueError, AttributeError):
                # 日期解析失败时忽略此检查
                pass

        # ath_change_percentage小于-98%，筛选掉
        if ath_change_percentage is not None and ath_change_percentage < -98:
            return (False, f"ATH跌幅过大: {ath_change_percentage:.2f}% < -98%")

        return (True, None)

    def filter_coins(self, limit: int = 250000) -> Tuple[List[Dict], List[Dict]]:
        """
        筛选优秀币种
        :param limit: 获取币种数量限制
        :return: (筛选后的币种列表（按评分排序）, 被过滤的币种列表（包含过滤原因）)
        """
        print(f"正在获取币种数据（前{limit}名）...")
        all_coins = self.get_all_coins(limit)

        if not all_coins:
            print("未获取到币种数据")
            return ([], [])

        print(f"共获取 {len(all_coins)} 个币种")
        print(f"开始筛选市值30亿以下的优秀币种...")

        # 筛选符合条件的币种
        filtered_coins = []
        filtered_out_coins = []
        for coin in all_coins:
            is_excellent, reason = self.is_excellent_coin(coin)
            if is_excellent:
                score = self.calculate_score(coin)
                coin['excellent_score'] = score
                filtered_coins.append(coin)
            else:
                # 记录被过滤的币种及其原因
                filtered_coin = {
                    'name': coin.get('name'),
                    'symbol': coin.get('symbol'),
                    'market_cap': coin.get('market_cap'),
                    'market_cap_rank': coin.get('market_cap_rank'),
                    'total_volume': coin.get('total_volume'),
                    'price_change_percentage_24h': coin.get('price_change_percentage_24h'),
                    'filter_reason': reason
                }
                filtered_out_coins.append(filtered_coin)

        # 按评分降序排序
        filtered_coins.sort(key=lambda x: x.get(
            'excellent_score', 0), reverse=True)

        return (filtered_coins, filtered_out_coins)

    def format_output(self, coin: Dict) -> str:
        """格式化输出币种信息"""
        name = coin.get('name', 'N/A')
        symbol = coin.get('symbol', 'N/A').upper()
        market_cap = coin.get('market_cap', 0) or 0
        price = coin.get('current_price', 0) or 0
        volume_24h = coin.get('total_volume', 0) or 0
        change_24h = coin.get('price_change_percentage_24h', 0) or 0
        change_7d = coin.get('price_change_percentage_7d', 0) or 0
        rank = coin.get('market_cap_rank', 'N/A')
        score = coin.get('excellent_score', 0)

        # 格式化市值和交易量
        market_cap_str = f"${market_cap/1e9:.2f}B" if market_cap >= 1e9 else f"${market_cap/1e6:.2f}M"
        volume_str = f"${volume_24h/1e6:.2f}M" if volume_24h >= 1e6 else f"${volume_24h/1e3:.2f}K"

        return (
            f"排名: {rank:>4} | "
            f"{name:20} ({symbol:>6}) | "
            f"市值: {market_cap_str:>10} | "
            f"价格: ${price:>12.6f} | "
            f"24h涨跌: {change_24h:>6.2f}% | "
            f"7d涨跌: {change_7d:>6.2f}% | "
            f"24h交易量: {volume_str:>10} | "
            f"评分: {score:.2f}"
        )


def main():
    """主函数"""
    print("=" * 150)
    print("币种筛选工具 - 筛选市值30亿以下的优秀币种")
    print("=" * 150)
    print()

    selector = CoinSelector()

    # 获取更多币种数据以提高筛选范围（通过分页突破单页 250 个限制）
    filtered_coins, filtered_out_coins = selector.filter_coins(limit=250000)

    if not filtered_coins:
        print("\n未找到符合条件的币种")
        # 即使没有符合条件的币种，也保存被过滤的币种信息
        if filtered_out_coins:
            debug_file = "debug_filtered_coins.json"
            debug_data = {
                'filter_time': datetime.now().isoformat(),
                'total_filtered_out': len(filtered_out_coins),
                'filtered_coins': filtered_out_coins
            }
            with open(debug_file, 'w', encoding='utf-8') as f:
                json.dump(debug_data, f, ensure_ascii=False, indent=2)
            print(
                f"被过滤的币种信息已保存到: {debug_file} (共 {len(filtered_out_coins)} 个)")
        return

    print(f"\n找到 {len(filtered_coins)} 个符合条件的优秀币种：")
    print("=" * 150)
    print(f"{'排名':<6} {'币种名称':<25} {'市值':<15} {'价格':<15} {'24h涨跌':<10} {'7d涨跌':<10} {'24h交易量':<15} {'评分':<8}")
    print("-" * 150)

    # 输出前20个
    top_n = min(20, len(filtered_coins))
    for i, coin in enumerate(filtered_coins[:top_n], 1):
        print(selector.format_output(coin))

    if len(filtered_coins) > top_n:
        print(f"\n... 还有 {len(filtered_coins) - top_n} 个币种未显示")

    print("\n" + "=" * 150)
    print(f"筛选完成！共找到 {len(filtered_coins)} 个优秀币种")
    print(f"筛选时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 保存完整结果到 JSON 文件
    output_file = "filtered_coins.json"
    output_data = {
        'filter_time': datetime.now().isoformat(),
        'total_count': len(filtered_coins),
        'criteria': {
            'max_market_cap': selector.MARKET_CAP_MAX,
            'min_market_cap': selector.MARKET_CAP_MIN,
        },
        'coins': filtered_coins
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"结果已保存到: {output_file}")

    # 额外保存一个“简易版本”，只保留最关键字段，方便快速查看 / 导入
    simple_file = "filtered_coins_simple.json"
    simple_coins = []
    for c in filtered_coins:
        simple_coins.append({
            "symbol": c.get("symbol"),
            "name": c.get("name"),
            "market_cap": c.get("market_cap"),
            "score": c.get("excellent_score"),
        })

    with open(simple_file, 'w', encoding='utf-8') as f:
        json.dump(simple_coins, f, ensure_ascii=False, indent=2)

    print(f"简易代币列表已保存到: {simple_file}")

    # 保存被过滤的币种信息到调试文件
    if filtered_out_coins:
        debug_file = "debug_filtered_coins.json"
        debug_data = {
            'filter_time': datetime.now().isoformat(),
            'total_filtered_out': len(filtered_out_coins),
            'total_passed': len(filtered_coins),
            'filtered_coins': filtered_out_coins
        }
        with open(debug_file, 'w', encoding='utf-8') as f:
            json.dump(debug_data, f, ensure_ascii=False, indent=2)
        print(f"被过滤的币种信息已保存到: {debug_file} (共 {len(filtered_out_coins)} 个)")


if __name__ == "__main__":
    main()

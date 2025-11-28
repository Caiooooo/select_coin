#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分页抓取 CoinGecko 币种信息，并将每一页结果缓存到本地文件。

功能特点：
- 每次请求一页，结果保存为单独的 JSON 文件（例如：coin_pages/page_1.json）
- 使用状态文件（fetch_state.json）记录「已经成功抓取到的最后一页」
- 下次运行时会自动从「最后一页的下一页」开始续抓，避免重复请求
"""

import os
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

import requests


BASE_URL = "https://api.coingecko.com/api/v3"
PER_PAGE = 250  # CoinGecko 单页最大限制

STATE_FILE = "fetch_state.json"        # 记录已抓取到的最后一页
OUTPUT_DIR = "coin_pages"              # 每页结果保存目录
MAX_RETRIES_PER_PAGE = 3              # 每页最大重试次数


session = requests.Session()
session.headers.update(
    {
        "Accept": "application/json",
        "User-Agent": "CoinSelector/Fetcher/1.0",
    }
)


def load_state() -> Dict[str, Any]:
    """从本地状态文件读取抓取进度。"""
    if not os.path.exists(STATE_FILE):
        return {"last_page": 0, "updated_at": None}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"last_page": 0, "updated_at": None}
        data.setdefault("last_page", 0)
        data.setdefault("updated_at", None)
        return data
    except Exception as e:
        print(f"读取状态文件 {STATE_FILE} 失败，将从第 1 页开始: {e}")
        return {"last_page": 0, "updated_at": None}


def save_state(last_page: int) -> None:
    """保存最新抓取到的页码。"""
    state = {
        "last_page": int(last_page),
        "updated_at": datetime.now().isoformat(),
    }
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        print(f"抓取进度已更新：last_page = {last_page}（写入 {STATE_FILE}）")
    except Exception as e:
        print(f"写入状态文件 {STATE_FILE} 失败: {e}")


def fetch_page(page: int) -> List[Dict[str, Any]]:
    """抓取指定页的数据，带简单重试和 429 限流处理。"""
    url = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": PER_PAGE,
        "page": page,
        "sparkline": False,
        "price_change_percentage": "24h,7d",
    }

    retries = 0
    while retries < MAX_RETRIES_PER_PAGE:
        try:
            resp = session.get(url, params=params, timeout=10)

            if resp.status_code == 429:
                wait_seconds = 60
                print(
                    f"[第 {page} 页] 请求过于频繁（429），休眠 {wait_seconds} 秒后重试 "
                    f"(第 {retries + 1} 次)..."
                )
                time.sleep(wait_seconds)
                retries += 1
                continue

            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                print(f"[第 {page} 页] 返回数据不是列表，实际类型：{type(data)}")
                return []
            return data
        except Exception as e:
            retries += 1
            print(f"[第 {page} 页] 抓取失败（第 {retries} 次尝试）: {e}")
            time.sleep(5)

    print(f"[第 {page} 页] 多次重试失败，放弃该页")
    return []


def save_page_data(page: int, coins: List[Dict[str, Any]]) -> None:
    """将某一页的数据保存到本地文件。"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_path = os.path.join(OUTPUT_DIR, f"page_{page}.json")

    payload = {
        "page": page,
        "per_page": PER_PAGE,
        "count": len(coins),
        "fetch_time": datetime.now().isoformat(),
        "coins": coins,
    }

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"[第 {page} 页] 数据已保存到文件：{file_path}（{len(coins)} 条）")
    except Exception as e:
        print(f"[第 {page} 页] 保存到文件失败：{file_path}，错误：{e}")


def fetch_coin_infos(
    max_pages: Optional[int] = None,
    start_page: Optional[int] = None,
) -> int:
    """
    逐页抓取币种信息，并缓存到本地。

    :param max_pages: 本次最多抓取多少页；为 None 时一直抓到接口无更多数据为止
    :param start_page: 手动指定起始页；为 None 时自动从状态文件中记录的下一页开始
    :return: 本次抓取到的最后一页页码（如果没有成功抓取任何页，则返回 0）
    """
    state = load_state()

    if start_page is not None and start_page > 0:
        current_page = int(start_page)
        print(
            f"从指定的起始页开始抓取：第 {current_page} 页 "
            f"(忽略状态文件的 last_page={state.get('last_page')})"
        )
    else:
        last_page = int(state.get("last_page", 0) or 0)
        current_page = last_page + 1 if last_page >= 0 else 1
        if last_page > 0:
            print(
                f"从状态文件恢复进度：已抓取到第 {last_page} 页，"
                f"本次将从第 {current_page} 页开始"
            )
        else:
            print("状态文件未记录进度，本次将从第 1 页开始抓取")

    fetched_pages = 0
    last_fetched_page = 0

    while True:
        if max_pages is not None and fetched_pages >= max_pages:
            print(
                f"本次已抓取 {fetched_pages} 页，达到 max_pages={max_pages} 的限制，停止抓取"
            )
            break

        print("=" * 80)
        print(f"开始抓取第 {current_page} 页 ...")
        coins = fetch_page(current_page)

        if not coins:
            print(f"第 {current_page} 页没有数据（或抓取失败），停止后续抓取")
            break

        save_page_data(current_page, coins)

        # 只有当当前页成功抓取并成功保存时，才更新状态文件
        save_state(current_page)

        fetched_pages += 1
        last_fetched_page = current_page
        current_page += 1

        # 页与页之间稍微休眠，降低被限流概率
        time.sleep(1)

        # 如果这一页返回的数据少于 PER_PAGE，则说明已经是最后一页
        if len(coins) < PER_PAGE:
            print(
                f"第 {last_fetched_page} 页返回数据量为 {len(coins)} (< {PER_PAGE})，"
                "推测已经到达最后一页，停止抓取"
            )
            break

    if last_fetched_page > 0:
        print(
            f"本次抓取结束：共抓取 {fetched_pages} 页，"
            f"最后一页为第 {last_fetched_page} 页"
        )
    else:
        print("本次未成功抓取任何页")

    return last_fetched_page


def main():
    """
    命令行入口：
    - 默认：从状态文件记录的下一页开始，抓到接口无更多数据为止
    - 可选：通过环境变量配置最大页数 / 起始页
        - FETCH_MAX_PAGES：本次最多抓取多少页（整数）
        - FETCH_START_PAGE：手动指定起始页（整数，优先级高于状态文件）
    """
    max_pages_env = os.getenv("FETCH_MAX_PAGES")
    start_page_env = os.getenv("FETCH_START_PAGE")

    max_pages: Optional[int] = None
    start_page: Optional[int] = None

    if max_pages_env:
        try:
            max_pages = int(max_pages_env)
        except ValueError:
            print(f"环境变量 FETCH_MAX_PAGES 无法解析为整数：{max_pages_env}")

    if start_page_env:
        try:
            start_page = int(start_page_env)
        except ValueError:
            print(f"环境变量 FETCH_START_PAGE 无法解析为整数：{start_page_env}")

    print("=" * 80)
    print("开始执行分页抓取任务（fetch_coin_infos）")
    print(f"- 配置 max_pages    = {max_pages_env!r}")
    print(f"- 配置 start_page   = {start_page_env!r}")
    print("=" * 80)

    fetch_coin_infos(max_pages=max_pages, start_page=start_page)


if __name__ == "__main__":
    main()

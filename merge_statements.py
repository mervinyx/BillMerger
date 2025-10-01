from __future__ import annotations

from io import BytesIO
from typing import BinaryIO, Iterable

import pandas as pd


FULL2HALF = str.maketrans({"（": "(", "）": ")", "\u3000": " "})
REQUIRED_COLUMNS: Iterable[str] = (
    "来源",
    "交易时间",
    "交易创建时间",
    "付款时间",
    "最近修改时间",
    "交易来源地",
    "交易类型",
    "交易对方",
    "商品名称",
    "金额(元)",
    "收/支",
    "交易状态",
    "服务费(元)",
    "成功退款(元)",
    "支付方式",
    "资金状态",
    "交易号",
    "商家订单号",
    "备注",
)


def merge_statements(alipay_file: BinaryIO, wechat_file: BinaryIO) -> BytesIO:
    """Merge Alipay CSV and WeChat Excel statements into a unified CSV."""

    alipay_df = _load_alipay(alipay_file)
    wechat_df = _load_wechat(wechat_file)

    aligned = pd.concat([alipay_df, wechat_df], ignore_index=True)

    sort_key = pd.Series([pd.NA] * len(aligned))
    for column in ("付款时间", "交易时间", "交易创建时间", "最近修改时间"):
        sort_key = sort_key.fillna(aligned[column].replace("", pd.NA))

    aligned["_排序时间"] = pd.to_datetime(sort_key, errors="coerce")
    aligned = (
        aligned.sort_values("_排序时间", kind="mergesort")
        .drop(columns=["_排序时间"])
        .fillna("")
    )

    output = BytesIO()
    aligned.to_csv(output, index=False, encoding="utf-8-sig")
    output.seek(0)
    return output


def _load_alipay(upload: BinaryIO) -> pd.DataFrame:
    upload.seek(0)
    raw_bytes = upload.read()
    if not raw_bytes:
        raise ValueError("支付宝账单为空，请重新上传")

    def read_with(encodings: Iterable[str]) -> pd.DataFrame:
        last_error: Exception | None = None
        for enc in encodings:
            try:
                return pd.read_csv(
                    BytesIO(raw_bytes), encoding=enc, skiprows=4, dtype=str
                )
            except UnicodeDecodeError as err:
                last_error = err
        if last_error:
            raise ValueError("无法识别支付宝账单编码，请确认文件格式无误") from last_error
        raise ValueError("无法解析支付宝账单编码")

    alipay = read_with(("gbk", "gb18030", "utf-8-sig", "utf-8"))
    upload.seek(0)
    if "Unnamed: 16" in alipay.columns and alipay["Unnamed: 16"].isna().all():
        alipay = alipay.drop(columns=["Unnamed: 16"])

    alipay.columns = [col.translate(FULL2HALF).strip() for col in alipay.columns]
    alipay = alipay[alipay["交易创建时间"].notna()].copy()

    for column in alipay.select_dtypes(include="object"):
        alipay[column] = (
            alipay[column]
            .str.replace("\t", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
            .fillna("")
        )

    alipay.rename(columns={"类型": "交易类型", "金额(元)": "金额(元)"}, inplace=True)
    alipay["交易时间"] = alipay["付款时间"].fillna("")
    missing_mask = alipay["交易时间"] == ""
    alipay.loc[missing_mask, "交易时间"] = alipay.loc[missing_mask, "交易创建时间"].fillna("")
    if "支付方式" not in alipay.columns:
        alipay["支付方式"] = ""
    alipay["来源"] = "支付宝"

    for column in REQUIRED_COLUMNS:
        if column not in alipay.columns:
            alipay[column] = ""

    return alipay.loc[:, REQUIRED_COLUMNS]


def _load_wechat(upload: BinaryIO) -> pd.DataFrame:
    upload.seek(0)
    wechat = pd.read_excel(upload, header=16, dtype=str)
    wechat = wechat.dropna(how="all")
    if "交易时间" not in wechat.columns:
        raise ValueError("未检测到有效的微信账单数据")

    wechat.rename(
        columns={
            "商品": "商品名称",
            "交易单号": "交易号",
            "商户单号": "商家订单号",
            "当前状态": "交易状态",
        },
        inplace=True,
    )

    for column in wechat.select_dtypes(include="object"):
        wechat[column] = (
            wechat[column]
            .str.replace("\t", "", regex=False)
            .str.replace("¥", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
            .fillna("")
        )

    wechat = wechat[wechat["交易时间"].notna()].copy()

    wechat["交易创建时间"] = wechat["交易时间"].fillna("")
    wechat["付款时间"] = wechat["交易时间"].fillna("")
    wechat["最近修改时间"] = wechat["交易时间"].fillna("")

    for column in ("交易来源地", "服务费(元)", "成功退款(元)", "资金状态"):
        wechat[column] = ""
    if "收/支" not in wechat.columns:
        wechat["收/支"] = ""
    if "支付方式" not in wechat.columns:
        wechat["支付方式"] = ""

    wechat["来源"] = "微信"

    for column in REQUIRED_COLUMNS:
        if column not in wechat.columns:
            wechat[column] = ""

    return wechat.loc[:, REQUIRED_COLUMNS]

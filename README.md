# 账单合并工具

一个简单的 Flask Web 应用，上传微信/支付宝账单后自动合并并导出统一的 CSV。

## 本地运行

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
flask --app app run
```

然后访问 http://127.0.0.1:5000 ，上传支付宝 CSV 与微信 Excel 文件即可下载合并结果。

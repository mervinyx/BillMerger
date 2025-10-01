from __future__ import annotations

import datetime as dt

from flask import Flask, render_template, request, send_file

from merge_statements import merge_statements

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024


@app.route("/", methods=["GET", "POST"])
def index():
    message = ""
    if request.method == "POST":
        alipay_file = request.files.get("alipay")
        wechat_file = request.files.get("wechat")

        if not alipay_file or alipay_file.filename == "":
            message = "请上传支付宝账单文件"
        elif not wechat_file or wechat_file.filename == "":
            message = "请上传微信账单文件"
        else:
            try:
                merged = merge_statements(alipay_file.stream, wechat_file.stream)
                timestamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")
                filename = f"merged-statements-{timestamp}.csv"
                return send_file(
                    merged,
                    as_attachment=True,
                    download_name=filename,
                    mimetype="text/csv",
                )
            except Exception as exc:  # pragma: no cover - surfaced to user
                message = f"合并失败：{exc}"

    return render_template("index.html", message=message)


if __name__ == "__main__":  # pragma: no cover
    app.run(debug=True)

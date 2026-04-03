import io
import tempfile
from pathlib import Path

import matplotlib.pyplot as plt
import streamlit as st

from report_pipeline import (
    excel_banners_to_bytes,
    excel_report_to_bytes,
    export_operational_template,
    run_pipeline,
)


def _fig_to_png(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    return buf.getvalue()


st.set_page_config(page_title="CJ Banner Report", layout="wide")
st.title("CJ Banner Report — PST")
st.caption("Upload file `.pst`, chọn khoảng ngày (hoặc để trống = toàn bộ), rồi **Chạy phân tích**.")

with st.sidebar:
    st.header("Cấu hình")
    pst_file = st.file_uploader("File PST", type=["pst"])
    c1, c2 = st.columns(2)
    with c1:
        date_from = st.text_input("Từ ngày (YYYY-MM-DD)", placeholder="để trống = không giới hạn")
    with c2:
        date_to = st.text_input("Đến ngày (YYYY-MM-DD)", placeholder="để trống = không giới hạn")

    st.subheader("운영접수리스트 (tuỳ chọn)")
    st.caption("Cần template Excel + file MD lookup như trong notebook.")
    tpl = st.file_uploader("Template (3월_운영접수리스트_*.xlsx)", type=["xlsx"])
    md_xlsx = st.file_uploader("ENM커머스 MD…xlsx", type=["xlsx"])

run = st.button("Chạy phân tích", type="primary")

if run:
    if not pst_file:
        st.error("Vui lòng upload file PST.")
        st.stop()

    df_from = date_from.strip() or None
    df_to = date_to.strip() or None

    suffix = Path(pst_file.name).suffix or ".pst"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(pst_file.getbuffer())
        pst_path = tmp.name

    try:
        with st.spinner("Đang đọc PST và phân tích…"):
            result = run_pipeline(pst_path, df_from, df_to)
    except Exception as e:
        st.exception(e)
        st.stop()
    finally:
        Path(pst_path).unlink(missing_ok=True)

    st.success(
        f"Đã load **{len(result['df'])}** email, **{len(result['tasks_df'])}** task, "
        f"**{result['stats']['Total Banners']}** banner."
    )

    st.subheader("Thống kê")
    st.dataframe(result["stats_df"], use_container_width=True, hide_index=True)

    st.subheader("Biểu đồ")
    st.pyplot(result["fig"])

    st.subheader("Bảng báo cáo (preview)")
    st.dataframe(result["report"].head(50), use_container_width=True)

    prefix = result["file_prefix"]
    main_xlsx = excel_report_to_bytes(result["report"], result["stats_df"])
    banners_xlsx = excel_banners_to_bytes(result["exp_df"])

    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button(
            label="Tải report_*.xlsx (chính + Statistics)",
            data=main_xlsx,
            file_name=f"report_{prefix}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with c2:
        st.download_button(
            label="Tải report_*_banners.xlsx",
            data=banners_xlsx,
            file_name=f"report_{prefix}_banners.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with c3:
        st.download_button(
            label="Tải biểu đồ PNG",
            data=_fig_to_png(result["fig"]),
            file_name=f"report_charts_{prefix}.png",
            mime="image/png",
        )

    with st.expander("Chi tiết feedback mapping"):
        st.text("\n".join(result["feedback_debug_lines"]))

    if tpl and md_xlsx:
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as t1:
                t1.write(tpl.getbuffer())
                tpl_path = t1.name
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as t2:
                t2.write(md_xlsx.getbuffer())
                md_path = t2.name
            op_bytes, op_log = export_operational_template(result["exp_df"], tpl_path, md_path)
            Path(tpl_path).unlink(missing_ok=True)
            Path(md_path).unlink(missing_ok=True)
            st.subheader("운영접수리스트")
            st.text(op_log)
            st.download_button(
                label="Tải 운영접수리스트 (template)",
                data=op_bytes,
                file_name=f"운영접수리스트_{prefix}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as e:
            st.warning(f"Không tạo được 운영접수리스트: {e}")

    plt.close(result["fig"])
else:
    st.info("Upload PST và bấm **Chạy phân tích**.")
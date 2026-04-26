
import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from kafka import KafkaConsumer
import threading
import time
from collections import deque

# ==========================================
# 1. KHỞI TẠO BIẾN LƯU TRỮ DỮ LIỆU
# ==========================================
data_store = {}
previous_ranks = {}  # Lưu thứ hạng của lần cập nhật trước để so sánh
throughput_history = deque(maxlen=60)  # Lưu lịch sử tốc độ 60 giây gần nhất
metrics = {"total_updates": 0, "last_count": 0, "start_time": time.time()}


# ==========================================
# 2. HÀM ĐỌC DỮ LIỆU TỪ KAFKA CHẠY NGẦM
# ==========================================
def consume_kafka():
    # Sử dụng 'latest' để bỏ qua dữ liệu cũ, chỉ đọc dữ liệu mới từ lúc bật Dashboard
    consumer = KafkaConsumer(
        'popular_pages',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        key_deserializer=lambda x: x.decode('utf-8') if x else None,
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )
    print("✅ Dashboard đã kết nối Kafka thành công! Đang chờ dữ liệu mới...")

    for message in consumer:
        try:
            if message.key and message.value:
                # Cập nhật dữ liệu vào dictionary
                data_store[message.key] = int(message.value)
                metrics["total_updates"] += 1
        except Exception as e:
            continue  # Bỏ qua lỗi nhỏ để không làm sập web


# Khởi chạy luồng đọc Kafka
threading.Thread(target=consume_kafka, daemon=True).start()

# ==========================================
# 3. THIẾT KẾ GIAO DIỆN WEB (DARK MODE)
# ==========================================
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

app.layout = dbc.Container([
    html.H1(" REAL-TIME POPULAR PAGES ANALYTICS", className="text-center text-info my-4"),

    dbc.Row([
        # --- CỘT BÊN TRÁI: BẢNG ĐIỀU KHIỂN ---
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Label("Select Top N Pages:", className="fw-bold text-light"),
                    dcc.Slider(
                        id='top-n-slider', min=5, max=20, step=5, value=10,
                        marks={5: '5', 10: '10', 15: '15', 20: '20'},
                        className="mb-4"
                    ),
                    html.Hr(),
                    html.Label("Time Window:", className="fw-bold text-light"),
                    dcc.Dropdown(
                        id='time-window',
                        options=[
                            {'label': 'Real-time (Cumulative)', 'value': 'all'},
                            {'label': 'Last 30 Minutes', 'value': '30m'},
                            {'label': 'Last 1 Hour', 'value': '1h'}
                        ],
                        value='all',
                        className="text-dark mb-2"
                    ),
                ])
            ], color="dark", outline=True)
        ], width=3),

        # --- CỘT BÊN PHẢI: BIỂU ĐỒ & THÔNG SỐ ---
        dbc.Col([
            dbc.Row([
                dbc.Col(dbc.Alert(id="active-pages-count", color="info", className="text-center fw-bold fs-5"),
                        width=6),
                dbc.Col(dbc.Alert(id="current-throughput", color="success", className="text-center fw-bold fs-5"),
                        width=6),
            ]),
            dcc.Graph(id='bar-chart-rank', animate=False),
            dcc.Graph(id='throughput-line-chart', animate=False)
        ], width=9)
    ]),

    # Bộ đếm nhịp: Cập nhật biểu đồ mỗi 2 giây (2000ms)
    dcc.Interval(id='interval', interval=2000, n_intervals=0)
], fluid=True)


# ==========================================
# 4. LOGIC XỬ LÝ & VẼ BIỂU ĐỒ (CALLBACK)
# ==========================================
@app.callback(
    [Output('bar-chart-rank', 'figure'),
     Output('throughput-line-chart', 'figure'),
     Output('active-pages-count', 'children'),
     Output('current-throughput', 'children')],
    [Input('interval', 'n_intervals')],
    [State('top-n-slider', 'value')]
)
def update_ui(n, top_n):
    global previous_ranks

    # --- 1. TÍNH TOÁN THROUGHPUT ---
    current_total = metrics["total_updates"]
    diff = current_total - metrics["last_count"]
    metrics["last_count"] = current_total
    instant_throughput = diff / 2.0  # Chia 2 vì Interval là 2 giây
    throughput_history.append({'time': time.strftime("%H:%M:%S"), 'val': instant_throughput})

    # --- 2. XỬ LÝ DỮ LIỆU & THỨ HẠNG ---
    if not data_store:
        # Nếu chưa có dữ liệu, trả về biểu đồ rỗng
        empty_fig = px.bar(template="plotly_dark")
        return empty_fig, empty_fig, "Total Pages: 0", "Speed: 0.0 msg/s"

    df = pd.DataFrame(list(data_store.items()), columns=["Page", "Views"])
    df = df.sort_values(by="Views", ascending=False).reset_index(drop=True)
    df['current_rank'] = df.index + 1

    # Tính sự thay đổi hạng (Rank Change)
    def get_delta(row):
        page = row['Page']
        curr = row['current_rank']
        if page in previous_ranks:
            prev = previous_ranks[page]
            delta = prev - curr
            if delta > 0:
                return f"▲ +{delta}"
            elif delta < 0:
                return f"▼ {delta}"
            else:
                return "-"
        return "New"

    df_top = df.head(top_n).copy()
    df_top['Rank_Change'] = df_top.apply(get_delta, axis=1)

    # Cập nhật danh sách rank để dùng cho giây tiếp theo
    previous_ranks = dict(zip(df['Page'], df['current_rank']))
    # --- 3. VẼ BIỂU ĐỒ BAR (TOP PAGES) --- (ĐÃ FIX LỖI CHÌM MÀU)
    # Sắp xếp ngược lại để trang top 1 nằm ở trên cùng của biểu đồ ngang
    df_top = df_top.sort_values(by="Views", ascending=True)

    fig_bar = px.bar(
        df_top, x="Views", y="Page", orientation='h', text="Rank_Change",
        color="Views",  # Tự động tô màu dựa trên View

        # 1. FIX DẢI MÀU: Chuyển sang 'Cividis' (nổi bật hơn 'Inferno' trên nền tối)
        color_continuous_scale=px.colors.sequential.Cividis,

        title=f"Top {top_n} Pages & Rank Status",
        template="plotly_dark"
    )

    # 2. FIX CHỮ: Đảm bảo chữ Rank_Change (New, +1) luôn có màu trắng để dễ đọc
    fig_bar.update_traces(
        textposition='outside',
        textfont=dict(color='white')
    )

    # 3. FIX NỀN & BỐ CỤC: Tăng tương phản bằng nền xám tối
    fig_bar.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        coloraxis_showscale=False,  # Giấu color bar

        # THÊM DÒNG NÀY: Đặt màu nền tối xám (#1e1e1e) thay vì đen tuyền (#000000)
        plot_bgcolor='#1e1e1e',
        paper_bgcolor='#1e1e1e',

        # Làm đẹp thêm: bo góc thanh biểu đồ (tùy chọn)
        bargap=0.1
    )
    # --- 4. VẼ BIỂU ĐỒ LINE (THROUGHPUT) ---
    df_tp = pd.DataFrame(list(throughput_history))
    fig_tp = px.line(
        df_tp, x='time', y='val',
        title="System Throughput Over Time (msg/sec)",
        template="plotly_dark",
        color_discrete_sequence=['#ffdd57']  # Màu vàng dạ quang
    )
    fig_tp.update_layout(margin=dict(l=20, r=20, t=40, b=20))

    return fig_bar, fig_tp, f"Total Pages Tracked: {len(data_store):,}", f"Speed: {instant_throughput:.1f} msg/s"


# ==========================================
# 5. KHỞI CHẠY SERVER
# ==========================================
if __name__ == '__main__':
    # use_reloader=False để tránh bị mở 2 luồng Kafka cùng lúc
    app.run(debug=True, port=8050, use_reloader=False)
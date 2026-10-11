# Demo Microservices với Kafka & Change Data Capture (CDC)

Dự án mẫu minh họa kiến trúc **Event-Driven Microservices** sử dụng cơ chế **Change Data Capture (CDC)** với **Debezium**, **Apache Kafka**, **MariaDB** và **Spring Boot**. Hệ thống tích hợp sẵn giao diện trực quan (**Web UI**) để quan sát từng bước dữ liệu di chuyển realtime qua luồng CDC.

---

## 📌 Điểm nổi bật của kiến trúc

1. **Transactional Outbox / CDC không can thiệp code ứng dụng**:
   - `Order Service` chỉ ghi dữ liệu vào MariaDB (`order_db.orders`). Service hoàn toàn không cần code logic gửi message lên Kafka.
   - **Debezium Engine** tự động đọc trực tiếp MySQL binlog của MariaDB và stream các thay đổi (CDC events) lên Kafka topic.
2. **Xử lý tồn kho bất đồng bộ (Asynchronous Inventory Reservation)**:
   - `Inventory Service` lắng nghe order CDC event từ Kafka, kiểm tra tồn kho và ghi kết quả giữ hàng (`RESERVED` hoặc `REJECTED`) vào database riêng (`inventory_db.reservations`).
3. **Thông báo và cập nhật tiến trình Realtime qua Server-Sent Events (SSE)**:
   - Debezium tiếp tục bắt CDC event từ `inventory_db` và đẩy lên Kafka topic.
   - `Notification Service` lưu thông báo vào `notification_db` và stream trực tiếp kết quả về trình duyệt qua **SSE**.
4. **Idempotency & Khả năng chịu lỗi**:
   - Hỗ trợ header `Idempotency-Key` (UUID) khi tạo đơn hàng.
   - Consumer xử lý idempotent dựa trên `order_id`, đảm bảo không trừ kho hai lần khi có replay event.

---

## 🏗️ Sơ đồ kiến trúc luồng dữ liệu

```text
Browser ──────── POST /api/orders ───────► Order Service (Port 8080)
  ▲                                             │
  │ (Web UI)                                    ▼
  │                               order_db.orders → binlog → Debezium
  │                                             │
  │                                             ▼
  │                               order_cdc.order_db.orders
  │                                  ├──► Inventory Consumer (inventory-service)
  │                                  │        │
  │                                  │        ▼
  │                                  │   inventory_db.reservations → binlog → Debezium
  │                                  │                                  │
  │                                  │                                  ▼
  │                                  │                inventory_cdc.inventory_db.reservations
  │                                  │                                  │
  │                                  │                                  ▼
  │                                  │                Notification Consumer → notification_db
  │                                  │                                  │
  │                                  │                                  └── SSE /api/notifications/stream ──► Browser
  │                                  │
  │                                  └──► Flow Observer (nhánh quan sát realtime)
  │                                             │
  └───────────── SSE /api/flow/stream ──────────┘
```

---

## 🌐 Danh sách cổng dịch vụ (Port Mapping)

Toàn bộ dịch vụ được thiết lập cổng cố định để tiện truy cập và kiểm thử:

| Dịch vụ | Host Port | Địa chỉ truy cập / Kết nối | Mô tả |
| :--- | :--- | :--- | :--- |
| **Frontend Web UI & Order API** | **`8080`** | **`http://localhost:8080`** | **Giao diện đặt hàng & theo dõi CDC luồng trực tiếp** |
| **Notification Service** | **`8090`** | **`http://localhost:8090`** | API & SSE Streams (`/api/notifications/stream`, `/api/flow/stream`) |
| **Apache Kafka Broker** | `9092` | `localhost:9092` | Kafka Broker (PLAINTEXT) |
| **Debezium Kafka Connect** | `8083` | `http://localhost:8083` | REST API quản lý connector Debezium |
| **Order DB (MariaDB)** | `3307` | `localhost:3307` | Database của Order Service (`order_db`) |
| **Inventory DB (MariaDB)** | `3308` | `localhost:3308` | Database của Inventory Service (`inventory_db`) |
| **Notification DB (MariaDB)** | `3309` | `localhost:3309` | Database của Notification Service (`notification_db`) |

---

## 🚀 Hướng dẫn chạy nhanh (Quick Start)

### Yêu cầu hệ thống
- Đã cài đặt **Docker** và **Docker Compose** (Docker Desktop trên Windows/macOS hoặc Docker Engine trên Linux).

### Bước 1: Khởi động hệ thống
Từ thư mục gốc của dự án, chạy lệnh:

```bash
docker compose -f infra/docker-compose.yaml up -d --build
```

*(Lần đầu tiên build và kéo các image có thể mất 2 - 5 phút tùy tốc độ mạng)*

### Bước 2: Kiểm tra trạng thái các container
```bash
docker compose -f infra/docker-compose.yaml ps
```

Đợi khi tất cả các container chuyển sang trạng thái `Up` (hoặc `healthy`), connector Debezium sẽ tự động khởi tạo và kết nối.

### Bước 3: Truy cập Web UI
Mở trình duyệt bất kỳ và truy cập vào:
👉 **[http://localhost:8080](http://localhost:8080)**

---

## 🧪 Hướng dẫn trải nghiệm & kiểm thử

### 1. Trải nghiệm trên Web UI (`http://localhost:8080`)
1. **Chọn người dùng**: Chọn `USER_A` hoặc `USER_B`.
2. **Chọn SKU sản phẩm**:
   - `IPHONE-15` *(Tồn kho ban đầu: 10)*
   - `MACBOOK-M3` *(Tồn kho ban đầu: 3)*
   - `AIRPODS-PRO` *(Tồn kho ban đầu: 0)*
3. **Nhập số lượng** và bấm **"Tạo order"**.
4. **Quan sát luồng**:
   - Hệ thống hiển thị thanh trạng thái thời gian thực theo từng mốc:
     1. Gửi request tạo order
     2. Order Service commit vào MariaDB
     3. Debezium phát hiện thay đổi trong binlog và đẩy lên Kafka
     4. Inventory Service xử lý kiểm tra tồn kho
     5. Debezium bắt kết quả reservation và đẩy lên Kafka
     6. Notification Service nhận kết quả, lưu DB và hiển thị thông báo trên Web UI.
   - Thử nghiệm đặt vượt quá tồn kho (ví dụ: mua 5 chiếc `MACBOOK-M3` hoặc mua `AIRPODS-PRO`) để quan sát kết quả từ chối (`REJECTED`) với lý do `Tồn kho không đủ`.

### 2. Gọi API trực tiếp qua cURL / PowerShell

Tạo đơn hàng mới:

```bash
# Bằng cURL
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"customerId":"USER_A","sku":"IPHONE-15","quantity":1}'
```

```powershell
# Bằng PowerShell (Windows)
$idempotencyKey = [guid]::NewGuid().ToString()
$orderBody = '{"customerId":"USER_A","sku":"IPHONE-15","quantity":1}'
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/orders" `
  -Headers @{ 'Idempotency-Key' = $idempotencyKey } `
  -ContentType 'application/json' -Body $orderBody
```

Xem danh sách thông báo đã lưu của người dùng:
```bash
curl "http://localhost:8090/api/notifications?customerId=USER_A"
```

Xem trực tiếp các event CDC trên Kafka:
```bash
# Xem Order CDC events
docker compose -f infra/docker-compose.yaml exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic order_cdc.order_db.orders \
  --from-beginning --property print.key=true --property print.headers=true

# Xem Reservation CDC events
docker compose -f infra/docker-compose.yaml exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic inventory_cdc.inventory_db.reservations \
  --from-beginning --property print.key=true --property print.headers=true
```

---

## 🛠️ Các lệnh quản trị & debug thường dùng

- **Xem log các service chính:**
  ```bash
  docker compose -f infra/docker-compose.yaml logs -f order-service inventory-service notification-service debezium-connect
  ```

- **Kiểm tra trạng thái Debezium Connectors:**
  ```bash
  curl -s http://localhost:8083/connectors/order-connector/status
  curl -s http://localhost:8083/connectors/inventory-connector/status
  ```

- **Dừng các container (vẫn giữ dữ liệu volume):**
  ```bash
  docker compose -f infra/docker-compose.yaml down
  ```

- **Xóa toàn bộ container và reset dữ liệu (xóa volumes DB & Kafka):**
  ```bash
  docker compose -f infra/docker-compose.yaml down -v
  ```

- **Scale Inventory Consumers (chia 3 partition Kafka):**
  ```bash
  docker compose -f infra/docker-compose.yaml up -d --scale inventory-service=3
  ```

---

## 📁 Cấu trúc thư mục

```text
├── docs/                                # Tài liệu kỹ thuật chi tiết
│   └── PLAN.md                          # Thiết kế chi tiết luồng CDC & nghiệp vụ
├── infra/                               # Cấu hình hạ tầng Docker
│   ├── docker-compose.yaml              # Định nghĩa toàn bộ services, databases, Kafka, Debezium
│   ├── connectors/                      # File cấu hình Debezium connectors
│   │   ├── order-connector.json         # Đọc binlog từ order-db
│   │   └── inventory-connector.json     # Đọc binlog từ inventory-db
│   └── mariadb/                         # Script khởi tạo và migration MariaDB cho từng service
│       ├── order/                       # CSDL cho order-service
│       ├── inventory/                   # CSDL cho inventory-service
│       └── notification/                # CSDL cho notification-service
├── src/
│   ├── backend/
│   │   ├── order-services/              # REST API tạo order, phục vụ Frontend static files
│   │   ├── inventory-service/           # Kafka Consumer nhận Order CDC, xử lý tồn kho
│   │   └── notification-service/        # Kafka Consumer nhận Reservation CDC, phát SSE
│   └── frontend/
│       └── order-ui/                    # Giao diện HTML/CSS/JS thuần, trực quan hóa luồng CDC
└── README.md
```

const form = document.querySelector("#order-form");
const submitButton = document.querySelector("#submit-button");
const submitLabel = document.querySelector(".submit-button__label");
const resultPanel = document.querySelector("#result-panel");
const resultEyebrow = document.querySelector("#result-eyebrow");
const resultTitle = document.querySelector("#result-title");
const resultMessage = document.querySelector("#result-message");
const orderIdPanel = document.querySelector("#order-id-panel");
const orderId = document.querySelector("#order-id");
const skuInput = document.querySelector("#sku");
const quantityInput = document.querySelector("#quantity");
const notificationConnection = document.querySelector("#notification-connection");
const notificationConnectionLabel = document.querySelector("#notification-connection-label");
const notificationList = document.querySelector("#notification-list");
const notificationEmpty = document.querySelector("#notification-empty");
const notificationsById = new Map();
const pendingOrderStorageKey = "order-cdc-demo.pending-order";
let pendingOrderAttempt = readPendingOrderAttempt();

if (pendingOrderAttempt) {
    skuInput.value = pendingOrderAttempt.sku;
    quantityInput.value = String(pendingOrderAttempt.quantity);
}

function readPendingOrderAttempt() {
    try {
        const saved = JSON.parse(sessionStorage.getItem(pendingOrderStorageKey));
        if (saved
            && typeof saved.idempotencyKey === "string"
            && typeof saved.sku === "string"
            && Number.isInteger(saved.quantity)
            && saved.quantity > 0) {
            return saved;
        }
    } catch {
        // Keep the form usable if session storage is unavailable.
    }
    return null;
}

function savePendingOrderAttempt(attempt) {
    pendingOrderAttempt = attempt;
    try {
        sessionStorage.setItem(pendingOrderStorageKey, JSON.stringify(attempt));
    } catch {
        // The in-memory key still supports retries until this page closes.
    }
}

function clearPendingOrderAttempt() {
    pendingOrderAttempt = null;
    try {
        sessionStorage.removeItem(pendingOrderStorageKey);
    } catch {
        // No persistent value was available to remove.
    }
}

function createIdempotencyKey() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
        return window.crypto.randomUUID();
    }
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (character) => {
        const random = Math.floor(Math.random() * 16);
        const value = character === "x" ? random : (random & 0x3) | 0x8;
        return value.toString(16);
    });
}

function getPendingOrderAttempt(request) {
    if (pendingOrderAttempt) {
        const samePayload = pendingOrderAttempt.sku === request.sku
            && pendingOrderAttempt.quantity === request.quantity;
        return samePayload ? pendingOrderAttempt : null;
    }

    const attempt = {
        idempotencyKey: createIdempotencyKey(),
        sku: request.sku,
        quantity: request.quantity,
    };
    savePendingOrderAttempt(attempt);
    return attempt;
}

function showResult({ state, eyebrow, title, message, id }) {
    resultPanel.hidden = false;
    resultPanel.dataset.state = state;
    resultPanel.setAttribute("aria-live", state === "error" ? "assertive" : "polite");
    resultEyebrow.textContent = eyebrow;
    resultTitle.textContent = title;
    resultMessage.textContent = message;
    orderIdPanel.hidden = !id;
    orderId.textContent = id ?? "";
}

function apiErrorMessage(body) {
    const details = body && typeof body.details === "object" && body.details !== null
        ? Object.entries(body.details).map(([field, message]) => {
            const label = field === "sku" ? "SKU" : field === "quantity" ? "Số lượng" : field;
            return `${label}: ${message}`;
        })
        : [];
    const message = body && typeof body.message === "string"
        ? body.message
        : "Order Service không thể tạo order.";

    return [message, ...details].join(" · ");
}

function setNotificationConnection(state, message) {
    notificationConnection.dataset.state = state;
    notificationConnectionLabel.textContent = message;
}

function notificationStatus(status) {
    return status === "RESERVED" ? "Đã giữ hàng" : "Không giữ được hàng";
}

function notificationReason(reason) {
    if (reason === "INSUFFICIENT_STOCK") {
        return "Tồn kho không đủ cho số lượng yêu cầu.";
    }
    if (reason === "SKU_NOT_FOUND") {
        return "Không tìm thấy SKU trong inventory.";
    }
    return reason || "";
}

function notificationTime(value) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? "" : date.toLocaleString("vi-VN");
}

function createNotificationItem(notification) {
    const item = document.createElement("li");
    item.className = "notification-item";
    item.dataset.state = notification.status;

    const heading = document.createElement("div");
    heading.className = "notification-item__heading";

    const title = document.createElement("h3");
    title.textContent = `${notification.sku} · ${notification.quantity} sản phẩm`;

    const status = document.createElement("span");
    status.className = "notification-item__status";
    status.textContent = notificationStatus(notification.status);
    heading.append(title, status);

    const order = document.createElement("code");
    order.className = "notification-item__order";
    order.textContent = `ORDER ${notification.orderId}`;

    item.append(heading, order);

    const reason = notificationReason(notification.reason);
    if (reason) {
        const reasonText = document.createElement("p");
        reasonText.className = "notification-item__reason";
        reasonText.textContent = reason;
        item.append(reasonText);
    }

    const time = notificationTime(notification.createdAt);
    if (time) {
        const timestamp = document.createElement("p");
        timestamp.className = "notification-item__time";
        timestamp.textContent = time;
        item.append(timestamp);
    }

    return item;
}

function mergeNotifications(items) {
    for (const item of items) {
        if (item && item.id !== undefined && typeof item.orderId === "string") {
            notificationsById.set(String(item.id), item);
        }
    }

    const newest = [...notificationsById.values()]
        .sort((left, right) => Number(right.id) - Number(left.id))
        .slice(0, 20);
    notificationsById.clear();
    for (const item of newest) {
        notificationsById.set(String(item.id), item);
    }

    notificationList.replaceChildren(...newest.map(createNotificationItem));
    notificationEmpty.hidden = newest.length > 0;
}

function parseEventData(event) {
    try {
        return JSON.parse(event.data);
    } catch {
        setNotificationConnection("error", "Dữ liệu notification không hợp lệ");
        return null;
    }
}

function connectNotificationStream() {
    if (window.location.protocol === "file:") {
        setNotificationConnection("error", "Mở giao diện tại http://localhost:8081");
        return;
    }

    const streamUrl = `${window.location.protocol}//${window.location.hostname}:8090/api/notifications/stream`;
    const eventSource = new EventSource(streamUrl);

    eventSource.onopen = () => setNotificationConnection("connected", "Đang nhận realtime");
    eventSource.onerror = () => setNotificationConnection("error", "Mất kết nối · đang thử lại");
    eventSource.addEventListener("snapshot", (event) => {
        const snapshot = parseEventData(event);
        if (Array.isArray(snapshot)) {
            mergeNotifications(snapshot);
        }
    });
    eventSource.addEventListener("notification", (event) => {
        const notification = parseEventData(event);
        if (notification) {
            mergeNotifications([notification]);
        }
    });
}

form.addEventListener("submit", async (event) => {
    event.preventDefault();

    if (window.location.protocol === "file:") {
        showResult({
            state: "error",
            eyebrow: "TRANG ĐANG MỞ TỪ FILE",
            title: "Mở giao diện qua Order Service",
            message: "File local chỉ hiển thị giao diện. Truy cập http://localhost:8081/ để gửi order tới API.",
        });
        return;
    }

    if (!form.reportValidity()) {
        return;
    }

    const formData = new FormData(form);
    const request = {
        sku: String(formData.get("sku")).trim(),
        quantity: Number(formData.get("quantity")),
    };
    const attempt = getPendingOrderAttempt(request);
    if (!attempt) {
        showResult({
            state: "error",
            eyebrow: "CÒN REQUEST CHƯA XÁC NHẬN",
            title: "Gửi lại order đang chờ trước",
            message: "Giữ nguyên SKU và số lượng để retry cùng Idempotency-Key. Sau khi nhận kết quả, bạn có thể tạo order mới.",
        });
        return;
    }

    submitButton.disabled = true;
    submitLabel.textContent = "Đang lưu order…";
    form.setAttribute("aria-busy", "true");
    showResult({
        state: "loading",
        eyebrow: "ORDER SERVICE",
        title: "Đang lưu order",
        message: "Đang chờ database commit.",
    });

    try {
        const response = await fetch("/api/orders", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Idempotency-Key": attempt.idempotencyKey,
            },
            body: JSON.stringify(request),
        });
        const body = await response.json().catch(() => null);

        if (!response.ok) {
            if (response.status < 500) {
                clearPendingOrderAttempt();
            }
            showResult({
                state: "error",
                eyebrow: `HTTP ${response.status}`,
                title: "Chưa tạo được order",
                message: apiErrorMessage(body),
            });
            return;
        }

        if (!body || typeof body.id !== "string") {
            throw new Error("Order Service trả về response không đúng định dạng.");
        }

        clearPendingOrderAttempt();
        showResult({
            state: "success",
            eyebrow: `HTTP ${response.status} · ORDER ĐÃ LƯU`,
            title: response.status === 201 ? "Order đã được tạo" : "Order này đã được ghi nhận trước đó",
            message: response.status === 201
                ? "Inventory sẽ giữ hàng bất đồng bộ. Kết quả được lưu qua CDC và hiện ở mục Kết quả giữ hàng."
                : "Request dùng cùng Idempotency-Key nên Order Service trả lại order đã tạo trước đó.",
            id: body.id,
        });
    } catch (error) {
        showResult({
            state: "error",
            eyebrow: "KHÔNG KẾT NỐI ĐƯỢC",
            title: "Chưa nhận được phản hồi",
            message: error instanceof TypeError
                ? "Không kết nối được Order Service. Hãy kiểm tra Docker Compose và tải lại trang."
                : error.message,
        });
    } finally {
        submitButton.disabled = false;
        submitLabel.textContent = "Tạo order";
        form.removeAttribute("aria-busy");
    }
});

if (pendingOrderAttempt) {
    showResult({
        state: "pending",
        eyebrow: "REQUEST ĐANG CHỜ XÁC NHẬN",
        title: "Có thể retry cùng một order",
        message: "Bấm Tạo order để gửi lại cùng Idempotency-Key và nhận kết quả trước đó nếu order đã được lưu.",
    });
}

connectNotificationStream();

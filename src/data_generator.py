import os
import time
import json
import random
import pandas as pd
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData
from faker import Faker

# --- CẤU HÌNH (THAY THẾ THÔNG TIN CỦA BẠN VÀO ĐÂY) ---
CONNECTION_STR = os.environ["EVENTHUB_CONNECTION_STR"]
EVENT_HUB_NAME = "server-telemetry"

fake = Faker()

# --- 1. HÀM TẠO DỮ LIỆU BATCH (INVENTORY - CSV) ---
def generate_inventory_csv(num_servers=50):
    print(f"Dang tao file Inventory ({num_servers} servers)...")
    servers = []
    for i in range(1, num_servers + 1):
        server = {
            "server_id": f"SRV-{1000+i}",
            "data_center_id": random.choice(["DC-HN-01", "DC-HCM-01", "DC-DN-01"]),
            "location": random.choice(["Ha Noi", "Ho Chi Minh", "Da Nang"]),
            "admin_email": fake.email(),
            "purchase_date": fake.date_between(start_date='-2y', end_date='today').isoformat()
        }
        servers.append(server)
    
    df = pd.DataFrame(servers)
    df.to_csv("server_inventory.csv", index=False)
    print("-> Da tao xong file 'server_inventory.csv'. Hay upload file nay len Container 'bronze' tren Azure.")

# --- 2. HÀM TẠO DỮ LIỆU COMPLEX (ERROR CODES - NESTED JSON) ---
def generate_error_defs_json():
    print("Dang tao file Error Definitions (Nested JSON)...")
    error_codes = [
        {
            "error_code": "ERR_CPU_HIGH",
            "description": "CPU usage exceeded safe threshold",
            "details": {
                "severity": "Critical",
                "possible_causes": ["Runaway process", "Malware infection", "Cooling failure"],
                "resolution_steps": ["Check task manager", "Scan virus", "Check fans"]
            }
        },
        {
            "error_code": "ERR_RAM_OOM",
            "description": "Out of Memory",
            "details": {
                "severity": "High",
                "possible_causes": ["Memory leak", "Too many apps"],
                "resolution_steps": ["Restart service", "Add RAM"]
            }
        },
         {
            "error_code": "ERR_DISK_FULL",
            "description": "Disk space is full",
            "details": {
                "severity": "Medium",
                "possible_causes": ["Log files accumulation", "Large downloads"],
                "resolution_steps": ["Clear temp files", "Archive logs"]
            }
        }
    ]
    
    with open("error_definitions.json", "w") as f:
        json.dump(error_codes, f, indent=4)
    print("-> Da tao xong file 'error_definitions.json'. Hay upload file nay len Container 'bronze' tren Azure.")

# --- 3. HÀM GỬI DỮ LIỆU STREAMING (TELEMETRY) ---
def stream_telemetry_to_azure():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR, 
        eventhub_name=EVENT_HUB_NAME
    )
    
    print("\n--- BAT DAU GUI DU LIEU STREAMING LEN AZURE EVENT HUBS ---")
    print("Nhan Ctrl+C de dung lai.\n")
    
    server_ids = [f"SRV-{1000+i}" for i in range(1, 51)] # Danh sach 50 server
    
    try:
        while True:
            event_data_batch = producer.create_batch()
            
            # Giả lập 1 đợt gửi 5 bản tin
            for _ in range(5):
                # Tạo dữ liệu ngẫu nhiên
                telemetry = {
                    "server_id": random.choice(server_ids),
                    "timestamp": datetime.utcnow().isoformat(),
                    "cpu_usage": round(random.uniform(10.0, 99.9), 2),
                    "ram_usage": round(random.uniform(20.0, 90.0), 2),
                    "temperature": random.randint(35, 95),
                    "status": "active"
                }
                
                # THỈNH THOẢNG TẠO DỮ LIỆU LỖI (Để demo khả năng xử lý lỗi của Databricks)
                if random.random() < 0.05: # 5% cơ hội sinh lỗi
                    telemetry["cpu_usage"] = -999.0 # Lỗi logic
                    print(f"[BAD DATA GENERATED] Server {telemetry['server_id']} sent invalid CPU data.")

                # Thêm vào batch
                event_data_batch.add(EventData(json.dumps(telemetry)))
            
            # Gửi lên Azure
            producer.send_batch(event_data_batch)
            print(f"-> Da gui 5 ban tin luc {datetime.now().strftime('%H:%M:%S')}")
            
            time.sleep(2) # Nghỉ 2 giây rồi gửi tiếp (High Velocity simulation)
            
    except KeyboardInterrupt:
        print("Da dung gui du lieu.")
    finally:
        producer.close()

if __name__ == "__main__":
    # Bước 1: Tạo file tĩnh (Chạy 1 lần rồi upload thủ công)
    generate_inventory_csv()
    generate_error_defs_json()
    
    # Bước 2: Chạy Streaming (Chạy liên tục khi demo)
    input("\nNhan Enter de bat dau day du lieu Streaming...")
    stream_telemetry_to_azure()
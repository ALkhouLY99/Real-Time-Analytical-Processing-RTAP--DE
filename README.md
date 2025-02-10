# 🗳️ UK Election Analytics Platform

![with_actons_streaming_process_](https://github.com/user-attachments/assets/ba33e857-d838-4299-b87e-257789931d54)

## 🚀 Overview
This project is a **real-time election analytics platform** that processes and visualizes voting data for UK candidates and voters. It integrates **Apache Spark, Kafka, PostgreSQL, Prometheus, Grafana, and Streamlit** to ensure **scalability, efficiency, and insightful data visualization**.

## 🏗️ Tech Stack
- **📡 Data Ingestion:** `API-based streaming source`
- **🔥 Data Processing:** `Apache Spark`
- **📨 Messaging Queue:** `Apache Kafka`
- **🗄️ Database Storage:** `PostgreSQL`
- **📊 Monitoring:** `Prometheus` & `Grafana`
- **🎨 Frontend Dashboard:** `Streamlit`
- **⚙️ Automation & Management:** `Makefile`

---

## 🔄 Workflow
1. **Data Ingestion:** The system fetches real-time voting data from an external API.
2. **Streaming Pipeline:** `Apache Kafka` queues and distributes the data efficiently.
3. **Processing Layer:** `Spark processes` and transforms the data for aggregation and analysis.
4. **Storage:** Cleaned data is stored in a `PostgreSQL` database.
5. **Monitoring:** `Prometheus` collects system metrics, and `Grafana` visualizes performance.
6. **Visualization Dashboard:** `Streamlit` presents live analytics on voter turnout, candidate rankings, and more.

---

## 📊 Key Features
✅ **Real-time vote tracking** with Kafka & Spark  
✅ **Live candidate leaderboards** using Streamlit  
✅ **Scalable data pipelines** built with Spark  
✅ **Historical vote analytics** stored in PostgreSQL  
✅ **System monitoring** via Prometheus & Grafana  
✅ **Automated workflow management** with Makefile  

---

## 🚀 Setup & Deployment
### 🔧 Prerequisites
Ensure you have the following installed:
- Docker & Docker Compose
- Python (>=3.8)
- Apache Kafka
- Apache Spark
- PostgreSQL
- Prometheus & Grafana

### 🛠️ Installation
Clone the repository and navigate to the project directory:
```sh
git clone https://github.com/your-repo/voting-analytics.git
cd voting-analytics
```

Use the **Makefile** for easy management:
```sh
make setup      # Install dependencies
make start      # Start all services
make stop       # Stop all services
make logs       # View logs
make clean      # Remove all containers and volumes
```

---

## 📺 Dashboard Previews
### 📌 Live Election Insights
![Streamlit Dashboard](assets/streamlit_dashboard.png)

### 📌 Vote Distribution Monitoring
![Grafana Metrics](assets/grafana_metrics.png)

---

## 💡 Future Enhancements
🚀 Add AI-based voter sentiment analysis  
🚀 Expand monitoring with custom Prometheus alerts  
🚀 Implement ML-based election outcome prediction  

---

💡 **Built with passion for real-time analytics & data-driven decision-making!** 🚀

# 🚲 **GreenMoving**
This project presents the design and implementation of a Self-Adaptive Management System for an urban E-Bike sharing service, orchestrated via Docker it integrates Python-based autonomous agents with InfluxDB for time-series data storage and Grafana for real-time observability. By leveraging a distributed architecture and a MAPE-K (Monitor-Analyze-Plan-Execute-Knowledge), the system autonomously manages e-bikes and smart charging stations.

## **System Architecture**
<img src="doc/img/sub_system_view.png" width="500">

## **Prerequisites**
- Windows 10/11 
- Docker Desktop 4.58.0
- Python 3.13.9 (for local development and testing only)


## 🏗 **Project Structure**
```
📂 nome_progetto
┣ 📂 src/        # source code
┣ 📂 docs/       # documentation
┣ 📄 README.md 
```

## **Installation Steps**
1. Clone the repo
```sh
git clone https://github.com/m4rylu/SE4AS_GreenMoving
cd SE4AS_GreenMoving/src
```

2. Build and launch the system
```sh
docker-compose up --build -d
```

3. Navigate to http://localhost:3000/ where you will have access to all dashboards
![sequence diagram](doc/img/dashboard_list.png)

4. Select one of them for retrieving information about monitoring, availability and events.

| Monitoring Dashboard | Availability Dashboard | Log Dashboard |
| :---: | :---: | :---: |
| <img src="doc/img/monitoring_dashboard.png" width="500"> | <img src="doc/img/available_dashboard.png" width="800"> | <img src="doc/img/log_dashboard.png" width="500"> |

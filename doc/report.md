# 📌 **GreenMoving**

**Versione:** 1.0  
**Ultimo aggiornamento:** 02/09/2026

## 📖 **Project Description**
### Introduction
This project is a Smart Mobility Monitoring System orchestrated via Docker. It integrates Python-based autonomous agents with InfluxDB for time-series data storage and Grafana for real-time observability.
### Goals of the System
### Managed Resources
### Sensors and Actuators
### Self-Adaptation
We added and explanation for each part of the self-adaptation choiche done in image above.
**Time**: Our application is *proactive* when monitoring bikes while have a *reactive* part for the station monitoring, ths is because bikes have to be controlled always for avoiding theft while station just have to monitor when bikes are connected or charging or when empty or full for balancing station loads.
**Technique**: The system relies on adjustable tresholds and *parameters* retrieved from monitored resources
**Approach**: is *external* because adaptation logic is decoupled from the managed resources
**Adaptation Decision Criteria**: The system operates to maintain a specific objective (not empty or full station and not discharged bikes) and uses also rules to reach them (balance energy for better charging)
**Degree of Centralization**: Hybrid because even if each agent has its own autonomy and logic all services are coordinated using docker compose and share a centralized state repository
**Reason**: change in the
**Application**: The system is not a single monolithic program but a distributed ensemble of independent services (containers) working together
**System Software**: Middleware because the system acts as layer between the raw infrastructure (simulated bikes/sensors) and the end-user interface
**Communication**: System relies on specific communication patterns mqtt and influxdb
**Context & Technical Resources**: the system monitors the context (station full or empty) and the technical resources (level of batter and other monitoring parameters)


## 🚀 **Installation**
### **Prerequisites**
- Windows 10/11 
- Docker Desktop 4.58.0
- Python 3.13.9 (for local development and testing only)

### **Passaggi per l'installazione**
```sh
# Clonare il repository
git clone https://github.com/m4rylu/SE4AS_GreenMoving
cd SE4AS_GreenMoving

# Build and launch the system
docker-compose up --build -d
```

## 🏗 **Struttura del Progetto**
```
📂 nome_progetto
┣ 📂 src/        # Codice sorgente
┃  ┣ 📄 init.py
┃  ┗ 📂 ab_minmax/ 
┣ 📂 docs/       # Documentazione
┣ 📂 tests/      # Test unitari
┣ 📄 README.md   # Documentazione principale
┣ 📄 requirements.txt # Dipendenze
┣ 📄 main.py     # File principale
```

## ⚙️ **Utilizzo**
Esempio di esecuzione:
```sh
python main.py --input file.csv
```

## 🛠 **Funzioni Principali**

### **Classe `CyberAnalyzer`**
```python
class CyberAnalyzer:
    """
    Classe principale per l'analisi dei dati.
    
    Metodi:
    - load_data(file_path: str) -> None
    - analyze() -> dict
    - generate_report() -> str
    """
```

### **Metodi Principali**
- **`load_data(file_path: str) -> None`**  
  Carica i dati da un file CSV.
- **`analyze() -> dict`**  
  Analizza i dati e restituisce statistiche principali.
- **`generate_report() -> str`**  
  Genera un report con i risultati dell’analisi.

## 📊 **Esempio di Output**
```sh
Analisi completata!
Totale record: 1200
Attività sospette rilevate: 34
```

## 📸 **Screenshot (se applicabile)**
_Aggiungi screenshot dell'interfaccia o dell'output del programma_

## 👤 **Autore e Contatti**
👨‍💻 **Sviluppato da:** Maria Alberta Caradio 
📧 **Email:** mariaalberta.caradio@student.univaq.it
🔗 **GitHub:** [github.com/tuo_progetto]  

📌 *Ultimo aggiornamento: [Data]*
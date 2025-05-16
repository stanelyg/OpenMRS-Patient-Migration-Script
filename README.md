# OpenMRS-Patient-Migration-Script
This Python script facilitates the automated migration of DREAMS client enrollment records from a legacy MySQL database into an OpenMRS instance. It performs full patient registration, including person details, addresses, attributes, and identifiers, as well as mapping client IDs to patient records for DREAMS program use.

## Features
- Connects to both source and destination MySQL databases.

- Migrates:

  - Person data (name, gender, birthdate)

  - Addresses and personal attributes

  - Patient identifiers (National ID or Birth Certificate)

  - Fallback to generated patient ID if no identifier exists

- Automatically maps client_id to patient_id in dreams_client_patient_mapping.

- Ensures all uuid fields are generated dynamically.
## Project Structure
- ├── migrate_patients.py  
- └── README.md           

## Prerequisites
- Python 3.6+
- MySQL Server (running locally or remotely)
- OpenMRS database schema already initialized
## Installation
```bash
  git clone https://github.com/your-username/openmrs-patient-migration.git
  cd openmrs-patient-migration
```
## Create a virtual environment
```bash
python3 -m venv venv
```
## Activate the virtual environment
- **On Linux/macOS:**
  ```bash
   source venv/bin/activate
  ```
- **On Windows:**
   ```bash
  venv\Scripts\activate
   ```
## Install required packages:
 ```bash
 pip install mysql-connector-python
```
## Usage
 ```bash
python migrate_patients.py
```
## Author
**Henry Gicugu**
For questions, contact via GitHub Issues or pull requests.





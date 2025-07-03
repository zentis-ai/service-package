#servicepackage

`servicepackage` is a reusable framework that interact with MongoDB and RabbitMQ. It provides the core logic for consuming messages, handling workflows, and dispatching output to the next nodes based on configurations stored in MongoDB.

---

##Features

- Plug-and-play message consumption from RabbitMQ
- Dynamic node execution based on MongoDB `runs` collection
- Parameter validation using your own `required_params`
- Integration point for your custom logic via `service_function`
- Auto-routing to next queue based on workflow definition

---

##Folder Structure (example)

```
servicepackage/
├── __init__.py
├── service.py             # Main framework logic
├── connections.py         # Shared DB & RabbitMQ connection logic
setup.py
README.md
```

---

##Installation

###From GitHub

```bash
pip install git+https://github.com/techvantageai/zentis-servicepackage.git@dev#egg=servicepackage
```

##Usage

### 1. Create your own `service_function.py` file

```python
# service_function.py
def main(input_params: dict, output_params: list, run_id: str) -> dict:
    # your business logic here
    return {"output_key": "output_value"}
```

### 2. Define your own `required_params.py`

```python
# required_params.py
REQUIRED_INPUT_PARAMS = {
    "subtype_id_example": [
        {"name": "input_key", "dataType": "string", "required": True}
    ]
}

NODE_OUTPUT_PARAMS = {
    "subtype_id_example": [
        {"name": "output_key", "dataType": "string"}
    ]
}
```

### 3. Initialize and start the service

```python
from servicepackage.service import Service
from required_params import REQUIRED_INPUT_PARAMS, NODE_OUTPUT_PARAMS
from service_function import main

svc = Service(
    queue_name="DB_PUSH",
    node_type="db-push",
    required_input_params=REQUIRED_INPUT_PARAMS,
    node_output_params=NODE_OUTPUT_PARAMS,
    service_function=main
)

svc.start()
```

---

##Requirements

- Python 3.8+
- RabbitMQ
- MongoDB

##Dependencies

Automatically installed:

- `pika`
- `pymongo`
- `python-dotenv`

---

##Environment Variables

Create a `.env` file or export these before running:

```
MONGO_URI=
MONGO_DATABASE=
RABBITMQ_HOST=
RABBITMQ_USERNAME=
RABBITMQ_PASSWORD=
```

---

## 📄 License

MIT License or specify your own.

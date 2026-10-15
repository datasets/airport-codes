VENV = env
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip

.PHONY: data clean

all: data

data: $(VENV)/bin/activate
	$(PYTHON) scripts/process.py

$(VENV)/bin/activate: scripts/requirements.txt
	python3 -m venv $(VENV)
	$(PIP) install -r scripts/requirements.txt

validate:
	$(PYTHON) -m frictionless validate datapackage.json

clean:
	rm -rf __pycache__
	rm -rf $(VENV)

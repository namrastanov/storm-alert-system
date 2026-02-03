# storm-alert-system

![Python](https://img.shields.io/badge/python-3.10+-blue.svg) ![Status](https://img.shields.io/badge/status-active-success.svg) ![Docker](https://img.shields.io/badge/docker-ready-blue.svg)

Real-time severe weather monitoring and alert notification system with AI-powered threat assessment


## Features

- **Real-time Monitoring**: Continuous processing of weather data streams
- **Smart Prioritization**: ML-based alert ranking by impact potential
- **Multi-Channel Alerts**: Email, SMS, push notifications, and webhooks
- **Radar Integration**: NEXRAD radar data processing for storm tracking
- **Historical Analysis**: Pattern recognition from past severe weather events


## Installation

```bash
# Clone the repository
git clone https://github.com/{username}/storm-alert-system.git
cd storm-alert-system

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

## Quick Start

```python
from storm_alert_system import main

# Initialize the application
app = main.create_app()

# Run the service
app.run()
```

## Configuration

Copy the example environment file and configure your settings:

```bash
cp .env.example .env
```

Key configuration options:
- `API_KEY`: Your API key for weather data providers
- `DATABASE_URL`: Database connection string
- `REDIS_URL`: Redis connection for caching
- `LOG_LEVEL`: Logging verbosity (DEBUG, INFO, WARNING, ERROR)

## Development

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Run tests
pytest

# Run linting
ruff check .

# Format code
black .
```

## API Documentation

Once running, access the API documentation at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

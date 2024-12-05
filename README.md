# Application Installation and Setup Instructions

## Installation Steps

### 1. Download the Repository
Clone the repository from GitHub using:
```bash
git clone https://github.com/pmarek15/streams-cli.git
```

### 2. Navigate to Code Directory
Enter the project directory:
```bash
cd streams-cli
```

### 3. Build the Application
Compile the code:
```bash
go build
```

### 4. Configure the Application
Copy the example configuration file with commented examples to create your config file:
```bash
cp config.example.yaml config.yaml
```

### 5. Configuration Setup
Modify the config.yaml file according to your environment settings.

## Usage Examples

### Kafka Operations

For producing messages to Kafka:
```bash
./stream produce -t kafka -f 1000
```

For consuming messages from Kafka:
```bash
./stream consume -t kafka -f 1000
```

For running a Kafka benchmark:
```bash
./stream benchmark -t kafka -d 30 -s 512
```
This command runs a 30-second benchmark with 512-byte messages.

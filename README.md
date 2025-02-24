# buzzline-06-gjrich

This project creates a dummy computing resource consumption dashboard using Kafka and Matplotlib. A producer generates random CPU, RAM, read, write, and disk space data, sending it to a Kafka topic and a JSON file. A consumer retrieves these messages, visualizing them in real-time with four charts in a single window: line graphs for CPU and RAM, a scatter plot for read/write, and a pie chart for disk space. The visualizations dynamically scale and update, showing trends over the last 30 snapshots. 

Example![image](https://github.com/user-attachments/assets/268478b2-c5df-4237-84ba-2679ff5b883e)


Brief Short Instructions:

Prereqs
- virtual environment set up with packages installed
- zookeeper installed and running
- kafka installed and running
See the references further down in the readme for more information.

On Windows, to run the environment (after installing, configuring, and running zookeeper and kafka), first start the producer:

```shell
.venv\Scripts\activate
py -m producers.producer_gjrich
```

Then start the consumer:

```shell
.venv\Scripts\activate
py -m consumers.consumer_gjrich
```


Detailed step by step instructions are included below, along with recommended VSCode extensions.

_____________________________________________________________________
## Recommended VS Code Extensions

- Black Formatter by Microsoft
- Markdown All in One by Yu Zhang
- PowerShell by Microsoft (on Windows Machines)
- Pylance by Microsoft
- Python by Microsoft
- Python Debugger by Microsoft
- Ruff by Astral Software (Linter)
- SQLite Viewer by Florian Klampfer
- WSL by Microsoft (on Windows Machines)



## Full Instructions

## Step 0. Clone down repository & Install Python 3.11
Run this in the target repository (from powershell if windows). Git must be installed.

```shell
git clone https://github.com/gjrich/buzzline-04-gjrich/
```


Download Python 3.11 for your OS:

https://www.python.org/downloads/release/python-3119/


## Step 1. Manage Local Project Virtual Environment (Windows included

### Windows Instructions:
Create Virtual Environment (in project directory)

```shell
py -3.11 -m venv .venv
```

Activate / Install packages
```shell
.venv\Scripts\activate
py -m pip install --upgrade pip setuptools wheel
py -m pip install --upgrade -r requirements.txt
```

### Mac/Linux:
Create Virtual Environment (in project directory)
```zsh
python3 -3.11 -m venv .venv
```

Activate / Install packages
```zsh
source .venv/bin/activate
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install --upgrade -r requirements.txt
```

## Step 2. Start Zookeeper and Kafka (2 Terminals)

If Zookeeper and Kafka are not already running, you'll need to restart them.
See instructions at [SETUP-KAFKA.md] to:

1. Start Zookeeper Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-7-start-zookeeper-service-terminal-1))
2. Start Kafka ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-8-start-kafka-terminal-2))

---

## Step 3. Start the Kafka Streaming Application

This will take two terminals:

1. One to run the producer which writes to a file in the data folder. 
2. Another to run the consumer which reads from the dynamically updated file. 


### Producer Terminal

Start the producer to generate the messages. 

In VS Code, open a NEW terminal.
Use the commands below to activate .venv, and start the producer. 

Windows:

```shell
.venv\Scripts\activate
py -m producers.producer_gjrich
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m producers.producer_gjrich
```


### Consumer Terminal

Start the associated consumer that will process and visualize the messages. 

In VS Code, open a NEW terminal in your root project folder. 
Use the commands below to activate .venv, and start the consumer. 

Windows:
```shell
.venv\Scripts\activate
py -m consumers.project_consumer_gjrich
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m consumers.project_consumer_gjrich
```



## Save Space
To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later. 


## License
This project is licensed under the MIT License as an example project. 
You are encouraged to fork, copy, explore, and modify the code as you like. 
See the [LICENSE](LICENSE.txt) file for more.

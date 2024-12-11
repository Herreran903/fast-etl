import asyncio
from asyncio import WindowsSelectorEventLoopPolicy
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import yaml
from sqlalchemy import create_engine, inspect, text

asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

notebooks = [
    "dim_client.ipynb", 
    "dim_date.ipynb", 
    "dim_headquarter.ipynb", 
    "dim_messenger.ipynb",
    "dim_time.ipynb",
    "dim_type_novelty.ipynb",
    "fact_novelty.ipynb",
    "fact_messaging_accumulating.ipynb",
    "fact_messaging_daily.ipynb",
    "fact_messaging_time.ipynb",
    "constraints.ipynb"
]

def run_notebook(notebook_path):
    try:
        with open("notebooks/" + notebook_path) as f:
            nb = nbformat.read(f, as_version=4)
        ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
        ep.preprocess(nb, {"metadata": {"path": "./notebooks/"}})
        print(f"Notebook {notebook_path} executed successfully")
        return True
    except Exception as e:
        print(f"Error executing {notebook_path}: {e}")
        return False

def main():

    with open('config.yaml') as f:
        config = yaml.safe_load(f)
        configDestination = config['destination']

    urlDestination = f"{configDestination['driver']}://{configDestination['user']}:{configDestination['password']}@{configDestination['host']}:{configDestination['port']}/{configDestination['db']}"
    engineDestination = create_engine(urlDestination)

    with engineDestination.connect() as connection:
        inspector = inspect(engineDestination)
        tables = inspector.get_table_names()
        
        if tables:
            print(f"A data warehouse named {configDestination['db']} already exists")
            print("Please hold on for a moment, everything is being prepared for the loading of the data warehouse")
            
            connection.execute(text("SET session_replication_role = 'replica';"))
            
            for table in tables:
                connection.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE;'))
                
            connection.commit()

            connection.execute(text("SET session_replication_role = 'origin';"))

            print("✅ Everything has been prepared correctly")

    print("Running notebooks...")
    all_success = True

    for notebook in notebooks:
        success = run_notebook(notebook)
        if not success:
            all_success = False

    if all_success:
        print("✅ All notebooks executed successfully")
    else:
        print("Some notebooks did not execute successfully")


if __name__ == "__main__":
    main()
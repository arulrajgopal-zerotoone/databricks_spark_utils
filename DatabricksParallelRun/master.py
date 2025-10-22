from concurrent.futures import ThreadPoolExecutor, as_completed


notebook_path = {
    'Task1':'./Task1',
    'Task2':'./Task2',
    'Task3':'./Task3'
}

def run_nb(path):
    nb_output = dbutils.notebook.run(path,0)
    return nb_output



# run without as_completed
with ThreadPoolExecutor() as executor:
    futures = [executor.submit(run_nb, nb_path) for nb_path in notebook_path.values()]

    for nb in futures:
        print(nb.result())


# run with as_completed
with ThreadPoolExecutor() as executor:
    futures = [executor.submit(run_nb, nb_path) for nb_path in notebook_path.values()]

    for nb in as_completed(futures):
        print(nb.result())


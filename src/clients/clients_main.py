from src.clients.abstract_client import AbstractClient

clients: dict[str, AbstractClient] = {}

def run_all_clients():
    print("clients running")
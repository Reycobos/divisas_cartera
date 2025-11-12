from typing import Dict
# from adapters.gate import GateAdapter
# from adapters.bingx import BingxAdapter

REGISTRY: Dict[str, object] = {
    # "gate": GateAdapter(...),
    # "bingx": BingxAdapter(...),
}

def get_adapters():
    return REGISTRY.items()



from rx import create
from rx import operators as ops


def kafka_observable(consumer):
    def _observable(observer, _):
        try:
            for msg in consumer:
                observer.on_next(msg.value)
        except Exception as e:
            observer.on_error(e)
    return create(_observable)

def machine_codes():
    return {"UNS56A": "UNSCRAMBLER",
        "WS964F": "WASHER",
        "IS8710": "INSPECTION",
        "FB713A": "FILLING",
        "C7841R": "CARBONATOR",
        "CPM784": "CAPPING",
        "LBL74F": "LABELING",
        "PLL741": "PALLETIZER"
    }
    
def properties_codes():
    return {
        "A7": "LITERS",
        "W8": "QUALITY",
        "L1": "LIGHT",
        "T3": "TIME",
        "P6": "POWER",
        "G8": "GRADES"
    }

def attributes_codes():
    return {
        "TS": "TIMESTAMP",
        "MC": "MACHINE",
        "PR": "PRODUCT",
        "PS": "PROPS"
    }

def machines_mapping(code):
    return machine_codes()[code]

def properties_mapping(code):
    return properties_codes()[code]

def attributes_mapping(code):
   return attributes_codes()[code]

def machine_name(event):
    event["MACHINE"] = machines_mapping(event["MACHINE"])
    return event

def props(event):
    print(f"props: {event}")
    event["PROPS"] = {properties_mapping(k): v for k, v in event["PROPS"].items()}
    print(f"props: {event}")
    return event

def aux(e):
    print(f"aux: {e}")

def auxa(event):
    print(f"a: {event}")

def auxb(event):
    print(f"b: {event}")

def build_pipeline(source, send_rich_event, save_raw_event, save_rich_event):
    return source.pipe(
        ops.do_action(lambda e: print(f"event received: {e}")),
        ops.do_action(save_raw_event),
        ops.map(lambda e: {attributes_mapping(k) : e[k] for k in e.keys()}),
        ops.filter(lambda e: e["MACHINE"] in machine_codes().keys()),
        ops.map(machine_name),
        ops.map(props),
        ops.do_action(aux),
        ops.do_action(send_rich_event),
        ops.do_action(save_rich_event),
        ops.do_action(lambda _: print("rich event send"))
    )
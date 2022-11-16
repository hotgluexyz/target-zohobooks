import json
import os
from cgitb import lookup

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class UnifiedMapping:
    def __init__(self) -> None:
        pass

    def read_json_file(self, filename):
        # read file
        with open(os.path.join(__location__, f"{filename}"), "r") as filetoread:
            data = filetoread.read()

        # parse file
        content = json.loads(data)

        return content

    def map_lists(self, addresses, address_mapping, payload, type="billing_address"):
        address = {}
        for key in address_mapping.keys():
            address[address_mapping[key]] = addresses[key]
        payload[type] = address
        return payload

    def map_lineitems(self, lineitems, lineitems_mapping, payload):
        payload["line_items"] = []
        if isinstance(lineitems, list):

            if len(lineitems) > 0:
                for line in lineitems:
                    line_item = {}
                    for key in lineitems_mapping.keys():
                        if key in line:
                            if line[key]:
                                line_item[lineitems_mapping[key]] = line[key]
                    line_item["quantity"] = int(line_item["quantity"])
                    payload["line_items"].append(line_item)

        return payload

    def prepare_payload(self, record, endpoint="contact"):
        mapping = self.read_json_file(f"mapping.json")
        ignore = mapping["ignore"]
        mapping = mapping[endpoint]
        payload = {}
        payload_return = {}
        lookup_keys = mapping.keys()
        for lookup_key in lookup_keys:
            if lookup_key == "address":
                payload = self.map_lists(
                    record.get(lookup_key, {}),
                    mapping[lookup_key],
                    payload,
                    "billing_address",
                )
                payload = self.map_lists(
                    record.get(lookup_key, {}),
                    mapping[lookup_key],
                    payload,
                    "shipping_address",
                )
            elif lookup_key == "lineItems":
                line_items = record.get(lookup_key, [])
                payload = self.map_lineitems(
                    line_items,
                    mapping[lookup_key],
                    payload,
                )
            else:
                val = record.get(lookup_key, "")
                if val:
                    payload[mapping[lookup_key]] = val

        # filter ignored keys
        for key in payload.keys():
            if key not in ignore:
                payload_return[key] = payload[key]
        return payload_return

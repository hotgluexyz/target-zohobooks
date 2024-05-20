"""Zohobooks target sink class, which handles writing streams."""


import json
import requests

from datetime import datetime, timedelta
from singer_sdk.sinks import RecordSink
from pendulum import parse
from target_zohobooks.mapping import UnifiedMapping
from backports.cached_property import cached_property


class ZohobooksSink(RecordSink):
    """Zohobooks target sink class."""

    access_token = None
    expires_at = None
    total = 0
    
    @cached_property
    def base_url(self) -> str:
        url = self.config.get("accounts-server", "https://www.zohoapis.com")
        api_url = "https://www.zohoapis.com/books/v3/"
        # Mapping domain suffixes to their corresponding base API URIs
        domain_mapping = {
            '.com': 'https://www.zohoapis.com/books/',
            '.eu': 'https://www.zohoapis.eu/books/',
            '.in': 'https://www.zohoapis.in/books/',
            '.com.au': 'https://www.zohoapis.com.au/books/',
            '.jp': 'https://www.zohoapis.jp/books/'
        }

        # Check for domain presence and update api_url dynamically
        for domain, base_api_url in domain_mapping.items():
            if domain in url:
                api_url = base_api_url + 'v3/'  # Append '/v3/' to the base URL
                break  # Stop checking further domains if found

        return api_url

    def get_auth(self):
        url = self.config.get("accounts-server", "https://accounts.zoho.com")
        if self.access_token is None or self.expires_at <= datetime.utcnow():
            response = requests.post(
                f"{url}/oauth/v2/token",
                data={
                    "client_id": self.config.get("client_id"),
                    "client_secret": self.config.get("client_secret"),
                    "refresh_token": self.config.get("refresh_token"),
                    "grant_type": "refresh_token",
                },
            )

            data = response.json()
            if data.get("error"):
                raise Exception(f"Auth request failed with response {response.text}")

            self.access_token = data["access_token"]

            self.expires_at = datetime.utcnow() + timedelta(
                seconds=int(data["expires_in"]) - 10
            )  # pad by 10 seconds for clock drift

        return self.access_token

    def entity_search(self, entity_name="contacts", params=None):
        url = f"{self.base_url}/{entity_name}"
        res = requests.get(url=url, params=params, headers=self.get_headers()).json()
        if entity_name in res:
            if len(res[entity_name]) > 0:
                return res[entity_name]
            else:
                return None
        else:
            return None

    def entity_post(self, entity_name, payload):
        url = f"{self.base_url}/{entity_name}"
        res = requests.post(url, headers=self.get_headers(), json=payload)
        return res

    def get_headers(self):
        headers = {}
        headers["Content-Type"] = "application/json"
        headers["Authorization"] = f"Bearer {self.get_auth()}"
        return headers

    def invoice_lookup(self, payload):
        # date format fixes
        created_date = datetime.strptime(
            payload["date"], "%Y-%m-%dT%H:%M:%SZ"
        ).strftime("%Y-%m-%d")
        last_modified_time = datetime.strptime(
            payload["last_modified_time"], "%Y-%m-%dT%H:%M:%SZ"
        ).strftime("%Y-%m-%d")
        due_date = datetime.strptime(
            payload["due_date"], "%Y-%m-%dT%H:%M:%SZ"
        ).strftime("%Y-%m-%d")
        payload.update(
            {
                "date": created_date,
                "last_modified_time": last_modified_time,
                "due_date": due_date,
            }
        )
        # line items
        lineitems = payload["line_items"]
        new_lineItems = []
        # lookup item_id
        for lineitem in lineitems:
            new_item = lineitem
            if "item_id" not in lineitem:
                item = self.entity_search("items", {"name": lineitem["name"]})
                if len(item) > 0:
                    item = item[0]
                    new_item.update(
                        {
                            "item_id": item["item_id"],
                        }
                    )
            new_lineItems.append(new_item)
        payload["line_items"] = new_lineItems
        # check contact_id
        if "customer_id" not in payload:
            customer = self.entity_search(
                "contacts", {"contact_name": payload["customer_name"]}
            )
            if len(customer) > 0:
                customer = customer[0]
                payload.update({"customer_id": customer["contact_id"]})

        return payload

    def process_invoice(self, record):
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(record, "invoices")
        if payload.get("payment_expected_date"):
            payload["status"] = "paid"

        payload = self.invoice_lookup(payload)
        res = self.entity_post("invoices", payload)
        self.post_message(res)

    def process_bill(self, record):
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(record, "bills")

        vendor_name = record.get("vendorName")
        if vendor_name:
            vendors = self.entity_search("contacts", {"contact_name": vendor_name})

        if vendors:
            vendor_id = vendors[0]["contact_id"]
            payload["vendor_id"] = vendor_id

        purchaseorder_ids = []
        if record.get("lineItems"):
            if isinstance(record.get("lineItems"), str):
                line_items = json.dumps(record.get("lineItems"))
            else:
                line_items = record.get("lineItems")

        purchaseorder_ids = [item["orderId"] for item in line_items]
        payload["purchaseorder_ids"] = purchaseorder_ids
        for date_obj in ["due_date", "date"]:
            if payload.get(date_obj):
                payload[date_obj] = parse(payload[date_obj]).strftime("%Y-%m-%d")

        res = self.entity_post("bills", payload)
        self.post_message(res)

    def process_buyorder(self, record):
        mapping = UnifiedMapping()
        #get product ids for lines
        
        payload = mapping.prepare_payload(record, "buy_orders")
        #get vendor id
        vendor_name = record.get("supplier_name")
        line_items = [item for item in payload.get("line_items") if item.get("item_id")]
        if not line_items:
            self.logger.info(f"skipping buyorder {vendor_name} with no")
            return
        else:
            payload["line_items"] = line_items

        
        if vendor_name:
            vendors = self.entity_search("contacts", {"contact_name": record.get("supplier_name")})
        if vendors:
            vendor_id = vendors[0]["contact_id"]
            payload["vendor_id"] = vendor_id
        else:
            raise Exception(f"Supplier with name={vendor_name} does not exist in zohobooks")

        res = self.entity_post("purchaseorders", payload)
        self.post_message(res)

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        self.logger.info(f"Processing record for stream {self.stream_name}, record: {record}")
        if self.stream_name == "Bills":
            self.process_bill(record)
        if self.stream_name == "Invoices":
            self.process_invoice(record)
        if self.stream_name == "BuyOrders":
            self.process_buyorder(record)

    def post_message(self, res):
        try:
            res.raise_for_status()
        except Exception as e:
            raise Exception(f"Request failed with response {res.text}")

        self.total = self.total + 1
        print(f"Status: {res.status_code}, {self.total} records processed so far.")

import time
import uuid
from datetime import datetime, timedelta

from main.config import get_config_by_name
from main.logger.custom_logging import log
from main.models.catalog import SearchType
from main.request_models.schema import Domain
from main.cron.search_by_city import dump_request_and_make_gateway_search
from main.constants.city_list import CITY_LIST

def make_http_requests_for_search_by_item(search_type: SearchType, domains=None, cities=None, name=None, gps=None):
    search_payload_list = []
    end_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    domain_list = [e.value for e in Domain] if domains is None else domains
    city_list = CITY_LIST if cities is None else cities

    message = {
        "intent": {
            "item": {
                "descriptor": {
                    "name": name
                }
            },
            "fulfillment": {
                "end": {
                    "location": {
                        "gps": gps,
                    }
                }
            },
            "payment": {
                "@ondc/org/buyer_app_finder_fee_type": "percent",
                "@ondc/org/buyer_app_finder_fee_amount": "3"
            },
            "tags": [
                {
                    "code": "bap_terms",
                    "list": [
                        {
                            "code": "static_terms",
                            "value": ""
                        },
                    ]
                }
            ]
        }
    }


    for d in domain_list:
        for c in city_list:
            search_payload = {
                "context": {
                    "domain": d,
                    "action": "search",
                    "country": "IND",
                    "city": c,
                    "core_version": "1.2.0",
                    "bap_id": get_config_by_name("BAP_ID"),
                    "bap_uri": get_config_by_name("BAP_URL"),
                    "transaction_id": str(uuid.uuid4()),
                    "message_id": str(uuid.uuid4()),
                    "timestamp": end_time,
                    "ttl": "PT30S"
                },
                "message": message
            }

            search_payload_list.append(search_payload)

    for x in search_payload_list:
        dump_request_and_make_gateway_search(search_type, x)
        time.sleep(1)

def make_full_catalog_search_by_item_requests(domains=None, cities=None, name=None, gps=None):
    make_http_requests_for_search_by_item(SearchType.FULL, domains=domains, cities=cities, name=name, gps=gps)
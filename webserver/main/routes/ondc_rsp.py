
from flask import request
from flask_restx import Namespace, Resource
from datetime import datetime

from main import constant
from main.logger.custom_logging import log
from main.service.utils import validate_auth_header
from main.utils.validation import validate_payload_schema_based_on_version
from main.service.common import dump_request_payload, update_dumped_request_with_response
from main.models import get_mongo_collection
from main.repository import mongo
from main.repository.ack_response import get_ack_response


ondc_rsp_namespace = Namespace('ondc_rsp', description='ONDC RSP Namespace')


@ondc_rsp_namespace.route("/v1/on_collector_recon")
class AddCollectorReconResponse(Resource):

    @validate_auth_header
    def post(self):
        request_payload = request.get_json()
        log(f"Got the on_collector_recon request payload {request_payload} \n headers: {dict(request.headers)}!")
        resp = validate_payload_schema_based_on_version(request_payload, 'on_collector_recon')
        print(f"response after validation {resp}")
        if resp is None:
            entry_object_id = dump_request_payload("on_collector_recon", request_payload)
            collection_name = get_mongo_collection('on_collector_recon')
            request_payload["created_at"] = datetime.utcnow()
            is_successful = mongo.collection_insert_one(collection_name, request_payload)

            update_dumped_request_with_response(entry_object_id, resp)
            log(f"Got the on_collector_recon response {resp}!")
            return get_ack_response(request_payload[constant.CONTEXT], ack=True)
        else:
            log(f"Got the on_collector_recon response {resp}!")
            return get_ack_response(request_payload[constant.CONTEXT], ack=False)
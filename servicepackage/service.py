import sys
import os
import json
import pika
from connections import get_mongo_client, get_pika_connection
from bson import ObjectId
from datetime import datetime, timezone
from datetime import datetime, timezone
from google.protobuf.struct_pb2 import Value, Struct, ListValue, NullValue
from google.protobuf.json_format import MessageToDict, ParseDict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Service:
    def __init__(self, queue_name, node_type, required_input_params, node_output_params, workflow_pb2,service_function):
        self.queue_name = queue_name
        self.node_type = node_type
        self.required_input_params = required_input_params
        self.node_output_params = node_output_params
        self.service_function = service_function
        _, self.db = get_mongo_client()

        _, self.DB = get_mongo_client()
        self.workflow_pb2 = workflow_pb2
        self.Value = Value
        self.PROTOBUF_AVAILABLE = True
        print("Protobuf support enabled")



    def main(self):
        connection = get_pika_connection()
        channel = connection.channel()

        channel.queue_declare(queue=self.queue_name)

        channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)

        logger.info('Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
        
    def get_workflow_from_run_id(self,run_id):
        """
        Retrieves the workflow json based on the run ID.
        """
        logger.info(f"Fetching workflow for Run ID: {run_id}")  # 👈 Added for Debug
        collection = self.DB['runs']
        workflow = collection.find_one({"_id": ObjectId(run_id)})
        if not workflow:
            raise ValueError(f"Workflow with run ID {run_id} not found in the database")
        logger.info(f"Fetched workflow config: {json.dumps(workflow.get('config', {}), indent=2, default=str)}")  # 👈 Added for Debug
        return workflow

    def get_current_node(self,workflow, run_id):
        workflow_config = workflow.get('config', {})
        workflow_nodes = workflow_config.get('nodes', [])
        logger.info(f"Total nodes in workflow: {len(workflow_nodes)}")  # 👈 Added for Debug

        if not workflow_nodes:
            raise ValueError("No nodes found in the workflow configuration")

        for node in workflow_nodes:
            logger.info(f"Checking node: ID={node.get('id')}, isCurrent={node.get('isCurrent')}, type={node.get('type')}")  # 👈 Added for Debug
            if node.get("isCurrent", False):
                logger.info(f"Found current node: {node}")  # 👈 Added for Debug
                return node, node.get("id")

        raise ValueError("No current node found in the workflow configuration")
        
    def get_next_queues(self,node_types: list[str]):
        collection = self.DB['nodes']
        next_queues = []
        nodes = collection.find({"type": {"$in": node_types}})
        for node in nodes:
            queue_name = node.get("queue")
            if queue_name:
                next_queues.append(queue_name)
        if not next_queues:
            raise ValueError(f"No queues found for node types: {node_types}")
        return next_queues
        
    def set_output_execution_params(self,workflow: dict, 
                                    current_node_id: str, 
                                    output_execution_params: dict, 
                                    output_parameter_mapping: dict,
                                    node_run: dict,
                                    input_params_dict: dict, 
                                    input_params_dtype_dict: dict,
                                    output_params_dtype_dict: dict) -> tuple:
        """
        Sets the input execution parameters and global parameters
        for the next node and updates the workflow.
        Returns the updated workflow and the next queue to send the message to.
        """
        
        # Get the next nodes from the current node
        for node in workflow['config']['nodes']:
            if node.get('id') == current_node_id:
                node['isCurrent'] = False  # Mark current node as not current
                next_nodes = node.get('next', [])
                for output in node.get('outputs', []):
                    if output.get('name') in output_execution_params:
                        output['value'] = output_execution_params[output['name']]
                break
        else:
            raise ValueError(f"Current node with ID {current_node_id} not found in the workflow")
        
        next_node_types = []
        
        # Set the output execution parameters to the next nodes
        for node in workflow['config']['nodes']:
            if node.get('id') in next_nodes:
                # Get the next node and set it as current
                node['isCurrent'] = True
                next_node_type = node.get('type')
                next_node_types.append(next_node_type)
                for output in node.get('outputs', []):
                    if output.get('name') in output_execution_params and \
                        output.get('dataType') == output_params_dtype_dict.get(output['name']):
                        output['value'] = output_execution_params[output['name']]
                
                for input in node['inputs']:
                    # Set the input parameters to the output execution parameter
                    # if the input parameter has the same name and data type
                    # as the output parameter
                    if input['name'] in output_execution_params \
                        and input['dataType'] == output_params_dtype_dict.get(input['name']):
                        input['value'] = output_execution_params[input['name']]
                    # If the input parameter is mapped to an output parameter, set it
                    # to the value of the output parameter
                    elif output_parameter_mapping.get(input['name']) \
                        and output_parameter_mapping[input['name']] in output_execution_params\
                            and input['dataType'] == output_params_dtype_dict.get(output_parameter_mapping[input['name']]):
                        input['value'] = output_execution_params[output_parameter_mapping[input['name']]]
                    # If the input parameter is a global parameter, set it
                    # to the value of the global parameter in the workflow
                    elif input['type'] == 'global':
                        if input['name'] in workflow:
                            input['value'] = workflow[input['name']]
                        elif input.get('mapParameterValue'):
                            if input['mapParameterValue'] in workflow:
                                input['value'] = workflow[input['mapParameterValue']]
                            else:
                                raise ValueError(f"Global parameter '{input['mapParameterValue']}' not found in the workflow")
                        else:
                            raise ValueError(f"Global parameter '{input['name']}' not found in the workflow")
                    # If the input parameter is an execution parameter and 
                    # it has no value
                    elif input['type'] == 'execution':
                        # Check if the input parameter can be found in the input parameters
                        # for the previous node
                        if input['name'] in input_params_dict and \
                            input_params_dtype_dict.get(input['name']) == input['dataType']:
                            input['value'] = input_params_dict[input['name']]
                        else:
                            raise ValueError(f"Execution parameter '{input['name']}' not found in the output execution parameters")
                    elif input['type'] != 'workflow':
                        raise ValueError(f"Input parameter '{input['name']}' not found in the output execution parameters or global parameters")
        next_queues = self.get_next_queues(next_node_types) if len(next_node_types) > 0 else []
        
        # 9. Update the workflow in MongoDB
        updated_at = workflow.get('updatedAt', datetime.now(timezone.utc).isoformat())
        node_run['endTime'] = updated_at
        node_run['status'] = "completed"
        if 'nodeRuns' not in workflow:
            workflow['nodeRuns'] = []
        workflow['nodeRuns'].append(node_run)
        workflow['updatedAt'] = updated_at
        
        
        return workflow, next_queues


    def update_workflow_to_mongo(self,workflow: dict, run_id: str):
        """
        Updates the workflow in MongoDB with the given run ID.
        """
        runs_collection = self.DB['runs']
        workflow['updatedAt'] = datetime.now(timezone.utc)
        runs_collection.update_one({"_id": ObjectId(run_id)}, {"$set": workflow})
        logger.info(f"Workflow with run ID {run_id} updated successfully")


    def send_message_to_queue(self,queue_name: str, message: bytes):

        """
        Sends a message (protobuf bytes) to the specified RabbitMQ queue.
        """
        connection = get_pika_connection()
        channel = connection.channel()
        channel.queue_declare(queue=queue_name)
        # Send as bytes
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(content_type='application/x-protobuf')
        )
        logger.info(f" [x] Sent protobuf message to {queue_name} queue")
        connection.close()
        
        
    def deserialize_protobuf_message(self,body):
        """
        Deserialize protobuf message to workflow dictionary.
        """
        if not self.PROTOBUF_AVAILABLE or self.workflow_pb2 is None:
            raise RuntimeError("Protobuf is not available but is required for message processing.")
        try:
            workflow_proto = self.workflow_pb2.Workflow()
            workflow_proto.ParseFromString(body)

            return MessageToDict(workflow_proto, including_default_value_fields=True, preserving_proto_field_name=True)
        except Exception as e:
            logger.error(f"Error deserializing protobuf message: {e}")
            raise

    def normalize_workflow_dict(self, d):
        from bson import ObjectId
        def to_iso(val):
            if isinstance(val, datetime):
                return val.isoformat() + 'Z'  # ISO 8601 with UTC timezone
            return val

        def convert_value(val):
            if isinstance(val, ObjectId):
                return str(val)
            elif isinstance(val, datetime):
                return to_iso(val)
            elif isinstance(val, dict):
                return {k: convert_value(v) for k, v in val.items()}
            elif isinstance(val, list):
                return [convert_value(v) for v in val]
            else:
                return val

        d = convert_value(d)
        if 'startedAt' in d:
            d['startedAt'] = to_iso(d.get('startedAt'))
        if 'updatedAt' in d:
            d['updatedAt'] = to_iso(d.get('updatedAt'))
        return d

    def dict_to_workflow_proto(self, workflow_dict):
        # Ensure all ObjectIds are converted to str before parsing to proto
        normalized_dict = self.normalize_workflow_dict(workflow_dict)
        print(f"Normalized dict: {normalized_dict}")
        from google.protobuf.json_format import ParseDict
        workflow_proto = ParseDict(normalized_dict, self.workflow_pb2.Workflow(), ignore_unknown_fields=True)
        return workflow_proto
    def callback(self, ch, method, properties, body):
        logger.info(f"Received message: {body}")
        try:
            workflow_dict = self.deserialize_protobuf_message(body)

            # For protobuf workflow messages, we need to extract the runId from the workflow
            run_id = workflow_dict.get('runId') or str(ObjectId())
            workflow_dict['runId'] = run_id

            start_time = datetime.now(timezone.utc)
            node_run = {
                "nodeId": None,
                "nodeType": self.node_type,
                "startTime": start_time,
                "endTime": None,
                "status": "running",
                "messageID": str(ObjectId()),
                "runId": run_id
            }
            logger.info(f"Protobuf workflow message, Run ID: {run_id}")

            # Use the deserialized workflow directly instead of fetching from DB
            workflow = workflow_dict
            print(f"Workflow: {workflow}")

            logger.info("🟢 STEP 2: Getting current node")  # 👈 Added for Debug
            current_node, current_node_id = self.get_current_node(workflow, run_id)

            if not current_node or not isinstance(current_node, dict) or current_node.get('type') != self.node_type:
                raise ValueError(f"Current node is not of type '{self.node_type}' or is missing in the workflow")
            logger.info(f"Current Node: {current_node}")
            node_run['nodeId'] = current_node_id

            # 3. Get the input and output parameters from the current node
            input_params = current_node.get('inputs', [])
            output_params = current_node.get('outputs', [])

            # This is the mapping for output parameter of current node
            # to the input parameter of the next node
            output_parameter_mapping = {}
            for op in output_params:
                if 'mapParameterValue' in op:
                    output_parameter_mapping[op['mapParameterValue']] = op['name']

            logger.info(f"Output Parameter Mapping: {output_parameter_mapping}")

            # Validate input and output parameters
            # All required input parameters must be present
            # in the input_params list, with given dataType
            subtype_id = None
            for input_param in input_params:
                if input_param['name'] == 'subtypeId' and input_param['dataType'] == 'string':
                    subtype_id = input_param.get('value')
                    break
            if not subtype_id:
                raise ValueError("subtypeId is missing or None")

            for param in self.required_input_params[subtype_id]:
                if param['required']:
                    if not any(input_param['name'] == param['name'] and input_param['dataType'] == param['dataType'] for input_param in input_params):
                        raise ValueError(f"Required input parameter '{param['name']}' with data type '{param['dataType']}' is missing")

            # The input execution parameters must have values
            for input_param in input_params:
                if input_param['type'] == 'execution' and input_param.get('value') is None:
                    raise ValueError(f"Execution parameter '{input_param['name']}' has no value")

            # All output parameters must be present in the output_params list
            for param in self.node_output_params[subtype_id]:
                if not any(output_param['name'] == param['name'] and output_param['dataType'] == param['dataType'] for output_param in output_params):
                    raise ValueError(f"Output parameter '{param['name']}' with data type '{param['dataType']}' is missing")
            logger.info(f"Input Parameters: {input_params}")
            logger.info(f"Output Parameters: {output_params}")

            current_node_type = current_node.get('type')

            input_params_dict = {
                input_param['name']: input_param.get('value', None) for input_param in input_params
            }

            input_params_dtype_dict = {
                input_param['name']: input_param.get('dataType') for input_param in input_params
            }

            output_params_dtype_dict = {
                output_param['name']: output_param.get('dataType') for output_param in output_params
            }

            # 5. Execute the function of the node
            output_execution_params = self.service_function(input_params_dict, output_params, run_id)

            # 6, 7, 8 & 9. Set the input execution parameters to output execution parameters
            # and set the next node(s) as current
            workflow, next_queues = self.set_output_execution_params(workflow,
                                                                current_node_id,
                                                                output_execution_params,
                                                                output_parameter_mapping,
                                                                node_run,
                                                                input_params_dict,
                                                                input_params_dtype_dict,
                                                                output_params_dtype_dict)

            # 10. Send the run ID to the next queue
            # Always send as protobuf, not dict
            import asyncio

            workflow_proto = self.dict_to_workflow_proto(workflow)
            message_bytes = workflow_proto.SerializeToString()

            if next_queues:
                for next_queue in next_queues:
                    self.send_message_to_queue(next_queue, message_bytes)
                logger.info(f"Processed message for Run ID {run_id} and sent to next queue {next_queues}")
                # update to mongo after sending to queue
                self.update_workflow_to_mongo(workflow, str(run_id))

            else:
                workflow['status'] = "completed"
                self.update_workflow_to_mongo(workflow, str(run_id))
            logger.info(f"Workflow with Run ID {run_id} processed successfully")
        except Exception as e:
            logger.exception(f"Error processing message: {e}")
            return

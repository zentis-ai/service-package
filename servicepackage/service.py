import sys
import os
import json
from .connections import get_mongo_client, get_pika_connection
from bson import ObjectId
from datetime import datetime, timezone
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Service:
    def __init__(self, queue_name, node_type, required_input_params, node_output_params, service_function):
        self.queue_name = queue_name
        self.node_type = node_type
        self.required_input_params = required_input_params
        self.node_output_params = node_output_params
        self.service_function = service_function
        _, self.db = get_mongo_client()

    # def __init__(self):
    #     self.queue_name = None
    #     self.node_type = None
    #     self.required_input_params = None
    #     self.node_output_params = None
    #     self.service_function = None
    #     self.db = None
        
    #     try:
    #         _, self.db = get_mongo_client()
    #     except Exception as e:
    #         raise RuntimeError(f"Failed to initialize MongoDB client: {str(e)}")

    # def configure(self, queue_name, node_type, required_input_params, node_output_params, service_function):
    #     """Configure the processor with required parameters."""
    #     if None in [queue_name, node_type, required_input_params, node_output_params, service_function]:
    #         raise ValueError("All configuration parameters must be provided")
        
    #     self.queue_name = queue_name
    #     self.node_type = node_type
    #     self.required_input_params = required_input_params
    #     self.node_output_params = node_output_params
    #     self.service_function = service_function

    def main(self):
        """Start consuming messages from the queue."""
        connection = get_pika_connection()
        channel = connection.channel()

        channel.queue_declare(queue=self.queue_name)
        channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)

        logger.info('Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    
    def get_workflow_from_run_id(self, run_id):
        """Retrieves the workflow json based on the run ID."""
        collection = self.db['runs']
        workflow = collection.find_one({"_id": ObjectId(run_id)})
        if not workflow:
            raise ValueError(f"Workflow with run ID {run_id} not found in the database")
        return workflow

    def get_current_node(self, workflow, run_id):
        workflow_config = workflow.get('config', {})
        workflow_nodes = workflow_config.get('nodes', [])
        if not workflow_nodes:
            raise ValueError("No nodes found in the workflow configuration")
        for node in workflow_nodes:
            if node.get("isCurrent", False):
                return node, node.get("id")
        else:
            raise ValueError("No current node found in the workflow configuration")
    
    def get_next_queues(self, node_types):
        collection = self.db['nodes']
        next_queues = []
        nodes = collection.find({"type": {"$in": node_types}})
        for node in nodes:
            queue_name = node.get("queue")
            if queue_name:
                next_queues.append(queue_name)
        if not next_queues:
            raise ValueError(f"No queues found for node types: {node_types}")
        return next_queues
    
    def set_output_execution_params(self, workflow, current_node_id, output_execution_params, 
                                  output_parameter_mapping, node_run, input_params_dict, 
                                  input_params_dtype_dict, output_params_dtype_dict):
        """Sets the input execution parameters and global parameters for the next node."""
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
                node['isCurrent'] = True
                next_node_type = node.get('type')
                next_node_types.append(next_node_type)
                
                for output in node.get('outputs', []):
                    if (output.get('name') in output_execution_params and 
                        output.get('dataType') == output_params_dtype_dict.get(output['name'])):
                        output['value'] = output_execution_params[output['name']]
                
                for input in node['inputs']:
                    if (input['name'] in output_execution_params and 
                        input['dataType'] == output_params_dtype_dict.get(input['name'])):
                        input['value'] = output_execution_params[input['name']]
                    elif (output_parameter_mapping.get(input['name']) and 
                          output_parameter_mapping[input['name']] in output_execution_params and
                          input['dataType'] == output_params_dtype_dict.get(output_parameter_mapping[input['name']])):
                        input['value'] = output_execution_params[output_parameter_mapping[input['name']]]
                    elif input['type'] == 'global':
                        if input['name'] in workflow:
                            input['value'] = workflow[input['name']]
                        elif input.get('mapParameterValue'):
                            if input['mapParameterValue'] in workflow:
                                input['value'] = workflow[input['mapParameterValue']]
                            else:
                                raise ValueError(f"Global parameter '{input['mapParameterValue']}' not found in workflow")
                        else:
                            raise ValueError(f"Global parameter '{input['name']}' not found in workflow")
                    elif input['type'] == 'execution':
                        if (input['name'] in input_params_dict and 
                            input_params_dtype_dict.get(input['name']) == input['dataType']):
                            input['value'] = input_params_dict[input['name']]
                        else:
                            raise ValueError(f"Execution parameter '{input['name']}' not found")
                    elif input['type'] != 'workflow':
                        raise ValueError(f"Input parameter '{input['name']}' not found")
        
        next_queues = self.get_next_queues(next_node_types) if next_node_types else []
        
        node_run['endTime'] = workflow['updatedAt']
        node_run['status'] = "completed"
        if 'nodeRuns' not in workflow:
            workflow['nodeRuns'] = []
        workflow['nodeRuns'].append(node_run)
        
        return workflow, next_queues

    def update_workflow_to_mongo(self, workflow, run_id):
        """Updates the workflow in MongoDB with the given run ID."""
        runs_collection = self.db['runs']
        workflow['updatedAt'] = datetime.now(timezone.utc)
        runs_collection.update_one({"_id": ObjectId(run_id)}, {"$set": workflow})
        logger.info(f"Workflow with run ID {run_id} updated successfully")

    def send_message_to_queue(self, queue_name, message):
        """Sends a message to the specified RabbitMQ queue."""
        connection = get_pika_connection()
        channel = connection.channel()
        channel.queue_declare(queue=queue_name)
        
        message_json = json.dumps(message)
        channel.basic_publish(exchange='', routing_key=queue_name, body=message_json)
        logger.info(f"Sent message to {queue_name} queue")
        connection.close()
    
    def callback(self, ch, method, properties, body):
        """Callback function for processing received messages."""
        logger.info(f"Received message: {body}")
        message = json.loads(body)
        start_time = datetime.now(timezone.utc)
        node_run = {
            "nodeId": None,
            "nodeType": self.node_type,
            "startTime": start_time,
            "endTime": None,
            "status": "running",
            "messageID": message.get('id'),
            "runId": message.get('runId')
        }
        
        try:
            run_id = message.get('runId')
            if not run_id:
                raise ValueError("Run ID is missing in the message")
            
            workflow = self.get_workflow_from_run_id(run_id)
            logger.info(f"Workflow for Run ID {run_id}: {workflow}")
            
            current_node, current_node_id = self.get_current_node(workflow, run_id)
            if not current_node or current_node.get('type') != self.node_type:
                raise ValueError(f"Current node is not of type '{self.node_type}'")
            logger.info(f"Current Node: {current_node}")
            node_run['nodeId'] = current_node_id
            
            input_params = current_node.get('inputs', [])
            output_params = current_node.get('outputs', [])
            
            output_parameter_mapping = {}
            for op in output_params:
                if 'mapParameterValue' in op:
                    output_parameter_mapping[op['mapParameterValue']] = op['name']
            
            subtype_id = None
            for input_param in input_params:
                if input_param['name'] == 'subtypeId' and input_param['dataType'] == 'string':
                    subtype_id = input_param.get('value')
                    break
            
            for param in self.required_input_params[subtype_id]:
                if param['required']:
                    if not any(input_param['name'] == param['name'] and 
                              input_param['dataType'] == param['dataType'] 
                              for input_param in input_params):
                        raise ValueError(f"Required input parameter '{param['name']}' missing")
            
            for input_param in input_params:
                if input_param['type'] == 'execution' and input_param.get('value') is None:
                    raise ValueError(f"Execution parameter '{input_param['name']}' has no value")
            
            for param in self.node_output_params[subtype_id]:
                if not any(output_param['name'] == param['name'] and 
                          output_param['dataType'] == param['dataType'] 
                          for output_param in output_params):
                    raise ValueError(f"Output parameter '{param['name']}' missing")
            
            input_params_dict = {
                input_param['name']: input_param.get('value', None) 
                for input_param in input_params
            }
            
            input_params_dtype_dict = {
                input_param['name']: input_param.get('dataType') 
                for input_param in input_params
            }
            
            output_params_dtype_dict = {
                output_param['name']: output_param.get('dataType') 
                for output_param in output_params
            }
            
            output_execution_params = self.service_function(input_params_dict, output_params, run_id)
             
            workflow, next_queues = self.set_output_execution_params(
                workflow, current_node_id, output_execution_params,
                output_parameter_mapping, node_run, input_params_dict,
                input_params_dtype_dict, output_params_dtype_dict
            )
            
            if next_queues:
                self.update_workflow_to_mongo(workflow, str(run_id))
                message = {"runId": run_id, "id": message.get('id')}
                for next_queue in next_queues:
                    self.send_message_to_queue(next_queue, message)
                logger.info(f"Processed message for Run ID {run_id}")
            else:
                workflow['status'] = "completed"
                self.update_workflow_to_mongo(workflow, str(run_id))
            
            logger.info(f"Workflow with Run ID {run_id} processed successfully")
        except Exception as e:
            workflow['status'] = "failed"
            self.update_workflow_to_mongo(workflow, str(workflow['_id']))
            logger.exception(f"Error processing message: {e}")